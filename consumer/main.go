// PulseCity Consumer
//
// Kafka'dan vehicle-pings topic'ini okur, batch halinde ScyllaDB'ye yazar.
// Zero-loss ilkesi:
//  1. Auto-commit KAPALI - offset sadece basariyla islendikten (ScyllaDB'ye
//     yazildiktan ya da DLQ'ya yonlendirildikten) SONRA commit edilir.
//  2. Isleme sirasinda hata olursa mesaj DLQ topic'ine yazilir, asla sessizce
//     atilmaz.
//  3. Consumer crash olursa, commit edilmemis mesajlar bir sonraki
//     baslatmada tekrar okunur (at-least-once garanti).
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"math"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gocql/gocql"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/segmentio/kafka-go"
)

// Faz 4: Prometheus metrikleri
var (
	metricProcessedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pulsecity_consumer_messages_processed_total",
		Help: "ScyllaDB'ye basariyla yazilan toplam mesaj sayisi",
	})
	metricDLQTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pulsecity_consumer_dlq_total",
		Help: "DLQ'ya yonlendirilen toplam mesaj sayisi",
	})
	metricBatchWriteDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "pulsecity_consumer_batch_write_seconds",
		Help:    "ScyllaDB batch yazma suresi (saniye)",
		Buckets: prometheus.DefBuckets,
	})
	// Bolge bazli analiz icin: Grafana'da "hangi bolgede ne kadar arac
	// yogunlugu var" panelini besler (kimlik kartinda tanimlanan domain-spesifik
	// metrik gereksinimi).
	metricPingsByZone = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pulsecity_vehicle_pings_by_zone_total",
		Help: "Bolgeye gore islenen GPS ping sayisi",
	}, []string{"zone_id"})
	metricAvgSpeedByZone = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "pulsecity_vehicle_avg_speed_kmh",
		Help: "Bolgeye gore son batch'teki ortalama hiz (km/h)",
	}, []string{"zone_id"})
	// Faz 8: anomali tespiti
	metricZoneAnomaly = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "pulsecity_zone_anomaly_detected",
		Help: "Bolgede anomali (olasi tikaniklik/kaza) tespit edildi mi: 1 = evet, 0 = hayir",
	}, []string{"zone_id"})
	metricZoneBaseline = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "pulsecity_zone_speed_baseline_kmh",
		Help: "Bolge icin ogrenilen normal hiz baseline'i (EMA, km/h)",
	}, []string{"zone_id"})
	// Cifte hata (ScyllaDB VE DLQ ayni anda yazilamiyor): batch commit
	// edilmeden birakilir, Kafka mesajlari yeniden teslim eder. Bu sayac
	// alarma baglanacak sinyaldir - artiyorsa boru hatti ilerlemiyor demektir.
	metricUncommittedBatches = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pulsecity_consumer_uncommitted_batches_total",
		Help: "Kalici olarak yazilamadigi icin offset'i commit EDILMEYEN batch sayisi",
	})
	// Readiness sondasi icin: ScyllaDB session'i canli mi.
	metricScyllaUp = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "pulsecity_consumer_scylla_up",
		Help: "ScyllaDB session'i saglikli mi: 1 = evet, 0 = hayir",
	})
)

// --- Faz 8: Anomali tespiti --------------------------------------------------
//
// Sistemi "veri tasiyan"dan "veriden anlam cikaran"a tasiyan katman. Her bolge
// icin normal hizin ne oldugunu EMA (ustel hareketli ortalama) ile OGRENIR ve
// bu normalden ciddi bir sapmayi anomali olarak isaretler.
//
// Not: zone_id hem Kafka partition key'i hem de ScyllaDB partition key'i
// oldugu icin bir bolgenin TUM mesajlari tek bir consumer replikasina duser.
// Bu sayede `docker compose up -d --scale consumer=3` yapildiginda baseline
// replikalara bolunmez, tutarli kalir - mevcut partition tasariminin bedava
// getirdigi bir ozellik.

const (
	// Isinma penceresi: baseline guvenilir hale gelene kadar anomali aranmaz.
	baselineWarmupBatches = 20
	// Normalin bu oranda altina dusen ortalama hiz anomali sayilir.
	anomalyDropRatio = 0.40
	// EMA agirligi: dusuk deger = baseline yavas ve istikrarli degisir.
	baselineAlpha = 0.05
	// DLQ yazimi kalici hata sayilmadan once kac kez denenir.
	dlqWriteAttempts = 3
)

type zoneBaseline struct {
	ema        float64
	samples    int
	wasAnomaly bool
}

// evaluate, bolgenin guncel ortalama hizini isler, baseline'i gunceller ve
// anomali olup olmadigini dondurur.
//
// Metriklerden ve loglamadan bagimsiz tutuldu ki dogrudan test edilebilsin.
func (b *zoneBaseline) evaluate(current float64) bool {
	b.samples++

	if b.samples <= baselineWarmupBatches {
		// Isinma: basit hareketli ortalama ile hizli yakinsama. Bu olmadan
		// baseline 0'dan baslar ve ilk batch'ler her zaman anomali gorunur.
		b.ema += (current - b.ema) / float64(b.samples)
		return false
	}

	if current < b.ema*(1-anomalyDropRatio) {
		// KRITIK TASARIM KARARI: anomali sirasinda baseline GUNCELLENMEZ.
		// Aksi halde uzun suren bir sikisiklik yavas yavas "yeni normal"
		// haline gelir, EMA ona yakinsar ve anomali bir daha asla tespit
		// edilemez - tespit sistemi kendi kendini kor eder.
		return true
	}

	b.ema = baselineAlpha*current + (1-baselineAlpha)*b.ema
	return false
}

var (
	baselineMu sync.Mutex
	baselines  = map[string]*zoneBaseline{}
)

// updateZoneAnalytics, basariyla yazilan bir zone grubunun domain metriklerini
// gunceller: yogunluk, ortalama hiz ve anomali durumu.
func updateZoneAnalytics(zoneID string, pings []VehiclePing) {
	metricPingsByZone.WithLabelValues(zoneID).Add(float64(len(pings)))

	var speedSum float64
	for _, p := range pings {
		speedSum += p.SpeedKmh
	}
	current := speedSum / float64(len(pings))
	metricAvgSpeedByZone.WithLabelValues(zoneID).Set(current)

	baselineMu.Lock()
	b, ok := baselines[zoneID]
	if !ok {
		b = &zoneBaseline{}
		baselines[zoneID] = b
	}
	anomaly := b.evaluate(current)
	baseline := b.ema
	changed := anomaly != b.wasAnomaly
	b.wasAnomaly = anomaly
	baselineMu.Unlock()

	metricZoneBaseline.WithLabelValues(zoneID).Set(baseline)
	if anomaly {
		metricZoneAnomaly.WithLabelValues(zoneID).Set(1)
	} else {
		metricZoneAnomaly.WithLabelValues(zoneID).Set(0)
	}

	// Her batch'te degil, sadece durum degistiginde logla - yoksa saniyede
	// onlarca satir akar.
	if changed {
		if anomaly {
			// Warn seviyesi: bu operatorun gormesi gereken bir durum, rutin
			// ilerleme bilgisi degil.
			slog.Warn("anomali tespit edildi",
				"zone_id", zoneID,
				"speed_kmh", round1(current),
				"baseline_kmh", round1(baseline),
				"drop_pct", round1((1-current/baseline)*100),
			)
		} else {
			slog.Info("bolge normale dondu",
				"zone_id", zoneID,
				"speed_kmh", round1(current),
				"baseline_kmh", round1(baseline),
			)
		}
	}
}

// round1, log alanlarini bir ondalik basamaga yuvarlar - ham float64
// (8.033333333333333) log ciktisini okunmaz hale getiriyor.
func round1(v float64) float64 {
	return math.Round(v*10) / 10
}

// startMetricsServer, /metrics'in yani sira liveness ve readiness sondalarini
// da sunar.
//
//   - /healthz (liveness): process ayakta ve HTTP'ye cevap veriyor mu.
//   - /readyz  (readiness): ScyllaDB session'i GERCEKTEN kullanilabilir mi.
//
// Ikisinin ayri olmasi onemli: connectWithRetry yalnizca ACILISTA calisiyor.
// Baglanti calisma sirasinda koparsa consumer process'i cokmez, her batch'i
// DLQ'ya yazmaya devam eder ve disaridan bakan hicbir sey bunu fark etmezdi.
// /readyz session uzerinde hafif bir sorgu kosturdugu icin tam bu durumu
// yakalar ve Docker'in healthcheck'i container'i yeniden baslatir.
func startMetricsServer(addr string, session *gocql.Session) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	})

	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()
		// system.local her node'da bulunan hafif bir tablodur; keyspace'e ya da
		// uygulama verisine dokunmadan session'in canliligini olcer.
		if err := session.Query("SELECT key FROM system.local").WithContext(ctx).Exec(); err != nil {
			metricScyllaUp.Set(0)
			http.Error(w, "scylla erisilemiyor: "+err.Error(), http.StatusServiceUnavailable)
			return
		}
		metricScyllaUp.Set(1)
		w.Write([]byte("ok"))
	})

	srv := &http.Server{Addr: addr, Handler: mux}
	go func() {
		slog.Info("metrics/health endpoint dinleniyor", "addr", addr,
			"paths", "/metrics,/healthz,/readyz")
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("metrics server hatasi", "err", err)
		}
	}()
	return srv
}

type VehiclePing struct {
	VehicleID string    `json:"vehicle_id"`
	Lat       float64   `json:"lat"`
	Lng       float64   `json:"lng"`
	SpeedKmh  float64   `json:"speed_kmh"`
	Heading   int       `json:"heading"`
	Timestamp time.Time `json:"timestamp"`
	ZoneID    string    `json:"zone_id"`
}

func getenv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}

// setupLogger, servisin genel logger'ini kurar.
//
// Neden yapilandirilmis log: onceki surumde her satir `log.Printf` ile
// elle bicimlendirilmis duz metindi ("[consumer] ANOMALI: istanbul-sisli-1
// bolgesinde hiz 8.0 km/h..."). Bir log toplama aracina (Loki/CloudWatch)
// baglandiginda bolgeye gore filtrelemek ya da hiz esigine gore alarm kurmak
// icin bu metni regex'le parse etmek gerekirdi - kirilgan ve pahali. JSON
// alanlari ise dogrudan sorgulanabilir: {zone_id="istanbul-sisli-1"}.
//
// Seviye ayrimi da yeni: onceki surumde ANOMALI ile "batch islendi" ayni
// seviyedeydi, yani "sadece hatalari goster" diye bir filtre kurulamiyordu.
//
// slog.SetDefault ayrica standart `log` paketinin ciktisini da bu handler'a
// yonlendirir - gocql ve kafka-go kendi loglarini `log` ile yaziyor, onlar da
// otomatik olarak yapilandirilmis hale gelir.
func setupLogger(service string) *slog.Logger {
	var level slog.Level
	switch strings.ToLower(getenv("LOG_LEVEL", "info")) {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{Level: level}

	var h slog.Handler
	// Varsayilan JSON: konteyner ciktisi dogrudan makine tarafindan okunur.
	// LOG_FORMAT=text yerelde goz ile okumak icin.
	if strings.ToLower(getenv("LOG_FORMAT", "json")) == "text" {
		h = slog.NewTextHandler(os.Stdout, opts)
	} else {
		h = slog.NewJSONHandler(os.Stdout, opts)
	}

	l := slog.New(h).With("service", service)
	slog.SetDefault(l)
	return l
}

func main() {
	brokers := getenv("KAFKA_BROKERS", "localhost:9092")
	topic := getenv("KAFKA_TOPIC", "vehicle-pings")
	dlqTopic := getenv("KAFKA_DLQ_TOPIC", "vehicle-pings-dlq")
	groupID := getenv("KAFKA_GROUP_ID", "pulsecity-consumers")
	scyllaHosts := getenv("SCYLLA_HOSTS", "localhost:9042")

	setupLogger("consumer")

	slog.Info("baslatiliyor",
		"brokers", brokers,
		"topic", topic,
		"group_id", groupID,
		"scylla_hosts", scyllaHosts,
	)

	// --- ScyllaDB baglantisi ---
	cluster := gocql.NewCluster(scyllaHosts)
	cluster.Keyspace = "pulsecity"
	cluster.Consistency = gocql.Quorum
	cluster.ConnectTimeout = 15 * time.Second
	cluster.Timeout = 10 * time.Second
	cluster.NumConns = 8 // Faz 3: zone bazli paralel batch yazma icin baglanti havuzu buyutuldu

	session, err := connectWithRetry(cluster, 10, 3*time.Second)
	if err != nil {
		slog.Error("ScyllaDB'ye baglanilamadi, cikiliyor", "err", err)
		os.Exit(1)
	}
	defer session.Close()

	// --- Kafka reader (manuel commit) ---
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{brokers},
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: 0, // 0 = manuel commit, otomatik commit YOK
	})
	defer reader.Close()

	dlqWriter := &kafka.Writer{
		Addr:         kafka.TCP(brokers),
		Topic:        dlqTopic,
		RequiredAcks: kafka.RequireAll,
	}
	defer dlqWriter.Close()

	metricsSrv := startMetricsServer(getenv("METRICS_ADDR", ":2113"), session)

	// SIGTERM'i yakala: `docker compose up -d` ile yapilan her deploy ve her
	// restart bu sinyali gonderir, 10 saniye sonra SIGKILL gelir. Sinyali
	// dinlemeden batch'in ortasinda oldurulmek veri kaybettirmez (commit
	// edilmemis mesajlar yeniden teslim edilir) ama her deploy'u gereksiz bir
	// reprocessing turuna cevirir.
	rootCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	batchSize, _ := strconv.Atoi(getenv("CONSUMER_BATCH_SIZE", "2000")) // Faz 3: buyutuldu
	const flushInterval = 250 * time.Millisecond

	msgBuf := make([]kafka.Message, 0, batchSize)

	processedTotal := 0

	// flush, elindeki batch'i isler ve YALNIZCA kalici olarak yazildiysa
	// offset'i commit eder.
	flush := func(ctx context.Context) {
		if len(msgBuf) == 0 {
			return
		}

		if err := writeBatchToScylla(session, dlqWriter, ctx, msgBuf); err != nil {
			// Ne ScyllaDB'ye ne DLQ'ya yazilabildi. Commit ETMIYORUZ: buffer'i
			// bosaltip offset'i ilerletirsek mesajlar kaybolur. Buffer'i
			// birakip donuyoruz, bir sonraki flush ayni mesajlari tekrar
			// dener; consumer bu arada yeniden baslatilirsa Kafka onlari
			// commit edilmemis offset'ten yeniden teslim eder.
			return
		}

		// Basariyla islendi (ScyllaDB'ye yazildi ya da DLQ'ya yonlendirildi) ->
		// simdi offset'i commit et. Bu sira ONEMLI: once isle, sonra commit et.
		if err := reader.CommitMessages(ctx, msgBuf...); err != nil {
			// Commit basarisiz: mesajlar ScyllaDB'de duruyor, kayip yok. Ayni
			// mesajlar yeniden teslim edilecek ve ayni birincil anahtara
			// yazilacak (idempotent). Buffer'i temizliyoruz ki bellek buyumesin.
			slog.Warn("offset commit hatasi (mesajlar yazildi, yeniden islenecekler)",
				"batch_size", len(msgBuf), "err", err)
		}

		processedTotal += len(msgBuf)
		// Debug: 2000'lik batch'lerle 50k/sn'de saniyede ~25 satir eder.
		// Rutin ilerleme bilgisi Info'yu bogar; ilerlemenin asil olcusu
		// zaten pulsecity_consumer_messages_processed_total metrigi.
		slog.Debug("batch islendi", "batch_size", len(msgBuf), "total", processedTotal)
		msgBuf = msgBuf[:0]
	}

loop:
	for {
		// FetchMessage kullaniyoruz (ReadMessage degil) cunku otomatik commit yapmiyor.
		fetchCtx, cancel := context.WithTimeout(rootCtx, flushInterval)
		m, err := reader.FetchMessage(fetchCtx)
		cancel()

		if err != nil {
			if rootCtx.Err() != nil {
				break loop // kapanis sinyali geldi
			}
			if errors.Is(err, context.DeadlineExceeded) {
				flush(rootCtx) // zaman doldu, elimizdekini isle
				continue
			}
			slog.Warn("kafka fetch hatasi", "err", err)
			continue
		}

		msgBuf = append(msgBuf, m)

		if len(msgBuf) >= batchSize {
			flush(rootCtx)
		}
	}

	// --- Duzenli kapanis ---
	// rootCtx artik iptal edilmis durumda, son flush'i temiz bir context ile
	// yapmamiz gerekiyor - aksi halde ScyllaDB yazimi ve commit aninda iptal
	// olur ve elimizdeki batch bosuna yeniden islenirdi.
	slog.Info("kapanis sinyali alindi, son batch isleniyor", "pending_messages", len(msgBuf))
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancelShutdown()

	flush(shutdownCtx)
	metricsSrv.Shutdown(shutdownCtx)
	slog.Info("kapandi", "total_processed", processedTotal)
}

func connectWithRetry(cluster *gocql.ClusterConfig, attempts int, delay time.Duration) (*gocql.Session, error) {
	var lastErr error
	for i := 0; i < attempts; i++ {
		session, err := cluster.CreateSession()
		if err == nil {
			return session, nil
		}
		lastErr = err
		slog.Warn("ScyllaDB baglanti denemesi basarisiz",
			"attempt", i+1, "max_attempts", attempts, "err", err)
		time.Sleep(delay)
	}
	return nil, lastErr
}

// writeBatchToScylla, mesajlari ScyllaDB'ye yazar.
//
// Faz 3 performans notu: Tum mesajlari TEK bir LoggedBatch'e koymak
// ScyllaDB'de ciddi bir anti-pattern'dir - LoggedBatch, farkli partition'lar
// (zone_id) arasinda atomiklik garantisi icin pahali bir batchlog mekanizmasi
// kullanir ve coklu-partition durumunda throughput'u boguar.
// Bunun yerine: mesajlari zone_id'ye gore grupluyoruz (ayni partition icindeki
// batch'ler ucuzdur -> UnloggedBatch kullanilabilir) ve her zone grubunu
// PARALEL goroutine'lerde yaziyoruz.
//
// DONUS DEGERI ZERO-LOSS ICIN KRITIK: nil olmayan bir hata "bu batch'in en az
// bir mesaji hicbir yere kalici olarak yazilamadi" demektir ve cagiran taraf
// offset'i commit ETMEMELIDIR. Onceki surumde bu fonksiyon void'di; ScyllaDB
// ile DLQ ayni anda erisilemez oldugunda (ornegin genis bir network kesintisi)
// yalnizca bir log satiri basiliyor, offset yine de ilerliyordu - mesajlar
// sessizce kayboluyordu.
func writeBatchToScylla(session *gocql.Session, dlqWriter *kafka.Writer, ctx context.Context, messages []kafka.Message) error {
	start := time.Now()
	defer func() { metricBatchWriteDuration.Observe(time.Since(start).Seconds()) }()

	byZone := make(map[string][]VehiclePing)
	var dlqMessages []kafka.Message
	var dlqMu sync.Mutex

	for _, m := range messages {
		var ping VehiclePing
		if err := json.Unmarshal(m.Value, &ping); err != nil {
			dlqMessages = append(dlqMessages, toDLQMessage(m.Value, "json_parse_error: "+err.Error()))
			continue
		}
		byZone[ping.ZoneID] = append(byZone[ping.ZoneID], ping)
	}

	var wg sync.WaitGroup
	for zoneID, pings := range byZone {
		wg.Add(1)
		go func(zoneID string, pings []VehiclePing) {
			defer wg.Done()

			batch := session.NewBatch(gocql.UnloggedBatch) // ayni partition (zone_id) icinde ucuz
			for _, p := range pings {
				batch.Query(
					`INSERT INTO vehicle_pings (zone_id, ping_time, vehicle_id, lat, lng, speed_kmh, heading, received_at)
					 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
					p.ZoneID, p.Timestamp, p.VehicleID, p.Lat, p.Lng, p.SpeedKmh, p.Heading, time.Now().UTC(),
				)
			}

			if err := session.ExecuteBatch(batch); err != nil {
				slog.Error("scylla batch yazma hatasi, mesajlar DLQ'ya yonlendiriliyor",
					"zone_id", zoneID, "message_count", len(pings), "err", err)
				dlqMu.Lock()
				for _, p := range pings {
					raw, _ := json.Marshal(p)
					dlqMessages = append(dlqMessages, toDLQMessage(raw, "scylla_batch_write_error: "+err.Error()))
				}
				dlqMu.Unlock()
				return
			}

			// Basarili yazim -> bolge bazli domain metrikleri + Faz 8 anomali analizi
			updateZoneAnalytics(zoneID, pings)
		}(zoneID, pings)
	}
	wg.Wait()

	if len(dlqMessages) > 0 {
		if err := writeDLQWithRetry(ctx, dlqWriter, dlqMessages, dlqWriteAttempts); err != nil {
			// Son care senaryo: ScyllaDB'ye de DLQ'ya da yazamiyoruz. Tek
			// yapabilecegimiz dogru sey commit ETMEMEK; Kafka mesajlari
			// yeniden teslim eder. ScyllaDB'ye zaten yazilmis olan mesajlar
			// tekrar islenir ama birincil anahtar ayni oldugu icin uzerine
			// yazilir - coklanma olmaz, at-least-once burada guvenli.
			slog.Error("KRITIK: scylla VE DLQ yazilamadi, batch commit edilmiyor",
				"message_count", len(messages),
				"action", "kafka mesajlari yeniden teslim edecek",
				"err", err)
			metricUncommittedBatches.Inc()
			return err
		}
		metricDLQTotal.Add(float64(len(dlqMessages)))
	}

	metricProcessedTotal.Add(float64(len(messages) - len(dlqMessages)))
	return nil
}

// writeDLQWithRetry, DLQ yazimini kisa bir backoff'la yeniden dener.
// Producer tarafinda writeWithRetry ile korunan yol burada korumasizdi:
// anlik bir broker hiccup'i dogrudan "kalici hata" sayiliyordu.
func writeDLQWithRetry(ctx context.Context, w *kafka.Writer, msgs []kafka.Message, attempts int) error {
	var err error
	for attempt := 1; attempt <= attempts; attempt++ {
		if err = w.WriteMessages(ctx, msgs...); err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return err // kapanis suruyor, denemeye devam etmenin anlami yok
		}
		backoff := time.Duration(attempt) * 200 * time.Millisecond
		slog.Warn("DLQ yazma denemesi basarisiz",
			"attempt", attempt, "max_attempts", attempts, "backoff", backoff.String(), "err", err)
		time.Sleep(backoff)
	}
	return err
}

func toDLQMessage(payload []byte, reason string) kafka.Message {
	envelope := map[string]string{
		"id":            uuid.NewString(),
		"raw_payload":   string(payload),
		"error_reason":  reason,
		"failed_at_utc": time.Now().UTC().Format(time.RFC3339),
	}
	b, _ := json.Marshal(envelope)
	return kafka.Message{Value: b}
}

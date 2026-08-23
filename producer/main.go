// PulseCity Load Generator
//
// Bu servis, sehirde dolasan sanal araclarin GPS ping'lerini simule eder
// ve Kafka'ya uretir. Faz 1'de dusuk hizda (TARGET_RATE_PER_SEC) calisir,
// Faz 3'te 50.000 msg/sn hedefine kadar tune edilecek.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"math"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/segmentio/kafka-go"
)

// Faz 4: Prometheus metrikleri - Grafana'da throughput ve hata oranini
// gercek zamanli gormek icin expose edilir.
var (
	metricProducedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pulsecity_producer_messages_total",
		Help: "Basariyla Kafka'ya yazilan toplam GPS ping sayisi",
	})
	metricProduceErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pulsecity_producer_errors_total",
		Help: "Kafka'ya yazilamayan (retry sonrasi da basarisiz) mesaj sayisi",
	})
)

// lastWriteOK, Kafka'ya en son basariyla yazilan saniyenin unix zamani.
// /readyz bu degere bakar: process ayakta olabilir ama broker'a hic
// yazamiyorsa saglikli sayilmamali.
var lastWriteOK atomic.Int64

// Producer saniyede bir yazar; bu suredir basarili yazma yoksa hazir degiliz.
// Esik writeWithRetry'in toplam backoff'undan (5 deneme, ~3sn) belirgin
// sekilde buyuk secildi ki gecici bir broker hiccup'i restart tetiklemesin.
const readyStaleAfter = 30 * time.Second

// startMetricsServer, /metrics'in yani sira liveness ve readiness sondalarini
// sunar ve sunucuyu duzenli kapatabilmek icin geri dondurur.
func startMetricsServer(addr string) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	})

	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		last := lastWriteOK.Load()
		if last == 0 {
			// Henuz ilk tur tamamlanmadi; start_period bu araligi kapsiyor.
			http.Error(w, "ilk yazma henuz tamamlanmadi", http.StatusServiceUnavailable)
			return
		}
		if age := time.Since(time.Unix(last, 0)); age > readyStaleAfter {
			http.Error(w, "Kafka'ya son basarili yazmanin uzerinden "+age.Truncate(time.Second).String()+" gecti",
				http.StatusServiceUnavailable)
			return
		}
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

// VehiclePing, Kafka'ya gonderilen tek bir GPS event'ini temsil eder.
type VehiclePing struct {
	VehicleID string    `json:"vehicle_id"`
	Lat       float64   `json:"lat"`
	Lng       float64   `json:"lng"`
	SpeedKmh  float64   `json:"speed_kmh"`
	Heading   int       `json:"heading"`
	Timestamp time.Time `json:"timestamp"`
	ZoneID    string    `json:"zone_id"`
}

// Istanbul'un en kalabalik 9 ilcesi. Her bolge = TEK bir gercek ilce.
//
// Onceki surumde 3 ilce x 3 alt bolge (kadikoy-1/2/3 gibi) kullaniliyordu;
// haritada ayni ilce adini tasiyan uc ayri cember yan yana cikinca en distaki
// komsu ilcenin uzerinde duruyormus gibi gorunuyor ve yanlis ilce adi
// okunuyordu. Bire bir ilce eslemesi bu belirsizligi ortadan kaldirir.
//
// Bolge sayisi ScyllaDB'de partition dagilimini belirler; 9 bolge tek bir
// global sayaca gore hot-partition riskini belirgin sekilde azaltir.
//
// Kimlikler bilerek ASCII: bu deger hem Kafka mesaj anahtari hem ScyllaDB
// partition key'i. Turkce gosterim adlari arayuz tarafinda eslenir.
var zones = []struct {
	ID      string
	LatBase float64
	LngBase float64
}{
	// Avrupa yakasi
	{"istanbul-esenyurt", 41.0290, 28.6740},
	{"istanbul-kucukcekmece", 41.0000, 28.7750},
	{"istanbul-bagcilar", 41.0390, 28.8560},
	{"istanbul-bahcelievler", 40.9980, 28.8590},
	{"istanbul-sultangazi", 41.1060, 28.8720},
	// Anadolu yakasi
	{"istanbul-uskudar", 41.0230, 29.0150},
	{"istanbul-umraniye", 41.0160, 29.1240},
	{"istanbul-maltepe", 40.9350, 29.1310},
	{"istanbul-pendik", 40.8770, 29.2330},
}

// vehicleState, her sanal aracin surekliligini (konum/hiz) tutar; boylece
// ping'ler birbirine baglantisiz rastgele noktalar degil, gercekci bir
// rotaya benzer sekilde ilerler.
type vehicleState struct {
	id      string
	zoneIdx int
	lat     float64
	lng     float64
	speed   float64
	heading int
}

func newVehicle(id string, rng *rand.Rand) *vehicleState {
	zi := rng.Intn(len(zones))
	z := zones[zi]

	// Arac, bolge dairesi icinde rastgele bir noktada dogar. sqrt(u) alan
	// bazinda duzgun dagilim verir; dogrudan u kullanmak araclari merkeze
	// yigar ve haritada ilcenin ortasinda kucuk bir kume gibi gorunurdu.
	angle := rng.Float64() * 2 * math.Pi
	dist := zoneRadiusMeters * math.Sqrt(rng.Float64())
	dLat, dLng := metersToDegrees(math.Cos(angle)*dist, math.Sin(angle)*dist, z.LatBase)

	return &vehicleState{
		id:      id,
		zoneIdx: zi,
		lat:     z.LatBase + dLat,
		lng:     z.LngBase + dLng,
		speed:   10 + rng.Float64()*50,
		heading: rng.Intn(360),
	}
}

const (
	// Bir derece enlem her yerde ~111.32 km. Boylam icin bu deger cos(enlem)
	// ile daralir: Istanbul'un 41. paralelinde 1 derece boylam ~84 km.
	metersPerDegreeLat = 111320.0

	// Aracin kendi bolge merkezinden uzaklasabilecegi azami mesafe; haritadaki
	// bolge cemberi de bu yaricapi temsil eder.
	//
	// Ilce olceginde anlamli gorunmesi icin 2 km secildi. En yakin iki merkez
	// (Bagcilar - Bahcelievler) yaklasik 4.6 km arayla oldugu icin cemberler
	// ust uste binmez - bolgeler haritada net ayrisir.
	zoneRadiusMeters = 2000.0
)

// metersToDegrees, bir metre farkini enlem/boylam derecesine cevirir.
// Boylam, verilen enlemdeki daralmaya gore olceklenir.
func metersToDegrees(north, east, atLat float64) (dLat, dLng float64) {
	dLat = north / metersPerDegreeLat
	dLng = east / (metersPerDegreeLat * math.Cos(atLat*math.Pi/180))
	return
}

// distanceMeters, iki nokta arasindaki yaklasik mesafe (duz duzlem yaklasimi -
// birkac kilometrelik olceklerde hata ihmal edilebilir).
func distanceMeters(lat1, lng1, lat2, lng2 float64) float64 {
	dNorth := (lat1 - lat2) * metersPerDegreeLat
	dEast := (lng1 - lng2) * metersPerDegreeLat * math.Cos(lat2*math.Pi/180)
	return math.Hypot(dNorth, dEast)
}

// step, araci bir sonraki ping icin hareket ettirir ve o ping'de bildirilecek
// efektif hizi dondurur.
//
// dt: ayni aracin iki ping'i arasinda gecen sure (saniye). Konum degisimi
// dogrudan speed_kmh'den turetilir - yani alanin bildirdigi hiz ile haritada
// gozlenen hiz birbirini tutar.
//
// speedFactor: normalde 1.0. Anomali demo modu bir bolgeyi yapay olarak
// tikadiginda 1'den kucuk gelir; hem hareket hem bildirilen hiz ayni oranda
// dustugu icin harita ile veri celismez.
func (v *vehicleState) step(rng *rand.Rand, dt, speedFactor float64) float64 {
	// Hiz zaman zaman degisir (trafik yogunlugu hissi vermek icin)
	v.speed += (rng.Float64() - 0.5) * 8
	if v.speed < 0 {
		v.speed = 0
	}
	if v.speed > 90 {
		v.speed = 90
	}

	// Yon zaman zaman hafifce sapar
	v.heading = (v.heading + rng.Intn(21) - 10 + 360) % 360

	effectiveSpeed := v.speed * speedFactor

	// km/h -> m/s -> bu adimda alinan metre
	meters := effectiveSpeed * 1000 / 3600 * dt

	rad := float64(v.heading) * math.Pi / 180
	dLat, dLng := metersToDegrees(math.Cos(rad)*meters, math.Sin(rad)*meters, v.lat)
	v.lat += dLat
	v.lng += dLng

	// Bolgede tutma: arac kendi bolgesinin yaricapini asarsa merkeze dogru
	// yonlendirilir. Bu olmadan arac saatler icinde sehrin disina cikar ama
	// zoneIdx hic degismedigi icin zone_id etiketi yanlis kalir - yani veri
	// yalan soyler. Ayrica haritada araclar Marmara'ya acilir.
	z := zones[v.zoneIdx]
	if distanceMeters(v.lat, v.lng, z.LatBase, z.LngBase) > zoneRadiusMeters {
		towardCenter := math.Atan2(
			(z.LngBase-v.lng)*math.Cos(v.lat*math.Pi/180),
			z.LatBase-v.lat,
		) * 180 / math.Pi
		// Tam merkeze kilitlenmesin diye +-30 derece jitter
		v.heading = ((int(towardCenter)+rng.Intn(61)-30)%360 + 360) % 360
	}

	return effectiveSpeed
}

// setupLogger, servisin genel logger'ini kurar. Gerekce ve alan semasi icin
// bkz. consumer/main.go — uc servis de ayni sozlesmeye uyar: JSON varsayilan,
// LOG_FORMAT=text yerel okuma icin, LOG_LEVEL ile seviye.
//
// slog.SetDefault standart `log` paketinin ciktisini da bu handler'a yonlendirir;
// kafka-go kendi loglarini `log` ile yaziyor, onlar da yapilandirilmis olur.
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
	if strings.ToLower(getenv("LOG_FORMAT", "json")) == "text" {
		h = slog.NewTextHandler(os.Stdout, opts)
	} else {
		h = slog.NewJSONHandler(os.Stdout, opts)
	}

	l := slog.New(h).With("service", service)
	slog.SetDefault(l)
	return l
}

func getenv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}

// --- Faz 8: anomali demo modu ------------------------------------------------
//
// Anomali tespitinin gercekten calistigini canli gosterebilmek icin periyodik
// olarak bir bolgeyi yapay olarak tikariz. Uretim verisini bilerek bozdugu
// icin VARSAYILAN KAPALI: docker-compose.yml bunu acikca acar, prod override
// ve benchmark olcumleri kapali birakir (yoksa bolgesel hiz metrikleri
// kirlenir ve benchmark sonucu yaniltici olur).

const (
	demoCongestionFactor = 0.25             // tikanik bolgede hiz %25'e duser
	demoPeriod           = 60 * time.Second // ne siklikla yeni bir bolge secilir
	demoDuration         = 20 * time.Second // tikaniklik ne kadar surer
)

// congestedZone: o an yapay olarak tikanik olan bolgenin indeksi, -1 = yok.
// Yayin dongusu bunu her ping'de okudugu icin atomik.
var congestedZone atomic.Int32

func init() { congestedZone.Store(-1) }

// speedFactorFor, verilen bolge icin uygulanacak hiz carpanini dondurur.
func speedFactorFor(zoneIdx int) float64 {
	if congestedZone.Load() == int32(zoneIdx) {
		return demoCongestionFactor
	}
	return 1.0
}

// runAnomalyDemo, ctx iptal edilene kadar periyodik olarak bir bolgeyi yapay
// olarak tikar. ctx olmadan bu goroutine kapanis sirasinda uyumaya devam eder
// ve process'in temiz cikisini geciktirirdi.
func runAnomalyDemo(ctx context.Context, rng *rand.Rand) {
	sleep := func(d time.Duration) bool {
		t := time.NewTimer(d)
		defer t.Stop()
		select {
		case <-ctx.Done():
			return false
		case <-t.C:
			return true
		}
	}

	for {
		if !sleep(demoPeriod - demoDuration) {
			return
		}
		idx := rng.Intn(len(zones))
		congestedZone.Store(int32(idx))
		slog.Info("demo: bolge yapay olarak tikandi",
			"zone_id", zones[idx].ID, "duration", demoDuration.String())
		if !sleep(demoDuration) {
			congestedZone.Store(-1)
			return
		}
		congestedZone.Store(-1)
		slog.Info("demo: bolge normale dondu", "zone_id", zones[idx].ID)
	}
}

func writeWithRetry(ctx context.Context, writer *kafka.Writer, batch []kafka.Message, maxAttempts int) error {
	var err error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		err = writer.WriteMessages(ctx, batch...)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			// Kapanis suruyor: backoff'la beklemenin anlami yok, SIGKILL'e
			// kadar olan sureyi tuketmis oluruz.
			return err
		}
		backoff := time.Duration(attempt) * 200 * time.Millisecond
		slog.Warn("kafka yazma denemesi basarisiz",
			"attempt", attempt, "max_attempts", maxAttempts,
			"backoff", backoff.String(), "err", err)
		time.Sleep(backoff)
	}
	return err
}

func main() {
	brokers := getenv("KAFKA_BROKERS", "localhost:9092")
	topic := getenv("KAFKA_TOPIC", "vehicle-pings")
	targetRate, _ := strconv.Atoi(getenv("TARGET_RATE_PER_SEC", "50000"))
	vehicleCount, _ := strconv.Atoi(getenv("VEHICLE_COUNT", "20000"))
	workerCount, _ := strconv.Atoi(getenv("PRODUCER_WORKERS", "8"))
	anomalyDemo := getenv("TEST_ANOMALY_DEMO", "false") == "true"

	// Ayni aracin iki ping'i arasinda gecen sure. Her arac saniyede
	// targetRate/vehicleCount kez ping'lendigi icin dt bunun tersidir
	// (2000 arac, 5000 msg/sn -> 0.4 sn). step() konum degisimini bu sureden
	// turetir; yanlis bir dt, araclarin bildirdikleri hizdan farkli bir hizla
	// hareket ediyormus gibi gorunmesine yol acar.
	dt := float64(vehicleCount) / float64(targetRate)

	setupLogger("producer")

	slog.Info("baslatiliyor",
		"brokers", brokers,
		"topic", topic,
		"target_rate_per_sec", targetRate,
		"vehicle_count", vehicleCount,
		"ping_interval_sec", math.Round(dt*1000)/1000,
		"anomaly_demo", anomalyDemo,
	)

	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokers),
		Topic:        topic,
		Balancer:     &kafka.Hash{}, // zone_id'ye gore tutarli partition secimi icin asagida key kullaniyoruz
		RequiredAcks: kafka.RequireAll,
		// Not: kafka-go native idempotent producer flag'i sunmuyor.
		// Zero-loss garantisi: (1) RequireAll + retry, (2) consumer tarafinda
		// idempotent yazma (ScyllaDB'de ayni primary key = uzerine yazar, coklanmaz).
		MaxAttempts:  5,
		BatchSize:    500,             // Faz 3: yuksek throughput icin buyutuldu
		BatchBytes:   4 * 1024 * 1024, // 4MB
		BatchTimeout: 20 * time.Millisecond,
		Async:        false,
	}
	defer writer.Close()

	metricsSrv := startMetricsServer(getenv("METRICS_ADDR", ":2112"))

	// SIGTERM: Docker her restart/deploy'da bu sinyali gonderir. Yakalamadan
	// oldurulmek, ucusta olan bir Kafka yaziminin yarida kesilmesi demek.
	rootCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	vehicles := make([]*vehicleState, vehicleCount)
	for i := 0; i < vehicleCount; i++ {
		vehicles[i] = newVehicle("v-"+strconv.Itoa(i), rng)
	}

	if anomalyDemo {
		go runAnomalyDemo(rootCtx, rand.New(rand.NewSource(time.Now().UnixNano()+9999)))
	}

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	ctx := rootCtx
	sentTotal := 0

	// Her worker'in kendi rand.Rand instance'i olmali - math/rand.Rand
	// concurrent kullanima karsi guvenli degil (data race).
	workerRngs := make([]*rand.Rand, workerCount)
	for i := range workerRngs {
		workerRngs[i] = rand.New(rand.NewSource(time.Now().UnixNano() + int64(i)))
	}

	// Her worker'a AYRIK (cakismayan) bir arac grubu atiyoruz. Bu, ayni
	// vehicleState struct'inin iki farkli goroutine tarafindan es zamanli
	// mutate edilmesini (data race) engeller - workerCount, vehicleCount'u
	// tam bolmese bile her worker sadece KENDI grubu icinde donguye girer.
	vehiclesPerWorker := vehicleCount / workerCount
	if vehiclesPerWorker < 1 {
		vehiclesPerWorker = 1
	}

	for {
		select {
		case <-rootCtx.Done():
			// Ucustaki tur yok (wg.Wait asagida bitiyor); guvenle cikabiliriz.
			slog.Info("kapanis sinyali alindi", "total_sent", sentTotal)
			shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 5*time.Second)
			metricsSrv.Shutdown(shutdownCtx)
			cancelShutdown()
			return
		case <-ticker.C:
		}

		secondStart := time.Now()

		// Faz 3: 50k/sn'lik batch'i workerCount kadar paralel goroutine'e
		// bolerek yaziyoruz - tek goroutine ile bu hizda Kafka'ya yetismek
		// zor, coklu baglanti/goroutine ile throughput artiriliyor.
		chunkSize := targetRate / workerCount
		var wg sync.WaitGroup
		var mu sync.Mutex
		var totalWritten int
		var writeErr error

		for w := 0; w < workerCount; w++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				batch := make([]kafka.Message, 0, chunkSize)
				workerRng := workerRngs[workerID]
				groupStart := workerID * vehiclesPerWorker

				for i := 0; i < chunkSize; i++ {
					idx := groupStart + (i % vehiclesPerWorker)
					if idx >= len(vehicles) {
						idx = idx % len(vehicles)
					}
					v := vehicles[idx]
					speed := v.step(workerRng, dt, speedFactorFor(v.zoneIdx))

					zone := zones[v.zoneIdx]
					ping := VehiclePing{
						VehicleID: v.id,
						Lat:       v.lat,
						Lng:       v.lng,
						SpeedKmh:  math.Round(speed*10) / 10,
						Heading:   v.heading,
						Timestamp: time.Now().UTC(),
						ZoneID:    zone.ID,
					}

					payload, err := json.Marshal(ping)
					if err != nil {
						continue
					}
					batch = append(batch, kafka.Message{Key: []byte(zone.ID), Value: payload})
				}

				if err := writeWithRetry(ctx, writer, batch, 5); err != nil {
					mu.Lock()
					writeErr = err
					mu.Unlock()
					metricProduceErrors.Add(float64(len(batch)))
					slog.Error("KRITIK: worker yazma basarisiz, mesajlar kaybolmus olabilir",
						"worker_id", workerID, "message_count", len(batch), "err", err)
					return
				}

				mu.Lock()
				totalWritten += len(batch)
				mu.Unlock()
			}(w)
		}

		wg.Wait()

		if totalWritten > 0 {
			// /readyz bu damgaya bakar: yazma tamamen durursa readiness duser
			// ve Docker healthcheck'i container'i yeniden baslatir.
			lastWriteOK.Store(time.Now().Unix())
		}

		sentTotal += totalWritten
		metricProducedTotal.Add(float64(totalWritten))
		elapsed := time.Since(secondStart)
		// Debug: saniyede bir satir, Info'da uzun kosularda gurultu yapar.
		// Throughput'un asil olcusu pulsecity_producer_messages_total metrigi.
		slog.Debug("saniyelik tur tamamlandi",
			"sent", totalWritten, "target", targetRate,
			"duration_ms", elapsed.Milliseconds(), "total", sentTotal,
			"had_error", writeErr != nil)
	}
}

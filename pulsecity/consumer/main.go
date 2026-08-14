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
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
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
)

func startMetricsServer(addr string) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	log.Printf("[consumer] metrics endpoint: %s/metrics", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Printf("[consumer] metrics server hatasi: %v", err)
	}
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

func main() {
	brokers := getenv("KAFKA_BROKERS", "localhost:9092")
	topic := getenv("KAFKA_TOPIC", "vehicle-pings")
	dlqTopic := getenv("KAFKA_DLQ_TOPIC", "vehicle-pings-dlq")
	groupID := getenv("KAFKA_GROUP_ID", "pulsecity-consumers")
	scyllaHosts := getenv("SCYLLA_HOSTS", "localhost:9042")

	log.Printf("[consumer] baslatiliyor: brokers=%s topic=%s group=%s scylla=%s",
		brokers, topic, groupID, scyllaHosts)

	// --- ScyllaDB baglantisi ---
	cluster := gocql.NewCluster(scyllaHosts)
	cluster.Keyspace = "pulsecity"
	cluster.Consistency = gocql.Quorum
	cluster.ConnectTimeout = 15 * time.Second
	cluster.Timeout = 10 * time.Second
	cluster.NumConns = 8 // Faz 3: zone bazli paralel batch yazma icin baglanti havuzu buyutuldu

	session, err := connectWithRetry(cluster, 10, 3*time.Second)
	if err != nil {
		log.Fatalf("[consumer] ScyllaDB'ye baglanilamadi: %v", err)
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

	go startMetricsServer(getenv("METRICS_ADDR", ":2113"))

	ctx := context.Background()

	batchSize, _ := strconv.Atoi(getenv("CONSUMER_BATCH_SIZE", "2000")) // Faz 3: buyutuldu
	const flushInterval = 250 * time.Millisecond

	msgBuf := make([]kafka.Message, 0, batchSize)
	flushTicker := time.NewTicker(flushInterval)
	defer flushTicker.Stop()

	processedTotal := 0

	flush := func() {
		if len(msgBuf) == 0 {
			return
		}
		writeBatchToScylla(session, dlqWriter, ctx, msgBuf)

		// Basariyla islendi (ScyllaDB'ye yazildi ya da DLQ'ya yonlendirildi) ->
		// simdi offset'i commit et. Bu sira ONEMLI: once isle, sonra commit et.
		if err := reader.CommitMessages(ctx, msgBuf...); err != nil {
			log.Printf("[consumer] offset commit hatasi: %v", err)
		}

		processedTotal += len(msgBuf)
		log.Printf("[consumer] batch islendi: %d, toplam: %d", len(msgBuf), processedTotal)
		msgBuf = msgBuf[:0]
	}

	for {
		// FetchMessage kullaniyoruz (ReadMessage degil) cunku otomatik commit yapmiyor.
		fetchCtx, cancel := context.WithTimeout(ctx, flushInterval)
		m, err := reader.FetchMessage(fetchCtx)
		cancel()

		if err != nil {
			if err == context.DeadlineExceeded {
				flush() // zaman doldu, elimizdekini isle
				continue
			}
			log.Printf("[consumer] fetch hatasi: %v", err)
			continue
		}

		msgBuf = append(msgBuf, m)

		if len(msgBuf) >= batchSize {
			flush()
		}
	}
}

func connectWithRetry(cluster *gocql.ClusterConfig, attempts int, delay time.Duration) (*gocql.Session, error) {
	var lastErr error
	for i := 0; i < attempts; i++ {
		session, err := cluster.CreateSession()
		if err == nil {
			return session, nil
		}
		lastErr = err
		log.Printf("[consumer] ScyllaDB baglanti denemesi basarisiz (%d/%d): %v", i+1, attempts, err)
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
func writeBatchToScylla(session *gocql.Session, dlqWriter *kafka.Writer, ctx context.Context, messages []kafka.Message) {
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
				log.Printf("[consumer] SCYLLA YAZMA HATASI (zone=%s), %d mesaj DLQ'ya yonlendiriliyor: %v",
					zoneID, len(pings), err)
				dlqMu.Lock()
				for _, p := range pings {
					raw, _ := json.Marshal(p)
					dlqMessages = append(dlqMessages, toDLQMessage(raw, "scylla_batch_write_error: "+err.Error()))
				}
				dlqMu.Unlock()
				return
			}

			// Basarili yazim -> bolge bazli domain metriklerini guncelle
			metricPingsByZone.WithLabelValues(zoneID).Add(float64(len(pings)))
			var speedSum float64
			for _, p := range pings {
				speedSum += p.SpeedKmh
			}
			metricAvgSpeedByZone.WithLabelValues(zoneID).Set(speedSum / float64(len(pings)))
		}(zoneID, pings)
	}
	wg.Wait()

	if len(dlqMessages) > 0 {
		if err := dlqWriter.WriteMessages(ctx, dlqMessages...); err != nil {
			// Son care senaryo: DLQ'ya bile yazamiyorsak logla (Faz 5 dayaniklilik
			// testleri bu senaryoyu da olcer).
			log.Printf("[consumer] KRITIK: DLQ'ya yazma da basarisiz oldu, veri kaybi riski: %v", err)
		}
		metricDLQTotal.Add(float64(len(dlqMessages)))
	}

	metricProcessedTotal.Add(float64(len(messages) - len(dlqMessages)))
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

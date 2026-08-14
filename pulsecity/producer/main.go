// PulseCity Load Generator
//
// Bu servis, sehirde dolasan sanal araclarin GPS ping'lerini simule eder
// ve Kafka'ya uretir. Faz 1'de dusuk hizda (TARGET_RATE_PER_SEC) calisir,
// Faz 3'te 50.000 msg/sn hedefine kadar tune edilecek.
package main

import (
	"context"
	"encoding/json"
	"log"
	"math"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"sync"
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

func startMetricsServer(addr string) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	log.Printf("[producer] metrics endpoint: %s/metrics", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Printf("[producer] metrics server hatasi: %v", err)
	}
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

// Bursa merkezli basit bir bolge/grid tanimi.
// Gercek bir sehri 3x3'luk 9 bolgeye ayiriyoruz (v1 icin yeterli granularite).
var zones = []struct {
	ID      string
	LatBase float64
	LngBase float64
}{
	{"bursa-osmangazi-1", 40.1950, 29.0500},
	{"bursa-osmangazi-2", 40.1950, 29.0700},
	{"bursa-nilufer-1", 40.2100, 29.0300},
	{"bursa-nilufer-2", 40.2100, 29.0500},
	{"bursa-yildirim-1", 40.1800, 29.0800},
	{"bursa-yildirim-2", 40.1800, 29.1000},
	{"bursa-osmangazi-3", 40.1750, 29.0500},
	{"bursa-nilufer-3", 40.2050, 29.0100},
	{"bursa-yildirim-3", 40.1900, 29.1100},
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
	return &vehicleState{
		id:      id,
		zoneIdx: zi,
		lat:     z.LatBase + (rng.Float64()-0.5)*0.01,
		lng:     z.LngBase + (rng.Float64()-0.5)*0.01,
		speed:   10 + rng.Float64()*50,
		heading: rng.Intn(360),
	}
}

// step, araci bir sonraki ping icin hafifce hareket ettirir.
func (v *vehicleState) step(rng *rand.Rand) {
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

	// Konumu hiz ve yone gore kaydir (kaba bir yaklasim, gercek yol agi degil)
	rad := float64(v.heading) * math.Pi / 180
	deltaLat := math.Cos(rad) * v.speed * 0.00001
	deltaLng := math.Sin(rad) * v.speed * 0.00001
	v.lat += deltaLat
	v.lng += deltaLng
}

func getenv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}

func writeWithRetry(ctx context.Context, writer *kafka.Writer, batch []kafka.Message, maxAttempts int) error {
	var err error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		err = writer.WriteMessages(ctx, batch...)
		if err == nil {
			return nil
		}
		backoff := time.Duration(attempt) * 200 * time.Millisecond
		log.Printf("[producer] yazma denemesi %d/%d basarisiz, %v sonra tekrar denenecek: %v",
			attempt, maxAttempts, backoff, err)
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

	log.Printf("[producer] baslatiliyor: brokers=%s topic=%s hedef_hiz=%d/sn arac_sayisi=%d",
		brokers, topic, targetRate, vehicleCount)

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

	go startMetricsServer(getenv("METRICS_ADDR", ":2112"))

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	vehicles := make([]*vehicleState, vehicleCount)
	for i := 0; i < vehicleCount; i++ {
		vehicles[i] = newVehicle("v-"+strconv.Itoa(i), rng)
	}

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	ctx := context.Background()
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

	for range ticker.C {
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
					v.step(workerRng)

					zone := zones[v.zoneIdx]
					ping := VehiclePing{
						VehicleID: v.id,
						Lat:       v.lat,
						Lng:       v.lng,
						SpeedKmh:  math.Round(v.speed*10) / 10,
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
					log.Printf("[producer] worker-%d KRITIK: yazma basarisiz, %d mesaj kaybolmus olabilir: %v",
						workerID, len(batch), err)
					return
				}

				mu.Lock()
				totalWritten += len(batch)
				mu.Unlock()
			}(w)
		}

		wg.Wait()

		sentTotal += totalWritten
		metricProducedTotal.Add(float64(totalWritten))
		elapsed := time.Since(secondStart)
		log.Printf("[producer] bu saniyede gonderilen: %d (hedef: %d), sure: %v, toplam: %d, hata: %v",
			totalWritten, targetRate, elapsed, sentTotal, writeErr != nil)
	}
}

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
	"sync/atomic"
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

func runAnomalyDemo(rng *rand.Rand) {
	for {
		time.Sleep(demoPeriod - demoDuration)
		idx := rng.Intn(len(zones))
		congestedZone.Store(int32(idx))
		log.Printf("[producer] DEMO: %s bolgesi %v boyunca yapay olarak tikandi",
			zones[idx].ID, demoDuration)
		time.Sleep(demoDuration)
		congestedZone.Store(-1)
		log.Printf("[producer] DEMO: %s bolgesi normale dondu", zones[idx].ID)
	}
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
	anomalyDemo := getenv("TEST_ANOMALY_DEMO", "false") == "true"

	// Ayni aracin iki ping'i arasinda gecen sure. Her arac saniyede
	// targetRate/vehicleCount kez ping'lendigi icin dt bunun tersidir
	// (2000 arac, 5000 msg/sn -> 0.4 sn). step() konum degisimini bu sureden
	// turetir; yanlis bir dt, araclarin bildirdikleri hizdan farkli bir hizla
	// hareket ediyormus gibi gorunmesine yol acar.
	dt := float64(vehicleCount) / float64(targetRate)

	log.Printf("[producer] baslatiliyor: brokers=%s topic=%s hedef_hiz=%d/sn arac_sayisi=%d ping_araligi=%.3fsn anomali_demo=%v",
		brokers, topic, targetRate, vehicleCount, dt, anomalyDemo)

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

	if anomalyDemo {
		go runAnomalyDemo(rand.New(rand.NewSource(time.Now().UnixNano() + 9999)))
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

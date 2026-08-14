// PulseCity Web Visualizer (Faz 9)
//
// Kafka'daki vehicle-pings akisini okur, bellekte "her aracin son konumu" +
// "bolge basina pencere istatistikleri" tutar ve bunu WebSocket uzerinden
// tarayiciya canli harita olarak yayinlar.
//
// Neden ayri bir servis:
//  1. Ana consumer zero-loss kritik yolunda ve manuel offset commit ediyor;
//     oraya WebSocket fan-out eklemek sunum katmanini dayaniklilik
//     garantisine baglardi.
//  2. Ana consumer `--scale consumer=3` ile calistiginda her replika akisin
//     sadece bir kismini gorur - harita eksik olurdu.
//
// Bu yuzden KENDI consumer group'unda (pulsecity-webviz) calisir: ayni
// topic'ten bagimsiz olarak tum akisi okur, ana consumer'in offset'lerine
// dokunmaz, ondan mesaj calmaz.
package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"log"
	"math"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/segmentio/kafka-go"
)

//go:embed static/index.html
var indexHTML []byte

var (
	metricConnectedClients = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "pulsecity_webviz_connected_clients",
		Help: "Haritaya bagli WebSocket istemci sayisi",
	})
	metricBroadcasts = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pulsecity_webviz_broadcasts_total",
		Help: "Yayinlanan snapshot sayisi",
	})
	metricConsumed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pulsecity_webviz_messages_consumed_total",
		Help: "Kafka'dan okunan ping sayisi",
	})
	metricDroppedClients = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pulsecity_webviz_dropped_clients_total",
		Help: "Yayina yetisemedigi icin dusurulen istemci sayisi",
	})
)

// VehiclePing, producer'in Kafka'ya yazdigi event ile ayni sema.
type VehiclePing struct {
	VehicleID string    `json:"vehicle_id"`
	Lat       float64   `json:"lat"`
	Lng       float64   `json:"lng"`
	SpeedKmh  float64   `json:"speed_kmh"`
	Heading   int       `json:"heading"`
	Timestamp time.Time `json:"timestamp"`
	ZoneID    string    `json:"zone_id"`
}

// --- Yayinlanan snapshot semasi ---------------------------------------------

type zoneOut struct {
	ID       string  `json:"id"`
	Lat      float64 `json:"lat"`
	Lng      float64 `json:"lng"`
	Radius   float64 `json:"radius"` // metre - haritadaki cemberin yaricapi
	Pings    int     `json:"pings"`
	AvgSpeed float64 `json:"avg_speed"`
	Baseline float64 `json:"baseline"`
	Anomaly  bool    `json:"anomaly"`
}

type statsOut struct {
	MsgPerSec int `json:"msg_per_sec"`
	Vehicles  int `json:"vehicles"`
	Clients   int `json:"clients"`
	Shown     int `json:"shown"`
}

// Snapshot'in kaynagi: canli Kafka akisi mi, ScyllaDB'den okunan gecmis mi.
// Tarayici tarafi ayni cizim koduyla ikisini de isler, yalnizca etiketleri
// ve anomali gosterimini bu alana gore degistirir.
const (
	modeLive    = "live"
	modeHistory = "history"
)

type snapshot struct {
	Ts   int64  `json:"ts"`
	Mode string `json:"mode"`
	// Araclar kompakt dizi olarak gonderilir ([lat, lng, hiz]) - obje olarak
	// gondermek alan adlarini her arac icin tekrarlar ve yuku ~3 katina cikarir.
	Vehicles [][]float64 `json:"vehicles"`
	Zones    []zoneOut   `json:"zones"`
	Stats    statsOut    `json:"stats"`
}

// --- Bellekteki durum -------------------------------------------------------

type vehicleSnapshot struct {
	lat, lng, speed float64
	seen            time.Time
}

type zoneAgg struct {
	// Pencere sayaclari - her yayindan sonra sifirlanir
	pings          int
	speedSum       float64
	latSum, lngSum float64

	// Bolge merkezi: producer'daki zone tablosunu burada TEKRARLAMAK yerine
	// gelen ping'lerin ortalamasindan turetilir. Boylece producer'daki sehir
	// ya da bolge tanimi degistiginde burada elle guncelleme gerekmez.
	centerLat, centerLng float64
	haveCenter           bool

	// Yaricap da ayni sekilde veriden turetilir: pencere icindeki en uzak
	// ping'in merkeze mesafesi. Producer'daki zoneRadiusMeters degeri
	// degistiginde harita kendiliginden uyum saglar.
	windowMaxDist float64
	radius        float64
}

const metersPerDegreeLat = 111320.0

func distanceMeters(lat1, lng1, lat2, lng2 float64) float64 {
	dNorth := (lat1 - lat2) * metersPerDegreeLat
	dEast := (lng1 - lng2) * metersPerDegreeLat * math.Cos(lat2*math.Pi/180)
	return math.Hypot(dNorth, dEast)
}

type zoneSignal struct {
	baseline float64
	anomaly  bool
}

type hub struct {
	mu      sync.RWMutex
	latest  map[string]*vehicleSnapshot
	zones   map[string]*zoneAgg
	sampled map[string]bool

	maxVehicles int

	clientsMu sync.RWMutex
	clients   map[*client]struct{}
}

func newHub(maxVehicles int) *hub {
	return &hub{
		latest:      map[string]*vehicleSnapshot{},
		zones:       map[string]*zoneAgg{},
		sampled:     map[string]bool{},
		maxVehicles: maxVehicles,
		clients:     map[*client]struct{}{},
	}
}

func (h *hub) ingest(p VehiclePing) {
	h.mu.Lock()
	defer h.mu.Unlock()

	v, ok := h.latest[p.VehicleID]
	if !ok {
		v = &vehicleSnapshot{}
		h.latest[p.VehicleID] = v
		// Haritada gosterilecek araclar ILK GORULDUKLERINDE secilir ve sabit
		// kalir. Her karede rastgele ornek almak noktalarin titremesine yol
		// acardi; bu yontemle ayni araclar kararli sekilde izlenir.
		if len(h.sampled) < h.maxVehicles {
			h.sampled[p.VehicleID] = true
		}
	}
	v.lat, v.lng, v.speed, v.seen = p.Lat, p.Lng, p.SpeedKmh, time.Now()

	z, ok := h.zones[p.ZoneID]
	if !ok {
		z = &zoneAgg{}
		h.zones[p.ZoneID] = z
	}
	z.pings++
	z.speedSum += p.SpeedKmh
	z.latSum += p.Lat
	z.lngSum += p.Lng

	// Merkez bir onceki pencereden biliniyorsa, bu pencerenin en uzak ping'ini
	// izle - yaricap bundan turetilir.
	if z.haveCenter {
		if d := distanceMeters(p.Lat, p.Lng, z.centerLat, z.centerLng); d > z.windowMaxDist {
			z.windowMaxDist = d
		}
	}
}

// buildSnapshot, pencere sayaclarindan yayinlanacak snapshot'i uretir ve
// sayaclari sifirlar. windowSeconds, msg/sn hesabi icin gecen sure.
func (h *hub) buildSnapshot(signals map[string]zoneSignal, windowSeconds float64, clients int) snapshot {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Uzun suredir ping gelmeyen araclari dusur (or. producer daha az arac ile
	// yeniden baslatilmis olabilir) - yoksa haritada donmus noktalar kalir.
	cutoff := time.Now().Add(-30 * time.Second)
	for id, v := range h.latest {
		if v.seen.Before(cutoff) {
			delete(h.latest, id)
			delete(h.sampled, id)
		}
	}

	vehicles := make([][]float64, 0, len(h.sampled))
	for id := range h.sampled {
		v, ok := h.latest[id]
		if !ok {
			continue
		}
		vehicles = append(vehicles, []float64{
			round(v.lat, 5), round(v.lng, 5), round(v.speed, 1),
		})
	}

	totalPings := 0
	zoneList := make([]zoneOut, 0, len(h.zones))
	for id, z := range h.zones {
		totalPings += z.pings

		if z.pings > 0 {
			wLat := z.latSum / float64(z.pings)
			wLng := z.lngSum / float64(z.pings)
			if !z.haveCenter {
				z.centerLat, z.centerLng, z.haveCenter = wLat, wLng, true
			} else {
				// Merkez pencereden pencereye zipllamasin diye yumusatilir.
				z.centerLat = 0.1*wLat + 0.9*z.centerLat
				z.centerLng = 0.1*wLng + 0.9*z.centerLng
			}
		}

		if z.windowMaxDist > 0 {
			if z.radius == 0 {
				z.radius = z.windowMaxDist
			} else {
				z.radius = 0.2*z.windowMaxDist + 0.8*z.radius
			}
		}

		avg := 0.0
		if z.pings > 0 {
			avg = z.speedSum / float64(z.pings)
		}
		sig := signals[id]
		zoneList = append(zoneList, zoneOut{
			ID:       id,
			Lat:      round(z.centerLat, 5),
			Lng:      round(z.centerLng, 5),
			Radius:   round(z.radius, 0),
			Pings:    z.pings,
			AvgSpeed: round(avg, 1),
			Baseline: round(sig.baseline, 1),
			Anomaly:  sig.anomaly,
		})

		// Pencere sayaclarini sifirla; merkez ve yaricap korunur.
		z.pings, z.speedSum, z.latSum, z.lngSum, z.windowMaxDist = 0, 0, 0, 0, 0
	}

	msgPerSec := 0
	if windowSeconds > 0 {
		msgPerSec = int(float64(totalPings) / windowSeconds)
	}

	return snapshot{
		Ts:       time.Now().Unix(),
		Mode:     modeLive,
		Vehicles: vehicles,
		Zones:    zoneList,
		Stats: statsOut{
			MsgPerSec: msgPerSec,
			Vehicles:  len(h.latest),
			Clients:   clients,
			Shown:     len(vehicles),
		},
	}
}

func round(v float64, decimals int) float64 {
	p := math.Pow(10, float64(decimals))
	return math.Round(v*p) / p
}

// --- WebSocket --------------------------------------------------------------

type client struct {
	conn *websocket.Conn
	send chan []byte
}

var upgrader = websocket.Upgrader{
	// Harita herkese acik salt-okunur bir gosterge; origin kisitlamasi yok.
	CheckOrigin: func(r *http.Request) bool { return true },
}

func (h *hub) addClient(c *client) {
	h.clientsMu.Lock()
	h.clients[c] = struct{}{}
	n := len(h.clients)
	h.clientsMu.Unlock()
	metricConnectedClients.Set(float64(n))
}

func (h *hub) removeClient(c *client) {
	h.clientsMu.Lock()
	if _, ok := h.clients[c]; ok {
		delete(h.clients, c)
		close(c.send)
	}
	n := len(h.clients)
	h.clientsMu.Unlock()
	metricConnectedClients.Set(float64(n))
}

func (h *hub) clientCount() int {
	h.clientsMu.RLock()
	defer h.clientsMu.RUnlock()
	return len(h.clients)
}

func (h *hub) broadcast(payload []byte) {
	h.clientsMu.RLock()
	targets := make([]*client, 0, len(h.clients))
	for c := range h.clients {
		targets = append(targets, c)
	}
	h.clientsMu.RUnlock()

	for _, c := range targets {
		select {
		case c.send <- payload:
		default:
			// Yavas istemci tum yayini bloke etmesin: tamponu dolmussa
			// baglantiyi dusururuz, istemci tarafi otomatik yeniden baglanir.
			metricDroppedClients.Inc()
			c.conn.Close()
		}
	}
}

func (h *hub) serveWS(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("[webviz] websocket upgrade hatasi: %v", err)
		return
	}
	c := &client{conn: conn, send: make(chan []byte, 8)}
	h.addClient(c)

	// Okuma tarafi: istemciden veri beklemiyoruz ama baglanti kapanisini
	// yakalamak icin okumak gerekiyor.
	go func() {
		defer func() {
			h.removeClient(c)
			conn.Close()
		}()
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}()

	for payload := range c.send {
		if err := conn.SetWriteDeadline(time.Now().Add(10 * time.Second)); err != nil {
			return
		}
		if err := conn.WriteMessage(websocket.TextMessage, payload); err != nil {
			return
		}
	}
}

// --- Prometheus'tan anomali sinyali -----------------------------------------

// fetchZoneSignals, Faz 8'in urettigi anomali ve baseline metriklerini
// Prometheus'tan ceker.
//
// webviz kendi EMA'sini TUTMAZ: anomali mantigi tek yerde (consumer) kalsin
// diye. Iki kopya olsaydi harita ile Grafana farkli seyler soyleyebilirdi.
func fetchZoneSignals(ctx context.Context, promURL string) map[string]zoneSignal {
	out := map[string]zoneSignal{}
	if promURL == "" {
		return out
	}

	query := `{__name__=~"pulsecity_zone_anomaly_detected|pulsecity_zone_speed_baseline_kmh"}`
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, promURL+"/api/v1/query", nil)
	if err != nil {
		return out
	}
	q := req.URL.Query()
	q.Set("query", query)
	req.URL.RawQuery = q.Encode()

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		// Prometheus gecici olarak erisilemezse harita calismaya devam eder,
		// sadece anomali rengi guncellenmez.
		return out
	}
	defer resp.Body.Close()

	var parsed struct {
		Data struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
				Value  []interface{}     `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return out
	}

	for _, r := range parsed.Data.Result {
		zone := r.Metric["zone_id"]
		if zone == "" || len(r.Value) < 2 {
			continue
		}
		raw, ok := r.Value[1].(string)
		if !ok {
			continue
		}
		val, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			continue
		}
		s := out[zone]
		switch r.Metric["__name__"] {
		case "pulsecity_zone_anomaly_detected":
			s.anomaly = val >= 1
		case "pulsecity_zone_speed_baseline_kmh":
			s.baseline = val
		}
		out[zone] = s
	}
	return out
}

// --- main -------------------------------------------------------------------

func getenv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}

func main() {
	brokers := getenv("KAFKA_BROKERS", "localhost:9092")
	topic := getenv("KAFKA_TOPIC", "vehicle-pings")
	groupID := getenv("KAFKA_GROUP_ID", "pulsecity-webviz")
	promURL := getenv("PROM_URL", "http://prometheus:9090")
	listenAddr := getenv("LISTEN_ADDR", ":8080")
	intervalMs, _ := strconv.Atoi(getenv("BROADCAST_INTERVAL_MS", "1000"))
	maxVehicles, _ := strconv.Atoi(getenv("MAX_VEHICLES_ON_MAP", "1500"))

	scyllaHosts := getenv("SCYLLA_HOSTS", "localhost:9042")
	histWindowMs, _ := strconv.Atoi(getenv("HISTORY_WINDOW_MS", "2000"))
	histMaxAgeSec, _ := strconv.Atoi(getenv("HISTORY_MAX_AGE_SECONDS", "1800"))
	// Bolge basina satir tavani: 50k/sn'de 2 saniyelik pencere bolge basina
	// ~11k satir eder. Tavan olmadan tek bir kare icin yuz binlerce satir
	// okunurdu; ortalama hiz ve merkez bu ornek uzerinden de saglikli cikar.
	histRowsPerZone, _ := strconv.Atoi(getenv("HISTORY_ROWS_PER_ZONE", "2000"))

	log.Printf("[webviz] baslatiliyor: brokers=%s topic=%s group=%s yayin=%dms max_arac=%d",
		brokers, topic, groupID, intervalMs, maxVehicles)

	h := newHub(maxVehicles)
	history := newHistoryStore(
		time.Duration(histWindowMs)*time.Millisecond,
		time.Duration(histMaxAgeSec)*time.Second,
		histRowsPerZone,
		maxVehicles,
	)

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{brokers},
		Topic:   topic,
		GroupID: groupID,
		// Canli goruntu istiyoruz: gecmisi bastan oynatmak yerine akisin
		// sonundan basla. Burada zero-loss garantisi GEREKMIYOR, bu yuzden
		// ana consumer'in aksine otomatik commit kullaniyoruz.
		StartOffset:    kafka.LastOffset,
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: time.Second,
	})
	defer reader.Close()

	ctx := context.Background()

	// Scylla baglantisi arka planda kurulur; hazir olana kadar harita canli
	// modda calisir, yalnizca zaman kaydiricisi kapali kalir.
	go history.connect(ctx, scyllaHosts)

	go func() {
		for {
			m, err := reader.ReadMessage(ctx)
			if err != nil {
				log.Printf("[webviz] kafka okuma hatasi: %v", err)
				time.Sleep(time.Second)
				continue
			}
			var p VehiclePing
			if err := json.Unmarshal(m.Value, &p); err != nil {
				continue
			}
			h.ingest(p)
			metricConsumed.Inc()
		}
	}()

	// Yayin dongusu: saniyede 5000+ mesaji tarayiciya iletmek mumkun degil.
	// Bunun yerine bellekte "her aracin son konumu" tutulur ve sabit araliklarla
	// tek bir snapshot yayinlanir - ag yuku mesaj hizindan bagimsiz hale gelir.
	interval := time.Duration(intervalMs) * time.Millisecond
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		last := time.Now()
		for range ticker.C {
			now := time.Now()
			window := now.Sub(last).Seconds()
			last = now

			sigCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			signals := fetchZoneSignals(sigCtx, promURL)
			cancel()

			snap := h.buildSnapshot(signals, window, h.clientCount())
			payload, err := json.Marshal(snap)
			if err != nil {
				continue
			}
			h.broadcast(payload)
			metricBroadcasts.Inc()
		}
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("/api/history", history.handleSnapshot)
	mux.HandleFunc("/api/history/meta", history.handleMeta)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(indexHTML)
	})
	mux.HandleFunc("/ws", h.serveWS)
	mux.Handle("/metrics", promhttp.Handler())

	log.Printf("[webviz] harita hazir: http://localhost%s", listenAddr)
	if err := http.ListenAndServe(listenAddr, mux); err != nil {
		log.Fatalf("[webviz] http server hatasi: %v", err)
	}
}

// PulseCity — Zaman yolculugu (gecmise bakma)
//
// Faz 9'a kadar ScyllaDB'ye yazilan veri hicbir yerde OKUNMUYORDU: harita
// Kafka'dan canli akisi izliyor, consumer ise kayitlari yalnizca yaziyordu.
// Depolama katmani bu yuzden "yaz ve unut" durumundaydi.
//
// Bu dosya o boslugu kapatir: haritadaki zaman kaydiricisi geriye cekildiginde
// istenen ana ait konumlar Kafka'dan degil ScyllaDB'den okunur.
//
// Sorgu deseni tablo semasiyla birebir ortusuyor:
//
//	PRIMARY KEY (zone_id, ping_time, vehicle_id)  WITH CLUSTERING ORDER BY (ping_time DESC)
//
// zone_id partition key oldugu icin her bolge sorgusu TEK partition'a iner;
// ping_time clustering key ve DESC sirali oldugu icin zaman araligi diskte
// zaten bitisik duran satirlardan okunur. Yani "Uskudar'in 14:32'si" sorgusu
// tarama degil, siralanmis bir aralik okumasidir - semanin bu erisim icin
// tasarlanmis olmasinin karsiligi burada aliniyor.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gocql/gocql"
)

// pingRow, vehicle_pings tablosundan okunan tek satir.
type pingRow struct {
	zoneID    string
	vehicleID string
	lat, lng  float64
	speed     float64
	pingTime  time.Time
}

// historyQuerier, okuma yolunu ScyllaDB'den ayirir. Boylece snapshot uretimi
// saf bir fonksiyon olarak kalir ve testlerde ayakta bir veritabani gerekmez.
type historyQuerier interface {
	zoneIDs(ctx context.Context) ([]string, error)
	zonePings(ctx context.Context, zoneID string, from, to time.Time, limit int) ([]pingRow, error)
}

var errHistoryUnavailable = errors.New("gecmis okuma hazir degil")

// --- ScyllaDB uygulamasi ----------------------------------------------------

type scyllaHistory struct {
	session *gocql.Session

	// Bolge listesi nadiren degisir; her karede DISTINCT sorgusu atmamak icin
	// kisa sureli onbellekte tutulur.
	zonesMu     sync.Mutex
	zonesCache  []string
	zonesExpiry time.Time
}

const zoneCacheTTL = time.Minute

func (s *scyllaHistory) zoneIDs(ctx context.Context) ([]string, error) {
	s.zonesMu.Lock()
	defer s.zonesMu.Unlock()

	if time.Now().Before(s.zonesExpiry) && len(s.zonesCache) > 0 {
		return s.zonesCache, nil
	}

	// DISTINCT partition key sorgusu yalnizca partition anahtarlarini okur,
	// satirlari taramaz - bolge sayisi kadar ucuzdur.
	iter := s.session.Query(`SELECT DISTINCT zone_id FROM vehicle_pings`).
		WithContext(ctx).Iter()

	var (
		zones []string
		zone  string
	)
	for iter.Scan(&zone) {
		zones = append(zones, zone)
	}
	if err := iter.Close(); err != nil {
		return nil, err
	}

	sort.Strings(zones)
	s.zonesCache, s.zonesExpiry = zones, time.Now().Add(zoneCacheTTL)
	return zones, nil
}

func (s *scyllaHistory) zonePings(ctx context.Context, zoneID string, from, to time.Time, limit int) ([]pingRow, error) {
	// LIMIT, clustering order DESC ile birlestiginde araligin EN YENI
	// satirlarini verir - "su andaki konum" sorusunun karsiligi tam olarak bu.
	iter := s.session.Query(
		`SELECT vehicle_id, lat, lng, speed_kmh, ping_time
		   FROM vehicle_pings
		  WHERE zone_id = ? AND ping_time >= ? AND ping_time < ?
		  LIMIT ?`,
		zoneID, from, to, limit,
	).WithContext(ctx).Iter()

	rows := make([]pingRow, 0, 64)
	row := pingRow{zoneID: zoneID}
	for iter.Scan(&row.vehicleID, &row.lat, &row.lng, &row.speed, &row.pingTime) {
		rows = append(rows, row)
	}
	return rows, iter.Close()
}

// --- Gecmis deposu ----------------------------------------------------------

type historyStore struct {
	mu sync.RWMutex
	q  historyQuerier

	window      time.Duration // her karede okunacak zaman araligi
	maxAge      time.Duration // kaydiricinin geriye gidebilecegi en uzak nokta
	rowsPerZone int           // bolge basina okunacak satir tavani
	maxVehicles int           // haritada gosterilecek arac tavani
}

func newHistoryStore(window, maxAge time.Duration, rowsPerZone, maxVehicles int) *historyStore {
	return &historyStore{
		window:      window,
		maxAge:      maxAge,
		rowsPerZone: rowsPerZone,
		maxVehicles: maxVehicles,
	}
}

func (s *historyStore) setQuerier(q historyQuerier) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.q = q
}

func (s *historyStore) querier() historyQuerier {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.q
}

func (s *historyStore) ready() bool { return s.querier() != nil }

// connect, ScyllaDB oturumunu ARKA PLANDA kurar.
//
// Bilerek engellemiyor: gecmis okuma haritanin kritik yolu degil. Scylla
// gecikirse ya da dusserse canli harita calismaya devam etmeli, yalnizca
// zaman kaydiricisi devre disi kalmali. Ayni yaklasim Prometheus icin de
// gecerli (bkz. fetchZoneSignals).
func (s *historyStore) connect(ctx context.Context, hosts string) {
	cluster := gocql.NewCluster(strings.Split(hosts, ",")...)
	cluster.Keyspace = "pulsecity"
	// RF=1 oldugu icin Quorum ile One ayni node'a gider; okuma yolu salt
	// gorsel oldugundan One yeterli ve daha ucuz.
	cluster.Consistency = gocql.One
	cluster.Timeout = 5 * time.Second
	cluster.ConnectTimeout = 5 * time.Second

	for attempt := 1; ; attempt++ {
		session, err := cluster.CreateSession()
		if err == nil {
			s.setQuerier(&scyllaHistory{session: session})
			log.Printf("[webviz] gecmis okuma hazir (scylla=%s)", hosts)
			return
		}
		// Ilk denemeler ve sonrasinda dakikada bir loglanir - saniyede bir
		// satir yazip loglari bogmamak icin.
		if attempt <= 3 || attempt%12 == 0 {
			log.Printf("[webviz] scylla baglantisi kurulamadi (deneme %d), gecmis okuma kapali: %v", attempt, err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Second):
		}
	}
}

// snapshotAt, verilen ana ait snapshot'i ScyllaDB'den uretir.
func (s *historyStore) snapshotAt(ctx context.Context, at time.Time) (snapshot, error) {
	q := s.querier()
	if q == nil {
		return snapshot{}, errHistoryUnavailable
	}

	zones, err := q.zoneIDs(ctx)
	if err != nil {
		return snapshot{}, err
	}

	from, to := at.Add(-s.window), at

	// Bolgeler ayri partition'lar; sorgular paralel gider. Bolge sayisi
	// (9 ilce) kucuk oldugu icin ek bir havuz gerekmiyor.
	var (
		wg     sync.WaitGroup
		mu     sync.Mutex
		byZone = make(map[string][]pingRow, len(zones))
		firstE error
	)
	for _, zone := range zones {
		wg.Add(1)
		go func(zone string) {
			defer wg.Done()
			rows, err := q.zonePings(ctx, zone, from, to, s.rowsPerZone)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				if firstE == nil {
					firstE = err
				}
				return
			}
			byZone[zone] = rows
		}(zone)
	}
	wg.Wait()

	// Bolgelerin bir kismi okunamadiysa elde kalanla devam edilir: eksik bir
	// harita, bos bir haritadan iyidir. Hicbiri okunamadiysa hata dondurulur.
	if firstE != nil && len(byZone) == 0 {
		return snapshot{}, firstE
	}

	return buildHistorySnapshot(at, s.window, byZone, s.maxVehicles), nil
}

// buildHistorySnapshot, okunan satirlardan canli yayinla AYNI semada bir
// snapshot uretir. Ayni sema sayesinde tarayici tarafi tek bir cizim koduyla
// hem canli hem gecmis kareyi isleyebiliyor.
//
// Anomali sinyali bilerek TASINMIYOR: Prometheus yalnizca ANLIK degeri
// tutuyor, onu gecmis bir kareye yapistirmak "14:32'de anomali vardi" gibi
// yanlis bir sey soylerdi. Gecmis modda bolge rengi sadece olculen hizdan
// turetilir.
func buildHistorySnapshot(at time.Time, window time.Duration, byZone map[string][]pingRow, maxVehicles int) snapshot {
	type vehPos struct {
		lat, lng, speed float64
		t               time.Time
	}

	latest := make(map[string]vehPos)
	zoneList := make([]zoneOut, 0, len(byZone))
	totalRows := 0

	zoneIDs := make([]string, 0, len(byZone))
	for id := range byZone {
		zoneIDs = append(zoneIDs, id)
	}
	sort.Strings(zoneIDs)

	for _, id := range zoneIDs {
		rows := byZone[id]
		totalRows += len(rows)

		var latSum, lngSum, speedSum float64
		for _, r := range rows {
			latSum += r.lat
			lngSum += r.lng
			speedSum += r.speed

			// Ayni arac pencerede birden fazla kez ping atmis olabilir (hatta
			// bolge degistirmis olabilir); en yeni konum kazanir.
			if prev, ok := latest[r.vehicleID]; ok && !r.pingTime.After(prev.t) {
				continue
			}
			latest[r.vehicleID] = vehPos{r.lat, r.lng, r.speed, r.pingTime}
		}

		z := zoneOut{ID: id, Pings: len(rows)}
		if n := float64(len(rows)); n > 0 {
			cLat, cLng := latSum/n, lngSum/n
			z.Lat, z.Lng = round(cLat, 5), round(cLng, 5)
			z.AvgSpeed = round(speedSum/n, 1)

			// Yaricap canli yoldaki gibi veriden turetilir: merkeze en uzak
			// ping. Boylece producer'daki bolge yaricapi degisirse gecmis
			// kareler de kendiliginden uyar.
			maxDist := 0.0
			for _, r := range rows {
				if d := distanceMeters(r.lat, r.lng, cLat, cLng); d > maxDist {
					maxDist = d
				}
			}
			z.Radius = round(maxDist, 0)
		}
		zoneList = append(zoneList, z)
	}

	// Gosterilecek araclar kimlige gore siralanip bastan alinir. Rastgele
	// secim, kareler arasinda farkli araclari secip noktalarin ziplamasina
	// yol acardi - canli yoldaki "sabit ornek" mantiginin gecmis karsiligi.
	ids := make([]string, 0, len(latest))
	for id := range latest {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	if len(ids) > maxVehicles {
		ids = ids[:maxVehicles]
	}

	vehicles := make([][]float64, 0, len(ids))
	for _, id := range ids {
		v := latest[id]
		vehicles = append(vehicles, []float64{round(v.lat, 5), round(v.lng, 5), round(v.speed, 1)})
	}

	msgPerSec := 0
	if sec := window.Seconds(); sec > 0 {
		msgPerSec = int(float64(totalRows) / sec)
	}

	return snapshot{
		Ts:       at.Unix(),
		Mode:     modeHistory,
		Vehicles: vehicles,
		Zones:    zoneList,
		Stats: statsOut{
			MsgPerSec: msgPerSec,
			Vehicles:  len(latest),
			Shown:     len(vehicles),
		},
	}
}

// --- HTTP -------------------------------------------------------------------

// handleMeta, arayuze kaydiricinin gosterilip gosterilmeyecegini bildirir.
// Scylla erisilemezse arayuz kaydiriciyi hic cizmez.
func (s *historyStore) handleMeta(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"enabled":         s.ready(),
		"max_age_seconds": int(s.maxAge.Seconds()),
		"window_seconds":  s.window.Seconds(),
	})
}

func (s *historyStore) handleSnapshot(w http.ResponseWriter, r *http.Request) {
	if !s.ready() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{
			"error": "gecmis okuma su an kullanilamiyor (scylla baglantisi yok)",
		})
		return
	}

	atUnix, err := strconv.ParseInt(r.URL.Query().Get("at"), 10, 64)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "'at' parametresi unix saniye olmali",
		})
		return
	}

	at := time.Unix(atUnix, 0)
	now := time.Now()

	// Istek araligi sinirlanir: aksi halde tek bir URL ile aylar oncesine ait
	// devasa bir tarama tetiklenebilirdi.
	if oldest := now.Add(-s.maxAge); at.Before(oldest) {
		at = oldest
	}
	if at.After(now) {
		at = now
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	snap, err := s.snapshotAt(ctx, at)
	if err != nil {
		log.Printf("[webviz] gecmis sorgusu basarisiz: %v", err)
		writeJSON(w, http.StatusBadGateway, map[string]string{
			"error": "gecmis verisi okunamadi",
		})
		return
	}
	writeJSON(w, http.StatusOK, snap)
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

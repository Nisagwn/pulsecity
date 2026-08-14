package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// fakeQuerier, ScyllaDB yerine gecer: gecmis mantigini ayakta bir veritabani
// olmadan test edebilmek icin okuma yolu arayuz arkasina alinmisti.
type fakeQuerier struct {
	zones []string
	rows  map[string][]pingRow
	fail  map[string]error

	mu    sync.Mutex
	calls []struct {
		zone     string
		from, to time.Time
		limit    int
	}
}

func (f *fakeQuerier) zoneIDs(context.Context) ([]string, error) { return f.zones, nil }

func (f *fakeQuerier) zonePings(_ context.Context, zone string, from, to time.Time, limit int) ([]pingRow, error) {
	f.mu.Lock()
	f.calls = append(f.calls, struct {
		zone     string
		from, to time.Time
		limit    int
	}{zone, from, to, limit})
	f.mu.Unlock()

	if err, ok := f.fail[zone]; ok {
		return nil, err
	}
	return f.rows[zone], nil
}

func row(zone, vehicle string, lat, lng, speed float64, t time.Time) pingRow {
	return pingRow{zoneID: zone, vehicleID: vehicle, lat: lat, lng: lng, speed: speed, pingTime: t}
}

func TestHistorySnapshotZoneStats(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	byZone := map[string][]pingRow{
		"istanbul-uskudar": {
			row("istanbul-uskudar", "v-1", 41.00, 29.00, 30, now),
			row("istanbul-uskudar", "v-2", 41.02, 29.04, 50, now),
		},
	}

	snap := buildHistorySnapshot(now, 2*time.Second, byZone, 100)

	if snap.Mode != modeHistory {
		t.Errorf("mode=%q, %q olmaliydi", snap.Mode, modeHistory)
	}
	if len(snap.Zones) != 1 {
		t.Fatalf("1 bolge bekleniyordu, %d geldi", len(snap.Zones))
	}
	z := snap.Zones[0]
	if z.Pings != 2 {
		t.Errorf("ping sayisi %d, 2 olmaliydi", z.Pings)
	}
	if math.Abs(z.AvgSpeed-40) > 1e-9 {
		t.Errorf("ortalama hiz %.2f, 40 olmaliydi", z.AvgSpeed)
	}
	// Merkez canli yoldaki gibi ping'lerin ortalamasindan turetilmeli.
	if math.Abs(z.Lat-41.01) > 1e-4 || math.Abs(z.Lng-29.02) > 1e-4 {
		t.Errorf("merkez (%.5f, %.5f), (41.01, 29.02) olmaliydi", z.Lat, z.Lng)
	}
	if z.Radius <= 0 {
		t.Errorf("yaricap %.0f, veriden turetilmeliydi", z.Radius)
	}
}

// Prometheus yalnizca ANLIK anomali degerini tutuyor; onu gecmis bir kareye
// tasimak "o an anomali vardi" gibi yanlis bir sey soylerdi.
func TestHistorySnapshotCarriesNoAnomalySignal(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	byZone := map[string][]pingRow{
		"z1": {row("z1", "v-1", 41, 29, 5, now)}, // cok dusuk hiz
	}

	z := buildHistorySnapshot(now, time.Second, byZone, 10).Zones[0]

	if z.Anomaly {
		t.Error("gecmis karede anomali bayragi tasinmamali")
	}
	if z.Baseline != 0 {
		t.Errorf("baseline %.1f, gecmis karede 0 olmaliydi", z.Baseline)
	}
}

// Ayni arac pencere icinde birden fazla ping atmis (hatta bolge degistirmis)
// olabilir; haritada tek nokta ve EN YENI konum gorunmeli.
func TestHistorySnapshotKeepsNewestPositionPerVehicle(t *testing.T) {
	base := time.Unix(1_700_000_000, 0)
	byZone := map[string][]pingRow{
		"z1": {
			row("z1", "v-1", 41.00, 29.00, 20, base.Add(-2*time.Second)),
			row("z1", "v-1", 41.05, 29.05, 60, base), // en yeni
		},
		"z2": {
			row("z2", "v-1", 40.90, 28.90, 10, base.Add(-time.Second)),
		},
	}

	snap := buildHistorySnapshot(base, 3*time.Second, byZone, 100)

	if len(snap.Vehicles) != 1 {
		t.Fatalf("1 arac bekleniyordu, %d geldi", len(snap.Vehicles))
	}
	got := snap.Vehicles[0]
	if math.Abs(got[0]-41.05) > 1e-9 || math.Abs(got[1]-29.05) > 1e-9 {
		t.Errorf("konum (%.5f, %.5f), en yeni ping (41.05, 29.05) olmaliydi", got[0], got[1])
	}
	if snap.Stats.Vehicles != 1 {
		t.Errorf("takip edilen arac %d, 1 olmaliydi", snap.Stats.Vehicles)
	}
}

// Canli yoldaki "sabit ornek" mantiginin gecmis karsiligi: kareler arasinda
// ayni araclar secilmeli, yoksa noktalar ziplar.
func TestHistorySnapshotVehicleCapIsDeterministic(t *testing.T) {
	const cap = 10
	now := time.Unix(1_700_000_000, 0)

	rows := make([]pingRow, 0, 100)
	for i := 0; i < 100; i++ {
		rows = append(rows, row("z1", fmt.Sprintf("v-%03d", i), 41, 29, 40, now))
	}
	byZone := map[string][]pingRow{"z1": rows}

	first := buildHistorySnapshot(now, time.Second, byZone, cap)
	second := buildHistorySnapshot(now, time.Second, byZone, cap)

	if len(first.Vehicles) != cap {
		t.Fatalf("haritada %d arac var, tavan %d", len(first.Vehicles), cap)
	}
	if first.Stats.Vehicles != 100 {
		t.Errorf("takip edilen arac %d, 100 olmaliydi", first.Stats.Vehicles)
	}
	for i := range first.Vehicles {
		if first.Vehicles[i][0] != second.Vehicles[i][0] || first.Vehicles[i][1] != second.Vehicles[i][1] {
			t.Fatalf("%d. arac kareler arasinda degisti - secim deterministik degil", i)
		}
	}
}

func TestHistorySnapshotAtQueriesRequestedWindow(t *testing.T) {
	at := time.Unix(1_700_000_000, 0)
	fq := &fakeQuerier{
		zones: []string{"z1", "z2"},
		rows:  map[string][]pingRow{"z1": {row("z1", "v-1", 41, 29, 40, at)}},
	}
	s := newHistoryStore(2*time.Second, 30*time.Minute, 500, 100)
	s.setQuerier(fq)

	if _, err := s.snapshotAt(context.Background(), at); err != nil {
		t.Fatalf("beklenmeyen hata: %v", err)
	}

	if len(fq.calls) != 2 {
		t.Fatalf("her bolge icin bir sorgu bekleniyordu, %d sorgu yapildi", len(fq.calls))
	}
	for _, c := range fq.calls {
		if !c.to.Equal(at) {
			t.Errorf("%s: aralik sonu %v, %v olmaliydi", c.zone, c.to, at)
		}
		if want := at.Add(-2 * time.Second); !c.from.Equal(want) {
			t.Errorf("%s: aralik basi %v, %v olmaliydi", c.zone, c.from, want)
		}
		// Satir tavani olmadan tek kare icin yuz binlerce satir okunabilirdi.
		if c.limit != 500 {
			t.Errorf("%s: limit %d, 500 olmaliydi", c.zone, c.limit)
		}
	}
}

// Bir bolge okunamazsa harita tamamen bos kalmamali: eksik bir kare, bos
// kareden iyidir.
func TestHistorySnapshotSurvivesPartialZoneFailure(t *testing.T) {
	at := time.Unix(1_700_000_000, 0)
	fq := &fakeQuerier{
		zones: []string{"z1", "z2"},
		rows:  map[string][]pingRow{"z1": {row("z1", "v-1", 41, 29, 40, at)}},
		fail:  map[string]error{"z2": errors.New("okuma zaman asimi")},
	}
	s := newHistoryStore(time.Second, time.Hour, 500, 100)
	s.setQuerier(fq)

	snap, err := s.snapshotAt(context.Background(), at)
	if err != nil {
		t.Fatalf("kismi hata snapshot'i tamamen dusurmemeliydi: %v", err)
	}
	if len(snap.Zones) != 1 || snap.Zones[0].ID != "z1" {
		t.Errorf("okunabilen bolge dondurulmeliydi, gelen: %+v", snap.Zones)
	}
}

func TestHistoryUnavailableBeforeScyllaConnects(t *testing.T) {
	s := newHistoryStore(time.Second, time.Hour, 500, 100)

	if s.ready() {
		t.Error("baglanti kurulmadan hazir gorunmemeli")
	}
	if _, err := s.snapshotAt(context.Background(), time.Now()); !errors.Is(err, errHistoryUnavailable) {
		t.Errorf("errHistoryUnavailable bekleniyordu, gelen: %v", err)
	}

	rec := httptest.NewRecorder()
	s.handleSnapshot(rec, httptest.NewRequest(http.MethodGet, "/api/history?at=1700000000", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("HTTP %d, 503 olmaliydi", rec.Code)
	}

	// Arayuz kaydiriciyi bu bilgiye gore gizler.
	rec = httptest.NewRecorder()
	s.handleMeta(rec, httptest.NewRequest(http.MethodGet, "/api/history/meta", nil))
	var meta struct {
		Enabled bool `json:"enabled"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&meta); err != nil {
		t.Fatalf("meta cozumlenemedi: %v", err)
	}
	if meta.Enabled {
		t.Error("scylla yokken meta enabled=false olmaliydi")
	}
}

// Tek bir URL ile aylar oncesine ait devasa bir tarama tetiklenememeli.
func TestHistoryHandlerClampsRequestedTime(t *testing.T) {
	maxAge := 30 * time.Minute
	fq := &fakeQuerier{zones: []string{"z1"}, rows: map[string][]pingRow{}}
	s := newHistoryStore(time.Second, maxAge, 500, 100)
	s.setQuerier(fq)

	tooOld := time.Now().Add(-72 * time.Hour).Unix()
	rec := httptest.NewRecorder()
	s.handleSnapshot(rec, httptest.NewRequest(
		http.MethodGet, fmt.Sprintf("/api/history?at=%d", tooOld), nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("HTTP %d, 200 olmaliydi", rec.Code)
	}
	if len(fq.calls) == 0 {
		t.Fatal("sorgu yapilmadi")
	}
	// Istek 3 gun oncesini istedi; sorgu maxAge sinirina cekilmeli.
	limit := time.Now().Add(-maxAge)
	if fq.calls[0].to.Before(limit.Add(-time.Minute)) {
		t.Errorf("sorgu zamani %v, %v sinirina cekilmeliydi", fq.calls[0].to, limit)
	}
}

func TestHistoryHandlerRejectsMissingTimestamp(t *testing.T) {
	s := newHistoryStore(time.Second, time.Hour, 500, 100)
	s.setQuerier(&fakeQuerier{zones: []string{"z1"}})

	rec := httptest.NewRecorder()
	s.handleSnapshot(rec, httptest.NewRequest(http.MethodGet, "/api/history", nil))

	if rec.Code != http.StatusBadRequest {
		t.Errorf("HTTP %d, 400 olmaliydi", rec.Code)
	}
}

package main

import (
	"fmt"
	"math"
	"testing"
)

func ping(id, zone string, lat, lng, speed float64) VehiclePing {
	return VehiclePing{VehicleID: id, ZoneID: zone, Lat: lat, Lng: lng, SpeedKmh: speed}
}

// Saniyede 5000 mesaj tarayiciya iletilemez. Haritada gosterilen arac sayisi
// sert bir tavanla sinirli olmali, ama takip edilen arac sayisi tam kalmali.
func TestSnapshotRespectsVehicleCap(t *testing.T) {
	const cap = 10
	h := newHub(cap)

	for i := 0; i < 100; i++ {
		h.ingest(ping(fmt.Sprintf("v-%d", i), "z1", 41, 29, 50))
	}

	snap := h.buildSnapshot(nil, 1.0, 0)

	if len(snap.Vehicles) != cap {
		t.Errorf("haritada %d arac var, tavan %d", len(snap.Vehicles), cap)
	}
	if snap.Stats.Vehicles != 100 {
		t.Errorf("takip edilen arac %d, 100 olmaliydi", snap.Stats.Vehicles)
	}
	if snap.Stats.Shown != cap {
		t.Errorf("Shown=%d, %d olmaliydi", snap.Stats.Shown, cap)
	}
}

// Gosterilen araclar karelerde degismemeli - her yayinda rastgele ornek almak
// noktalarin titremesine yol acardi.
func TestSampledVehiclesAreStableAcrossSnapshots(t *testing.T) {
	h := newHub(5)
	for i := 0; i < 50; i++ {
		h.ingest(ping(fmt.Sprintf("v-%d", i), "z1", 41, 29, 50))
	}

	h.mu.RLock()
	first := make(map[string]bool, len(h.sampled))
	for id := range h.sampled {
		first[id] = true
	}
	h.mu.RUnlock()

	for round := 0; round < 5; round++ {
		for i := 0; i < 50; i++ {
			h.ingest(ping(fmt.Sprintf("v-%d", i), "z1", 41, 29, 50))
		}
		h.buildSnapshot(nil, 1.0, 0)
	}

	h.mu.RLock()
	defer h.mu.RUnlock()
	if len(h.sampled) != len(first) {
		t.Fatalf("ornek kumesi buyudu: %d -> %d", len(first), len(h.sampled))
	}
	for id := range h.sampled {
		if !first[id] {
			t.Errorf("ornek kumesi degisti, %s sonradan eklendi", id)
		}
	}
}

func TestZoneAggregation(t *testing.T) {
	h := newHub(100)

	h.ingest(ping("v-1", "istanbul-sisli-1", 41.067, 28.995, 30))
	h.ingest(ping("v-2", "istanbul-sisli-1", 41.067, 28.995, 50))
	h.ingest(ping("v-3", "istanbul-kadikoy-1", 40.980, 29.026, 20))

	signals := map[string]zoneSignal{
		"istanbul-sisli-1": {baseline: 45, anomaly: true},
	}
	snap := h.buildSnapshot(signals, 1.0, 0)

	if len(snap.Zones) != 2 {
		t.Fatalf("2 bolge bekleniyordu, %d geldi", len(snap.Zones))
	}

	byID := map[string]zoneOut{}
	for _, z := range snap.Zones {
		byID[z.ID] = z
	}

	sisli := byID["istanbul-sisli-1"]
	if sisli.Pings != 2 {
		t.Errorf("sisli ping sayisi %d, 2 olmaliydi", sisli.Pings)
	}
	if math.Abs(sisli.AvgSpeed-40) > 1e-9 {
		t.Errorf("sisli ortalama hizi %.2f, 40 olmaliydi", sisli.AvgSpeed)
	}
	// Anomali sinyali Prometheus'tan (yani consumer'dan) gelmeli, burada
	// yeniden hesaplanmamali.
	if !sisli.Anomaly || sisli.Baseline != 45 {
		t.Errorf("anomali sinyali tasinmadi: anomaly=%v baseline=%.1f", sisli.Anomaly, sisli.Baseline)
	}

	kadikoy := byID["istanbul-kadikoy-1"]
	if kadikoy.Anomaly {
		t.Error("sinyali olmayan bolge anomali gorunmemeli")
	}
}

// Pencere sayaclari her yayindan sonra sifirlanmali, yoksa "ping/sn" degeri
// kumulatif hale gelir ve surekli buyur.
func TestWindowCountersResetAfterSnapshot(t *testing.T) {
	h := newHub(100)
	for i := 0; i < 10; i++ {
		h.ingest(ping("v-1", "z1", 41, 29, 40))
	}

	if got := h.buildSnapshot(nil, 1.0, 0).Zones[0].Pings; got != 10 {
		t.Fatalf("ilk pencerede %d ping, 10 olmaliydi", got)
	}
	if got := h.buildSnapshot(nil, 1.0, 0).Zones[0].Pings; got != 0 {
		t.Errorf("ikinci pencerede %d ping, sayaclar sifirlanmaliydi", got)
	}
}

// Bolge merkezi producer'daki tabloyu tekrarlamak yerine gelen ping'lerden
// turetilir; sehir/bolge tanimi degistiginde burada elle guncelleme gerekmesin.
func TestZoneCenterDerivedFromPings(t *testing.T) {
	h := newHub(100)
	h.ingest(ping("v-1", "z1", 41.00, 29.00, 40))
	h.ingest(ping("v-2", "z1", 41.02, 29.04, 40))

	z := h.buildSnapshot(nil, 1.0, 0).Zones[0]

	if math.Abs(z.Lat-41.01) > 1e-4 || math.Abs(z.Lng-29.02) > 1e-4 {
		t.Errorf("bolge merkezi (%.5f, %.5f), (41.01, 29.02) olmaliydi", z.Lat, z.Lng)
	}
}

// Yaricap da merkez gibi veriden turetilir; arayuzde sabit bir deger yazili
// kalmasin diye. Ilk pencerede merkez henuz bilinmedigi icin yaricap 0'dir,
// ikinci pencerede olcum baslar.
func TestZoneRadiusDerivedFromPingSpread(t *testing.T) {
	h := newHub(100)

	// 1. pencere: merkezi belirler (yaricap icin henuz referans yok)
	h.ingest(ping("v-1", "z1", 41.0000, 29.0000, 40))
	h.buildSnapshot(nil, 1.0, 0)

	// 2. pencere: merkeze ~1 km uzaklikta bir ping
	h.ingest(ping("v-1", "z1", 41.0000, 29.0000, 40))
	h.ingest(ping("v-2", "z1", 41.0090, 29.0000, 40)) // ~1002 m kuzey
	z := h.buildSnapshot(nil, 1.0, 0).Zones[0]

	if z.Radius < 900 || z.Radius > 1100 {
		t.Errorf("turetilen yaricap %.0f m, ~1000 m olmaliydi", z.Radius)
	}
}

func TestMsgPerSecUsesWindowDuration(t *testing.T) {
	h := newHub(100)
	for i := 0; i < 500; i++ {
		h.ingest(ping("v-1", "z1", 41, 29, 40))
	}

	// 0.5 saniyelik pencerede 500 ping -> 1000 msg/sn
	if got := h.buildSnapshot(nil, 0.5, 0).Stats.MsgPerSec; got != 1000 {
		t.Errorf("msg/sn %d, 1000 olmaliydi", got)
	}
}

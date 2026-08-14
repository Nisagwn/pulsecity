package main

import (
	"math"
	"math/rand"
	"testing"
)

// Faz 9 oncesinde konum degisimi `speed * 0.00001` derece sabitiyle
// hesaplaniyordu; bu, araclarin bildirdikleri hizin ~10 katiyla hareket
// etmesine yol aciyordu. Sayisal grafiklerde gorunmeyen bu hata haritada
// aninda goze batiyordu. Asagidaki test o regresyonu kilitler.
func TestStepMovesAtDeclaredSpeed(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	z := zones[0]

	for _, dt := range []float64{0.2, 0.4, 1.0} {
		v := &vehicleState{
			id: "v-0", zoneIdx: 0,
			lat: z.LatBase, lng: z.LngBase,
			speed: 50, heading: 90,
		}
		startLat, startLng := v.lat, v.lng

		reported := v.step(rng, dt, 1.0)

		got := distanceMeters(v.lat, v.lng, startLat, startLng)
		want := reported * 1000 / 3600 * dt // km/h -> m/s -> metre

		if math.Abs(got-want) > 0.5 {
			t.Errorf("dt=%.1f: arac %.2f m hareket etti, bildirilen hiza gore %.2f m olmaliydi",
				dt, got, want)
		}
	}
}

// Boylam dereceleri enleme gore daralir (Istanbul'un 41. paralelinde 1 derece
// boylam ~84 km, 1 derece enlem ~111 km). Bu olceklemeyi atlamak dogu-bati
// hareketini ~%33 abartir.
func TestLongitudeScaledByLatitude(t *testing.T) {
	const meters = 1000.0
	atLat := 41.0

	_, dLng := metersToDegrees(0, meters, atLat)
	dLat, _ := metersToDegrees(meters, 0, atLat)

	if dLng <= dLat {
		t.Fatalf("41. paralelde ayni metre icin boylam degisimi (%.6f) enlem degisiminden (%.6f) buyuk olmali",
			dLng, dLat)
	}

	wantLng := meters / (metersPerDegreeLat * math.Cos(atLat*math.Pi/180))
	if math.Abs(dLng-wantLng) > 1e-9 {
		t.Errorf("boylam donusumu = %.9f, beklenen %.9f", dLng, wantLng)
	}
}

// Arac kendi bolgesinin disina kalici olarak cikmamali: zoneIdx hic
// degismedigi icin disari cikan bir arac zone_id etiketini yalanlar.
func TestVehicleStaysWithinItsZone(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	const zoneIdx = 3
	z := zones[zoneIdx]

	v := &vehicleState{
		id: "v-1", zoneIdx: zoneIdx,
		lat: z.LatBase, lng: z.LngBase,
		speed: 90, heading: 0, // azami hiz, en zorlayici senaryo
	}

	maxSeen := 0.0
	for i := 0; i < 5000; i++ {
		v.step(rng, 0.4, 1.0)
		if d := distanceMeters(v.lat, v.lng, z.LatBase, z.LngBase); d > maxSeen {
			maxSeen = d
		}
	}

	// Yaricap asildiktan sonra arac merkeze yonlendirilir, ama o adimda zaten
	// alinmis mesafe kadar tasma normaldir. 1.5x guvenli bir ust sinir.
	if maxSeen > zoneRadiusMeters*1.5 {
		t.Errorf("arac bolge merkezinden %.0f m uzaklasti, sinir %.0f m",
			maxSeen, zoneRadiusMeters*1.5)
	}
}

// Bölge çemberleri haritada üst üste binmemeli. Binerse hangi noktanın hangi
// ilçeye ait olduğu okunamaz - önceki "3 ilçe x 3 alt bölge" tasarımının
// sorunu tam olarak buydu: aynı ilçe adını taşıyan üç çember yayılınca en
// dıştaki komşu ilçenin üzerindeymiş gibi görünüyordu.
func TestZoneCirclesDoNotOverlap(t *testing.T) {
	minGap := 2 * zoneRadiusMeters
	for i := 0; i < len(zones); i++ {
		for j := i + 1; j < len(zones); j++ {
			d := distanceMeters(
				zones[i].LatBase, zones[i].LngBase,
				zones[j].LatBase, zones[j].LngBase,
			)
			if d < minGap {
				t.Errorf("%s ile %s çakışıyor: aralarında %.0f m var, en az %.0f m olmalı",
					zones[i].ID, zones[j].ID, d, minGap)
			}
		}
	}
}

// Bölge kimlikleri Kafka mesaj anahtarı ve ScyllaDB partition key'i olarak
// kullanılıyor; tekil ve ASCII olmalılar.
func TestZoneIDsAreUniqueAndASCII(t *testing.T) {
	seen := map[string]bool{}
	for _, z := range zones {
		if seen[z.ID] {
			t.Errorf("bölge kimliği tekrar ediyor: %s", z.ID)
		}
		seen[z.ID] = true

		for _, r := range z.ID {
			if r > 127 {
				t.Errorf("%s ASCII olmayan karakter içeriyor: %q", z.ID, r)
				break
			}
		}
	}
	if len(zones) != 9 {
		t.Errorf("9 bölge bekleniyordu, %d tanımlı", len(zones))
	}
}

func TestNewVehicleAssignsValidZone(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	for i := 0; i < 200; i++ {
		v := newVehicle("v-x", rng)
		if v.zoneIdx < 0 || v.zoneIdx >= len(zones) {
			t.Fatalf("gecersiz zone indeksi: %d", v.zoneIdx)
		}
		z := zones[v.zoneIdx]
		if d := distanceMeters(v.lat, v.lng, z.LatBase, z.LngBase); d > zoneRadiusMeters {
			t.Errorf("yeni arac bolge disinda dogdu: %.0f m", d)
		}
	}
}

// Faz 8 demo modu: tikanik bolgede hem bildirilen hiz hem alinan mesafe ayni
// oranda dusmeli - yoksa harita ile veri celisir (yavas gorunen arac hizli
// hareket eder).
func TestCongestionSlowsReportAndMovementTogether(t *testing.T) {
	z := zones[0]
	newV := func() *vehicleState {
		return &vehicleState{
			id: "v-0", zoneIdx: 0,
			lat: z.LatBase, lng: z.LngBase,
			speed: 60, heading: 45,
		}
	}

	normal := newV()
	normalSpeed := normal.step(rand.New(rand.NewSource(5)), 0.4, 1.0)
	normalDist := distanceMeters(normal.lat, normal.lng, z.LatBase, z.LngBase)

	slow := newV()
	slowSpeed := slow.step(rand.New(rand.NewSource(5)), 0.4, demoCongestionFactor)
	slowDist := distanceMeters(slow.lat, slow.lng, z.LatBase, z.LngBase)

	if math.Abs(slowSpeed-normalSpeed*demoCongestionFactor) > 1e-9 {
		t.Errorf("bildirilen hiz %.2f, beklenen %.2f", slowSpeed, normalSpeed*demoCongestionFactor)
	}
	if math.Abs(slowDist-normalDist*demoCongestionFactor) > 0.01 {
		t.Errorf("alinan mesafe %.3f m, beklenen %.3f m", slowDist, normalDist*demoCongestionFactor)
	}
}

func TestSpeedFactorFollowsCongestedZone(t *testing.T) {
	congestedZone.Store(-1)
	if f := speedFactorFor(2); f != 1.0 {
		t.Errorf("tikaniklik yokken carpan %.2f, 1.0 olmaliydi", f)
	}

	congestedZone.Store(2)
	defer congestedZone.Store(-1)

	if f := speedFactorFor(2); f != demoCongestionFactor {
		t.Errorf("tikanik bolgede carpan %.2f, %.2f olmaliydi", f, demoCongestionFactor)
	}
	if f := speedFactorFor(3); f != 1.0 {
		t.Errorf("komsu bolge etkilenmemeliydi, carpan %.2f", f)
	}
}

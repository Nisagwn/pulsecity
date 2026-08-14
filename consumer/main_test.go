package main

import (
	"encoding/json"
	"math"
	"testing"
)

// warmedBaseline, isinma penceresini sabit bir hizla doldurup baseline'i
// o degere oturtur.
func warmedBaseline(t *testing.T, speed float64) *zoneBaseline {
	t.Helper()
	b := &zoneBaseline{}
	for i := 0; i < baselineWarmupBatches; i++ {
		if b.evaluate(speed) {
			t.Fatalf("isinma penceresinde (batch %d) anomali bildirildi", i+1)
		}
	}
	if math.Abs(b.ema-speed) > 1e-9 {
		t.Fatalf("isinma sonrasi baseline %.4f, beklenen %.4f", b.ema, speed)
	}
	return b
}

// Isinma olmadan baseline 0'dan baslar ve ilk batch'ler her zaman anomali
// gorunur. Bu test o korumanin yerinde oldugunu dogrular.
func TestNoAnomalyDuringWarmup(t *testing.T) {
	b := &zoneBaseline{}
	speeds := []float64{40, 5, 60, 2, 45} // ic ice sert dalgalanmalar

	for i := 0; i < baselineWarmupBatches; i++ {
		if b.evaluate(speeds[i%len(speeds)]) {
			t.Fatalf("isinma sirasinda (batch %d) anomali bildirilmemeliydi", i+1)
		}
	}
}

func TestAnomalyDetectedOnSignificantDrop(t *testing.T) {
	b := warmedBaseline(t, 40)

	// %40'tan az dusus: anomali DEGIL (esik tam sinirda test ediliyor)
	if b.evaluate(40 * 0.75) {
		t.Error("%25 dusus anomali sayilmamaliydi")
	}

	b = warmedBaseline(t, 40)
	// %40'tan fazla dusus: anomali
	if !b.evaluate(40 * 0.5) {
		t.Error("%50 dusus anomali olarak isaretlenmeliydi")
	}
}

// FAZ 8'IN KRITIK TASARIM KARARI:
// Anomali sirasinda baseline GUNCELLENMEZ. Aksi halde uzun suren bir
// sikisiklik yavas yavas "yeni normal" olur, EMA ona yakinsar ve tespit
// sistemi kendi kendini kor eder.
func TestBaselineFrozenDuringAnomaly(t *testing.T) {
	b := warmedBaseline(t, 40)
	before := b.ema

	// Uzun suren agir bir sikisiklik simule ediliyor
	for i := 0; i < 500; i++ {
		if !b.evaluate(5) {
			t.Fatalf("batch %d: anomali beklenirken normal bildirildi (baseline %.2f'ye kaymis olabilir)",
				i+1, b.ema)
		}
	}

	if math.Abs(b.ema-before) > 1e-9 {
		t.Errorf("baseline anomali sirasinda kaydi: %.4f -> %.4f", before, b.ema)
	}
}

// Normal degerler baseline'i guncellemeli - aksi halde baseline hic ogrenmez.
func TestBaselineTracksNormalTraffic(t *testing.T) {
	b := warmedBaseline(t, 40)

	for i := 0; i < 200; i++ {
		if b.evaluate(60) {
			t.Fatalf("batch %d: hiz ARTISI anomali sayilmamali", i+1)
		}
	}

	if b.ema <= 40 {
		t.Errorf("baseline yeni normale dogru ilerlemeliydi, hala %.2f", b.ema)
	}
	if b.ema > 60 {
		t.Errorf("baseline hedefi asti: %.2f", b.ema)
	}
}

// Sikisiklik bittiginde sistem normale donmeli ve ogrenmeye devam etmeli.
func TestRecoveryAfterAnomaly(t *testing.T) {
	b := warmedBaseline(t, 40)

	for i := 0; i < 50; i++ {
		b.evaluate(5) // sikisiklik
	}
	if b.evaluate(40) {
		t.Error("hiz normale dondugunde anomali bitmeliydi")
	}
}

func TestToDLQMessageIsValidJSON(t *testing.T) {
	m := toDLQMessage([]byte(`{"bozuk":`), "json_parse_error: beklenmeyen son")

	var env map[string]string
	if err := json.Unmarshal(m.Value, &env); err != nil {
		t.Fatalf("DLQ zarfi gecerli JSON degil: %v", err)
	}

	for _, k := range []string{"id", "raw_payload", "error_reason", "failed_at_utc"} {
		if env[k] == "" {
			t.Errorf("DLQ zarfinda %q alani bos", k)
		}
	}
	if env["raw_payload"] != `{"bozuk":` {
		t.Errorf("ham payload korunmadi: %q", env["raw_payload"])
	}
}

// Bozuk JSON sessizce dusmemeli: parse edilemeyen mesaj DLQ'ya gitmeli,
// ScyllaDB'ye yazilacak gruba KARISMAMALI.
func TestMalformedPayloadRoutedToDLQ(t *testing.T) {
	inputs := [][]byte{
		[]byte(`{"vehicle_id":"v-1","zone_id":"istanbul-sisli-1","speed_kmh":40}`),
		[]byte(`bu json degil`),
		[]byte(`{"vehicle_id":"v-2","zone_id":"istanbul-sisli-1","speed_kmh":50}`),
	}

	byZone := map[string][]VehiclePing{}
	dlq := 0
	for _, raw := range inputs {
		var p VehiclePing
		if err := json.Unmarshal(raw, &p); err != nil {
			dlq++
			continue
		}
		byZone[p.ZoneID] = append(byZone[p.ZoneID], p)
	}

	if dlq != 1 {
		t.Errorf("DLQ'ya 1 mesaj gitmeliydi, %d gitti", dlq)
	}
	if got := len(byZone["istanbul-sisli-1"]); got != 2 {
		t.Errorf("zone grubunda 2 ping olmaliydi, %d var", got)
	}
}

func TestUpdateZoneAnalyticsHandlesFirstBatch(t *testing.T) {
	// Metrik kaydi da dahil uctan uca calissin; panik atmamali.
	baselineMu.Lock()
	delete(baselines, "test-zone")
	baselineMu.Unlock()

	updateZoneAnalytics("test-zone", []VehiclePing{
		{ZoneID: "test-zone", SpeedKmh: 30},
		{ZoneID: "test-zone", SpeedKmh: 50},
	})

	baselineMu.Lock()
	b := baselines["test-zone"]
	baselineMu.Unlock()

	if b == nil {
		t.Fatal("bolge icin baseline olusturulmadi")
	}
	if math.Abs(b.ema-40) > 1e-9 {
		t.Errorf("ilk batch baseline'i %.2f, ortalama olan 40 olmaliydi", b.ema)
	}
}

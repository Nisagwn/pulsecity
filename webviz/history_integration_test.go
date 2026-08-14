//go:build integration

// Gercek bir ScyllaDB'ye karsi calisan entegrasyon testi.
//
// Birim testler (history_test.go) snapshot mantigini dogruluyor ama sorgu
// METNINI dogrulayamiyor: historyQuerier arayuzu sahte bir uygulamayla
// degistiriliyor. CQL'de bir yazim hatasi ya da desteklenmeyen bir kalip
// (or. LIMIT'te bind parametresi, DISTINCT partition key okumasi) ancak
// gercek veritabaninda ortaya cikar. Bu test o boslugu kapatir.
//
// Calistirma:
//
//	SCYLLA_TEST_HOSTS=localhost:9042 go test -tags=integration ./...
//
// SCYLLA_TEST_HOSTS bos birakilirsa test atlanir - normal `go test` ve CI
// akisi ayakta bir veritabani gerektirmez.
package main

import (
	"context"
	"fmt"
	"math"
	"os"
	"testing"
	"time"

	"github.com/gocql/gocql"
)

func TestIntegrationScyllaHistoryQueries(t *testing.T) {
	hosts := os.Getenv("SCYLLA_TEST_HOSTS")
	if hosts == "" {
		t.Skip("SCYLLA_TEST_HOSTS tanimli degil, entegrasyon testi atlaniyor")
	}

	cluster := gocql.NewCluster(hosts)
	cluster.Keyspace = "pulsecity"
	cluster.Consistency = gocql.One
	cluster.Timeout = 10 * time.Second
	cluster.ConnectTimeout = 10 * time.Second

	session, err := cluster.CreateSession()
	if err != nil {
		t.Fatalf("scylla baglantisi kurulamadi: %v", err)
	}
	// Kapatma `defer` ile DEGIL t.Cleanup ile kaydediliyor: defer'lar
	// t.Cleanup'tan once calisir, oturum erken kapanirsa asagidaki temizlik
	// sorgusu "session has been closed" ile duserdi. Cleanup'lar LIFO
	// sirayla kostugu icin once DELETE, sonra Close calisir.
	t.Cleanup(session.Close)

	// Uretim verisini kirletmemek icin teste ozel bir bolge kimligi.
	zone := fmt.Sprintf("test-zone-%d", time.Now().UnixNano())
	t.Cleanup(func() {
		if err := session.Query(
			`DELETE FROM vehicle_pings WHERE zone_id = ?`, zone).Exec(); err != nil {
			t.Logf("temizlik basarisiz (zone=%s): %v", zone, err)
		}
	})

	base := time.Now().Truncate(time.Millisecond)
	rows := []struct {
		vehicle  string
		lat, lng float64
		speed    float64
		at       time.Time
	}{
		{"v-1", 41.00, 29.00, 20, base.Add(-9 * time.Second)}, // pencere disinda
		{"v-1", 41.02, 29.02, 40, base.Add(-2 * time.Second)},
		{"v-2", 41.04, 29.04, 60, base.Add(-1 * time.Second)},
	}
	for _, r := range rows {
		if err := session.Query(
			`INSERT INTO vehicle_pings
			   (zone_id, ping_time, vehicle_id, lat, lng, speed_kmh, heading, received_at)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			zone, r.at, r.vehicle, r.lat, r.lng, r.speed, 90, time.Now(),
		).Exec(); err != nil {
			t.Fatalf("ornek kayit yazilamadi: %v", err)
		}
	}

	ctx := context.Background()
	store := &scyllaHistory{session: session}

	// 1) DISTINCT partition key okumasi calismali.
	zones, err := store.zoneIDs(ctx)
	if err != nil {
		t.Fatalf("zoneIDs hatasi: %v", err)
	}
	found := false
	for _, z := range zones {
		if z == zone {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("yazilan bolge %q DISTINCT sonucunda yok: %v", zone, zones)
	}

	// 2) Aralik + LIMIT sorgusu yalnizca pencere icindeki satirlari vermeli.
	//    (Pencere disindaki ilk kayit gelmemeli.)
	got, err := store.zonePings(ctx, zone, base.Add(-4*time.Second), base, 100)
	if err != nil {
		t.Fatalf("zonePings hatasi: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("%d satir geldi, 2 olmaliydi (pencere disindaki kayit sizdi mi?)", len(got))
	}

	// 3) Clustering order DESC: en yeni satir bassa gelmeli. Snapshot'taki
	//    "en yeni konum kazanir" mantigi bu siraya dayaniyor.
	if got[0].vehicleID != "v-2" {
		t.Errorf("ilk satir %q, en yeni kayit olan v-2 olmaliydi", got[0].vehicleID)
	}

	// 4) gocql tur eslemeleri (double -> float64, timestamp -> time.Time).
	if math.Abs(got[0].speed-60) > 1e-9 || math.Abs(got[0].lat-41.04) > 1e-9 {
		t.Errorf("alan eslemesi bozuk: %+v", got[0])
	}
	if got[0].pingTime.IsZero() {
		t.Error("ping_time okunamadi")
	}

	// 5) LIMIT'te bind parametresi gecerli olmali.
	limited, err := store.zonePings(ctx, zone, base.Add(-4*time.Second), base, 1)
	if err != nil {
		t.Fatalf("LIMIT bind parametresi calismadi: %v", err)
	}
	if len(limited) != 1 {
		t.Errorf("LIMIT 1 ile %d satir geldi", len(limited))
	}
}

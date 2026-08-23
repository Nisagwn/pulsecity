-- PulseCity keyspace ve tablo tanimi
-- Partition key: zone_id  -> bolge bazli sorgulari hizlandirir, hot-partition riskini
--                            tek bir global sayac yerine cok sayida bolgeye dagitarak azaltir
-- Clustering key: ping_time, vehicle_id -> bolge icinde zamana gore siralama + ayni anda
--                            gelen farkli araclarin cakismasini onler

CREATE KEYSPACE IF NOT EXISTS pulsecity
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
  -- Not: Faz 6'da (canliya alma) replication_factor 3'e cikarilacak (NetworkTopologyStrategy)

USE pulsecity;

-- Veri yasam dongusu politikasi (Faz 13)
--
-- default_time_to_live = 3600 (1 saat)
--   Kalici volume eklendikten sonra bu tablo sinirsiz buyurdu. TTL, haritanin
--   geriye bakabildigi pencereye (webviz HISTORY_MAX_AGE_SECONDS = 1800sn)
--   gore iki kat genis secildi: kaydiricinin ulasabildigi her an icin veri
--   var, otesi ise tutulmuyor.
--
-- TimeWindowCompactionStrategy
--   Zaman serisi + TTL icin dogru strateji budur. Varsayilan SizeTiered,
--   farkli zamanlara ait SSTable'lari birlestirir; TTL ile suresi dolan
--   satirlar taze verinin icine karisir ve alan ancak buyuk, pahali bir
--   compaction sonrasi geri alinir. TWCS her zaman penceresini AYRI bir
--   SSTable'da tutar, boylece suresi tamamen dolmus bir pencere compaction
--   yapilmadan, dosya silinerek geri kazanilir.
--   10 dakikalik pencere: 1 saatlik TTL icin ~6 aktif SSTable demek.
--
-- gc_grace_seconds = 3600 (varsayilan 10 gun)
--   Varsayilan deger, bir node cevrimdisiyken silinen bir satirin "geri
--   dirilmemesi" icin repair penceresi birakir. RF=1 tek node kurulumda
--   replika yok, dolayisiyla o risk de yok - 10 gunluk varsayilan yalnizca
--   tombstone birikmesine ve okuma yavaslamasina yol acardi.
--   NOT: RF=3'e cikilirsa bu deger repair araligina gore yukseltilmelidir.
CREATE TABLE IF NOT EXISTS vehicle_pings (
  zone_id      text,
  ping_time    timestamp,
  vehicle_id   text,
  lat          double,
  lng          double,
  speed_kmh    double,
  heading      int,
  received_at  timestamp,
  PRIMARY KEY (zone_id, ping_time, vehicle_id)
) WITH CLUSTERING ORDER BY (ping_time DESC)
  AND default_time_to_live = 3600
  AND gc_grace_seconds = 3600
  AND compaction = {
    'class': 'TimeWindowCompactionStrategy',
    'compaction_window_unit': 'MINUTES',
    'compaction_window_size': '10'
  };

-- DIKKAT: yukaridaki CREATE TABLE `IF NOT EXISTS` oldugu icin ONCEDEN olusmus
-- bir tabloya bu politikalari UYGULAMAZ. Mevcut bir kurulumu guncellerken:
--
--   ALTER TABLE pulsecity.vehicle_pings WITH default_time_to_live = 3600
--     AND gc_grace_seconds = 3600
--     AND compaction = {'class':'TimeWindowCompactionStrategy',
--                       'compaction_window_unit':'MINUTES',
--                       'compaction_window_size':'10'};
--
-- (TTL yalnizca ALTER'dan SONRA yazilan satirlara uygulanir; eski satirlar
--  sonsuza kadar kalir, gerekiyorsa elle silinmelidir.)

-- Dead-letter kayitlarini da ScyllaDB'de tutmak istersen (opsiyonel, debug icin faydali)
CREATE TABLE IF NOT EXISTS vehicle_pings_dlq (
  id           uuid,
  raw_payload  text,
  error_reason text,
  failed_at    timestamp,
  PRIMARY KEY (id)
);

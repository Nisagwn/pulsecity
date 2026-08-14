-- PulseCity keyspace ve tablo tanimi
-- Partition key: zone_id  -> bolge bazli sorgulari hizlandirir, hot-partition riskini
--                            tek bir global sayac yerine cok sayida bolgeye dagitarak azaltir
-- Clustering key: ping_time, vehicle_id -> bolge icinde zamana gore siralama + ayni anda
--                            gelen farkli araclarin cakismasini onler

CREATE KEYSPACE IF NOT EXISTS pulsecity
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
  -- Not: Faz 6'da (canliya alma) replication_factor 3'e cikarilacak (NetworkTopologyStrategy)

USE pulsecity;

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
) WITH CLUSTERING ORDER BY (ping_time DESC);

-- Dead-letter kayitlarini da ScyllaDB'de tutmak istersen (opsiyonel, debug icin faydali)
CREATE TABLE IF NOT EXISTS vehicle_pings_dlq (
  id           uuid,
  raw_payload  text,
  error_reason text,
  failed_at    timestamp,
  PRIMARY KEY (id)
);

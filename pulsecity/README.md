# PulseCity

Go, Kafka ve ScyllaDB ile şehir içi trafik yoğunluğunu saniyede 50.000 GPS
ping'i hızında, sıfır kayıpla işleyen ve Grafana'da canlı analiz eden bir
veri boru hattı.

Sanal araçlardan gelen konum verisini simüle eden bir load generator, Kafka
üzerinden akan bu veriyi paralel Go consumer'lar ScyllaDB'ye batch halinde
yazar; Prometheus + Grafana ile bölge bazlı trafik yoğunluğu gerçek zamanlı
izlenir. Uber/Yandex gibi şirketlerin çözdüğü probleme benzer bir mimari.

## Mimari

```
[Go Load Generator] --> [Kafka: vehicle-pings (12 partition)] --> [Go Consumer Group]
                         [Kafka: vehicle-pings-dlq]  <--(hata)--'         |
                                                                          v
                                                                   [ScyllaDB]
                                                                  (zone_id partition key)
                                                                          |
                                                     [Prometheus] <-------'
                                                          |
                                                     [Grafana Dashboard]
```

**Neden bu tasarım kararları:**

| Karar | Gerekçe |
|---|---|
| Kafka partition key = `zone_id` | Aynı bölgenin ping'leri aynı partition'a gider → bölge içi sıralama korunur |
| ScyllaDB partition key = `zone_id` | Bölge bazlı sorguları hızlandırır, tek bir global sayaç yerine 9 bölgeye dağıtarak hot-partition riskini azaltır |
| Manuel Kafka offset commit | Auto-commit ile "işlemeden önce commit" riski var — zero-loss için önce ScyllaDB'ye yaz, sonra commit et |
| UnloggedBatch (zone bazlı), LoggedBatch değil | ScyllaDB'de çoklu-partition LoggedBatch pahalı bir batchlog mekanizması kullanır; aynı partition içi UnloggedBatch + paralel goroutine çok daha performanslı |
| DLQ (dead-letter queue) | Hiçbir mesaj sessizce kaybolmasın — parse/yazma hatası olan her mesaj DLQ'ya yönlendirilir |

## Klasör yapısı

```
pulsecity/
├── docker-compose.yml       # Kafka, ScyllaDB, producer, consumer, Prometheus, Grafana
├── producer/                # Go load generator (sanal araç GPS ping üreteci)
├── consumer/                # Go consumer (Kafka -> ScyllaDB, DLQ yönlendirme)
├── scylla-init/schema.cql   # ScyllaDB şema tanımı
├── monitoring/               # Prometheus config + Grafana provisioning/dashboard
├── scripts/                  # zero-loss testi, benchmark, chaos testing scriptleri
└── deploy/                   # Production compose override, Nginx, VPS deployment rehberi
```

## Hızlı başlangıç (local)

```bash
docker compose up -d --build
```

Servisler ayağa kalktıktan (~30sn) sonra:

- **Grafana**: http://localhost:3000 (admin / pulsecity) — canlı dashboard
- **Prometheus**: http://localhost:9090 — ham metrikler
- ScyllaDB'de veri doğrulama: `docker exec -it pulsecity-scylla cqlsh -e "SELECT * FROM pulsecity.vehicle_pings LIMIT 10;"`

Consumer'ı paralel ölçeklendirmek için:

```bash
docker compose up -d --scale consumer=3
```

## Faz faz yol haritası ve sonuçlar

### Faz 1 — MVP ✅
Uçtan uca happy-path: producer → Kafka → consumer → ScyllaDB. `docker-compose.yml`,
`producer/`, `consumer/`, `scylla-init/` bu fazda kuruldu.

### Faz 2 — Zero-Loss Garantisi ✅
- Producer: `RequireAll` acks + retry-with-backoff (`writeWithRetry`)
- Consumer: manuel offset commit, sadece ScyllaDB'ye yazdıktan/DLQ'ya yönlendirdikten SONRA commit
- Doğrulama: `./scripts/verify-zero-loss.sh` — consumer'ı işlem ortasında kill eder,
  gönderilen mesaj sayısı ile (ScyllaDB satırı + DLQ satırı) toplamının eşit olduğunu doğrular

### Faz 3 — Performans (50k/sn hedefi) ✅
- Producer: 8 paralel worker goroutine, her biri kendi Kafka bağlantısı ve ayrık araç
  grubuyla çalışır (data race'siz)
- Consumer: mesajlar `zone_id`'ye göre gruplanır, her grup paralel `UnloggedBatch`
  ile yazılır; batch boyutu 2000'e çıkarıldı
- Ölçüm: `./scripts/benchmark.sh 300` — Prometheus'tan throughput/p99 metriklerini çekip
  `benchmark-results.md` üretir

### Faz 4 — Monitoring (Prometheus + Grafana) ✅
- Her iki Go servisi de `/metrics` endpoint'i expose eder (producer: 2112, consumer: 2113)
- Grafana dashboard'u otomatik provision edilir (`monitoring/grafana/provisioning/`):
  throughput, hata/DLQ oranı, **bölge bazlı araç yoğunluğu**, **bölge bazlı ortalama hız**,
  ScyllaDB yazma gecikmesi (p50/p95/p99)

### Faz 5 — Dayanıklılık (Chaos Testing) ✅
- `./scripts/chaos-test.sh` — consumer, Kafka broker ve ScyllaDB için sırayla crash
  simülasyonu yapar, recovery süresini ve veri kaybı olup olmadığını ölçer
- Sonuçlar: bkz. `chaos-test-results.md` *(script çalıştırıldıktan sonra üretilir)*

### Faz 6 — Canlıya Alma (Deployment) ✅
- `deploy/docker-compose.prod.yml`: kaynak limitleri, restart policy, 3 consumer replikası
- `deploy/nginx.conf`: Grafana'yı tek bir public port (80) üzerinden dışa açar
- `deploy/DEPLOY.md`: adım adım VPS kurulum rehberi (Hetzner/DigitalOcean, 2vCPU/4GB yeterli)

### Faz 7 — Belgeleme ✅
Bu README — mimari, tasarım kararları, kurulum, sonuçlar tek yerde.

## Sonuçları doğrulama sırası

Projeyi baştan sona kendi ortamında doğrulamak istersen:

```bash
# 1. Temel doğrulama
docker compose up -d --build
docker compose logs -f producer consumer

# 2. Zero-loss kanıtı
./scripts/verify-zero-loss.sh 60

# 3. Performans ölçümü
./scripts/benchmark.sh 300

# 4. Dayanıklılık testi
./scripts/chaos-test.sh

# 5. Production deployment (opsiyonel, gerçek VPS gerektirir)
# bkz. deploy/DEPLOY.md
```

## Bilinen sınırlamalar (v1, kasıtlı kapsam dışı)

- Multi-region / multi-datacenter dağıtım yok
- Kubernetes yok (bu ölçekte overkill; Docker Compose + tek VPS yeterli)
- Karmaşık stream-processing (windowing, join, CEP) yok — ayrı bir proje fikri olarak değerlendirildi
- TLS/HTTPS henüz yok (Faz 6'da not edildi, gerçek domain ile kolayca eklenebilir)
- Kafka/ScyllaDB tek node (RF=1) — production-grade bir kurulumda RF=3 + multi-broker gerekir

## Teknoloji yığını

Go · Apache Kafka (KRaft mode) · ScyllaDB · Prometheus · Grafana · Docker Compose · Nginx

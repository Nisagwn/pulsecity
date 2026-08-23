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
| Bölge = tek bir gerçek ilçe (1:1) | Başlangıçta 3 ilçe × 3 alt bölge vardı (`kadikoy-1/2/3` gibi); haritada aynı ilçe adını taşıyan üç çember yayılınca en dıştaki komşu ilçenin üzerindeymiş gibi görünüyor ve yanlış ilçe adı okunuyordu. İstanbul'un en kalabalık 9 ilçesine bire bir eşleme bu belirsizliği kaldırdı — `TestZoneCirclesDoNotOverlap` çakışmayı kilitler |
| Manuel Kafka offset commit | Auto-commit ile "işlemeden önce commit" riski var — zero-loss için önce ScyllaDB'ye yaz, sonra commit et |
| UnloggedBatch (zone bazlı), LoggedBatch değil | ScyllaDB'de çoklu-partition LoggedBatch pahalı bir batchlog mekanizması kullanır; aynı partition içi UnloggedBatch + paralel goroutine çok daha performanslı |
| DLQ (dead-letter queue) | Hiçbir mesaj sessizce kaybolmasın — parse/yazma hatası olan her mesaj DLQ'ya yönlendirilir |

## Klasör yapısı

```
.
├── docker-compose.yml       # Kafka, ScyllaDB, producer, consumer, webviz, Prometheus, Grafana
├── producer/                # Go load generator (sanal araç GPS ping üreteci)
├── consumer/                # Go consumer (Kafka -> ScyllaDB, DLQ, anomali tespiti)
├── webviz/                  # Go WebSocket servisi + Leaflet canlı harita (Faz 9)
│                            # + ScyllaDB'den geçmişe bakma (Faz 11)
├── scylla-init/schema.cql   # ScyllaDB şema tanımı
├── monitoring/              # Prometheus config + alarm kuralları, Alertmanager,
│                            # Grafana provisioning/dashboard
├── scripts/                 # zero-loss testi, benchmark, chaos testing scriptleri
├── deploy/                  # Production/CI compose override, Nginx, VPS deployment rehberi
├── infra/terraform/         # Altyapı kod olarak: VPC, EC2, IAM, SSM (Faz 14)
└── .github/workflows/       # CI/CD (lint, test, uçtan uca test, imaj yayınlama)
```

## Hızlı başlangıç (local)

```bash
docker compose up -d --build
```

Servisler ayağa kalktıktan (~30sn) sonra:

- **Canlı harita**: http://localhost:8080 — bölgeler yoğunluğa göre renklenir,
  araçlar nokta olarak akar, anomali tespit edilen bölge kırmızı yanıp söner
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
- Doğrulama: `./scripts/verify-zero-loss.sh` — consumer'ı işlem ortasında SIGKILL ile
  öldürür, sonra Kafka'daki **tekil birincil anahtar** sayısı ile (ScyllaDB satırı + DLQ
  mesajı) toplamının eşit olduğunu doğrular

  Neden ham mesaj sayısıyla değil: tablonun birincil anahtarı
  `(zone_id, ping_time, vehicle_id)` ve ScyllaDB'nin `timestamp` tipi milisaniye
  hassasiyetinde. Aynı aracın aynı milisaniyeye düşen iki ping'i ikinci bir satır değil,
  üzerine yazma üretir — ölçülen oran ~%7. Ham sayıyla karşılaştıran bir test, boru hattı
  hiçbir şey kaybetmese bile başarısız görünür.

  Ölçülen sonuç (30sn, 5000 msg/sn): 165.000 mesaj → 6.430'u aynı anahtara düştü →
  beklenen 158.570 tekil satır; ScyllaDB'de tam olarak 158.570 satır, DLQ boş, kayıp yok.

### Faz 3 — Performans (50k/sn hedefi) ✅
- Producer: 8 paralel worker goroutine, her biri kendi Kafka bağlantısı ve ayrık araç
  grubuyla çalışır (data race'siz)
- Consumer: mesajlar `zone_id`'ye göre gruplanır, her grup paralel `UnloggedBatch`
  ile yazılır; batch boyutu 2000'e çıkarıldı
- Ölçüm: `./scripts/benchmark.sh 300` — hedef hızda ayağa kaldırır, Prometheus'tan
  throughput/gecikme/lag metriklerini çekip `benchmark-results.md` üretir

**Ölçülen sonuç — hedef tutturuldu.** 180 saniyelik ölçüm (+60sn ısınma):

| Metrik | Değer |
|---|---|
| Üretilen throughput | **51.193 msg/sn** |
| İşlenen throughput | **50.477 msg/sn** |
| Hedefe ulaşma | **%101** |
| ScyllaDB batch yazma p50 / p95 / p99 | 18,4 / 38,7 / **48,4 ms** |
| Producer hata oranı | **0** |
| DLQ oranı | **0** |

Test ortamı: Docker'a ayrılmış 12 çekirdek / 7,6 GB · Kafka tek broker (KRaft, 12 partition) ·
ScyllaDB tek node RF=1 (4 shard / 2,5 GB) · 1 consumer replikası. Ölçüm sırasında anomali
demo modu kapalıdır (yapay tıkanıklık bölgesel hız metriklerini bozar).

**Consumer geride kalmıyor.** Lag'i 10 saniye aralıklarla örnekledim:
16.500 → 48.000 → 32.000 → 29.500 → **0** → 48.000 mesaj. Bu testere dişi deseni batch'leme
kaynaklı: consumer 2000'lik grupları biriktirip boşaltıyor. Kritik olan **lag'in düzenli
olarak 0'a inmesi** — sistem yetişemiyor olsaydı lag hiç sıfırlanmaz, monoton büyürdü.
Tepe değeri 48.000 mesaj, 50k/sn hızda yaklaşık 1 saniyelik uçuş halindeki veriye denk geliyor.

Günlük geliştirmede `docker-compose.yml` bilerek hafif bir profille (5.000 msg/sn) gelir ki
bir laptop'ı boğmasın. Hedef hız `deploy/docker-compose.bench.yml` override'ıyla açılır:

```bash
docker compose -f docker-compose.yml -f deploy/docker-compose.bench.yml up -d --build
./scripts/benchmark.sh 180
```

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

### Faz 8 — Anomali Tespiti ✅
Sistemi "veri taşıyan" seviyesinden "veriden anlam çıkaran" seviyeye taşıyan katman.

- Consumer, her bölge için **EMA ile öğrenilen bir "normal" hız baseline'ı** tutar
- Bir bölgenin ortalama hızı baseline'ın **%40'ından fazla** altına düşerse anomali
  (olası tıkanıklık/kaza) olarak işaretlenir; `pulsecity_zone_anomaly_detected` 1 olur
- **Isınma penceresi**: ilk 20 batch'te sadece baseline beslenir, anomali aranmaz —
  yoksa baseline 0'dan başlar ve ilk batch'ler her zaman anomali görünür
- **Kritik tasarım kararı**: baseline yalnızca anomali OLMAYAN batch'lerde güncellenir.
  Aksi halde uzun süren bir sıkışıklık yavaş yavaş "yeni normal" olur, EMA ona yakınsar
  ve tespit sistemi kendi kendini kör eder. `TestBaselineFrozenDuringAnomaly` bunu kilitler.
- `zone_id` hem Kafka hem ScyllaDB partition key'i olduğu için bir bölgenin tüm mesajları
  tek bir consumer replikasına düşer — `--scale consumer=3` yapıldığında baseline bölünmez
- **Demo modu**: producer'da `TEST_ANOMALY_DEMO=true` periyodik olarak rastgele bir bölgeyi
  yapay olarak tıkar. Bölgesel hız verisini bilerek bozduğu için kodda varsayılan **kapalı**;
  `docker-compose.yml` local'de açar, prod ve CI override'ları kapatır (yoksa `benchmark.sh`
  ölçümü kirlenir)

Ölçülen sonuç: producer `istanbul-sisli-1`'i tıkadıktan **1 saniye sonra** consumer yakaladı
(8.0 km/h vs öğrenilen normal 37.6 km/h), tıkanıklık boyunca baseline 37.6'da sabit kaldı.

### Faz 9 — Canlı Harita ✅
Grafana'nın sayısal grafiklerinin yanına, trafiği gerçekten *görebildiğin* bir katman.

- `webviz/`: Kafka'yı okuyup WebSocket üzerinden tarayıcıya yayın yapan Go servisi +
  Leaflet tabanlı harita arayüzü (binary'ye `go:embed` ile gömülü)
- **Ayrı servis, ayrı consumer group** (`pulsecity-webviz`): ana consumer zero-loss kritik
  yolunda ve manuel offset commit ediyor; sunum katmanını oraya eklemek dayanıklılık
  garantisine bağlardı. Ayrıca `--scale consumer=3` durumunda her replika akışın sadece bir
  kısmını görür, harita eksik olurdu.
- **Throttling kritik tasarım kararı**: saniyede 5000+ mesaj tarayıcıya iletilemez. Bellekte
  "her aracın son konumu" tutulur ve saniyede bir tek snapshot yayınlanır — ağ yükü mesaj
  hızından bağımsız hale gelir (ölçülen: 37 KB/snapshot, 1500 araç)
- Haritada gösterilen araçlar ilk görüldüklerinde seçilir ve sabit kalır; her karede rastgele
  örneklemek noktaların titremesine yol açardı
- Bölge merkezleri ve çember yarıçapları producer'daki tablodan kopyalanmaz, gelen
  ping'lerin dağılımından türetilir — şehir/bölge tanımı değiştiğinde burada elle
  güncelleme gerekmez (ölçülen: producer'da 2000 m, haritada türetilen ~2020-2120 m)
- Bölgeler İstanbul'un en kalabalık 9 ilçesi: Esenyurt, Küçükçekmece, Bağcılar,
  Bahçelievler, Sultangazi (Avrupa) · Üsküdar, Ümraniye, Maltepe, Pendik (Anadolu).
  Kimlikler ASCII (Kafka anahtarı + ScyllaDB partition key), Türkçe gösterim adları
  arayüzde eşlenir
- Anomali sinyali webviz'de yeniden hesaplanmaz, Faz 8'in Prometheus metriklerinden okunur;
  mantık tek yerde kalsın diye

**Ön koşul olarak düzeltilen hareket matematiği**: `step()` konum değişimini
`speed * 0.00001` sabitiyle hesaplıyordu; bu araçların bildirdikleri hızın ~10 katıyla
hareket etmesine yol açıyordu. Ayrıca boylam enleme göre ölçeklenmiyordu (41. paralelde
1° boylam ~84 km, 1° enlem ~111 km) ve hiç sınır kontrolü yoktu — araçlar şehrin dışına
çıkıyor ama `zone_id` etiketleri değişmediği için veri yalan söylüyordu. Üçü de düzeltildi;
`TestStepMovesAtDeclaredSpeed` ve `TestVehicleStaysWithinItsZone` regresyonu kilitler.

### Faz 10 — CI/CD ✅
`.github/workflows/ci.yml` — her push ve PR'da çalışır:

| Job | Ne yapar |
|---|---|
| `lint-build` | 3 modül için `gofmt` + `go vet` + derleme |
| `test` | `go test -race -cover` (yarış dedektörü: üç servis de eşzamanlı kod içeriyor) |
| `integration` | Düşük yükle tüm stack'i ayağa kaldırır; producer üretiyor mu, consumer işliyor mu, DLQ boş mu, ScyllaDB'ye yazılıyor mu, 9 bölge var mı, Faz 8 baseline üretiliyor mu, harita 200 dönüyor mu — hepsini doğrular, sonra temizler |
| `docker-push` | Sadece `main`'e push'ta: 3 imajı GHCR'a yayınlar (ek secret gerekmez, `GITHUB_TOKEN` yeterli) |
| `deploy` | **Pasif.** Elle tetikleme + `DEPLOY_ENABLED=true` repository variable olmadan çalışmaz |

Ayrıca build altyapısı düzeltildi: `go.sum` dosyaları eklendi ve Dockerfile'lar
`go mod tidy` yerine `go mod download` kullanacak şekilde yeniden yazıldı — build artık
tekrarlanabilir ve bağımlılık katmanı önbelleğe alınabiliyor.

### Faz 11 — Zaman Yolculuğu ✅
Faz 10'a kadar ScyllaDB'ye **yazılan veri hiçbir yerde okunmuyordu**: harita Kafka'dan
canlı akışı izliyor, consumer ise kayıtları yalnızca yazıyordu. Depolama katmanı
"yaz ve unut" durumundaydı. Bu faz o boşluğu kapatır.

Haritanın altındaki zaman kaydırıcısı geriye çekildiğinde kareler Kafka'dan değil
ScyllaDB'den okunur — son 30 dakikanın herhangi bir anına gidip trafiği izleyebilirsin.

- `webviz/history.go`: okuma yolu + `GET /api/history?at=<unix>` uç noktası
- **Sorgu deseni şemayla birebir örtüşüyor**: `zone_id` partition key olduğu için her bölge
  sorgusu tek partition'a iner, `ping_time` clustering key ve `DESC` sıralı olduğu için
  zaman aralığı diskte zaten bitişik duran satırlardan okunur. Yani "Üsküdar'ın 14:32'si"
  bir tarama değil, sıralanmış aralık okumasıdır — Faz 1'deki partition key kararının
  karşılığı burada alınır
- **Bölge başına satır tavanı** (`HISTORY_ROWS_PER_ZONE`, varsayılan 2000): 50k/sn'de
  2 saniyelik pencere bölge başına ~11k satır eder. Tavan olmadan tek bir kare için yüz
  binlerce satır okunurdu; ortalama hız ve merkez bu örneklem üzerinden de sağlıklı çıkar.
  Arayüz bunu gizlemez, geçmiş modda etiket `/sn` yerine `örnek` yazar
- **Bağlantı arka planda kurulur, `depends_on` eklenmedi**: geçmiş okuma kritik yol değil.
  Scylla geç açılsa ya da düşse canlı harita çalışmaya devam eder, yalnızca kaydırıcı
  gizlenir (`/api/history/meta` → `enabled: false`). Aynı yaklaşım Prometheus için de geçerli
- **Anomali sinyali geçmiş kareye taşınmaz**: Prometheus yalnızca anlık değeri tutuyor,
  onu geçmiş bir kareye yapıştırmak "14:32'de anomali vardı" gibi yanlış bir şey söylerdi
- **Oynatma sabit gecikmeyle çalışır**: kaydırıcının değeri sabit bir *offset*'tir, istenen
  an `now + offset` olarak hesaplanır. Offset sabit kaldığı için zaman kendiliğinden 1x
  hızda ilerler — ayrıca bir sayaç ya da hız çarpanı gerekmez
- `history_integration_test.go` (`-tags=integration`): birim testler `historyQuerier`
  arayüzünü sahteleyip snapshot mantığını doğruluyor ama **sorgu metnini** doğrulayamıyor.
  CQL'deki bir yazım hatası ya da desteklenmeyen kalıp (LIMIT'te bind parametresi,
  DISTINCT partition key okuması) ancak gerçek veritabanında ortaya çıkar. Bu test gerçek
  ScyllaDB'ye karşı koşar; `SCYLLA_TEST_HOSTS` tanımsızsa atlanır, normal `go test` ve CI
  akışı ayakta bir veritabanı gerektirmez

**Harita okunabilirliği düzeltmesi (aynı fazda):** efsane yalnızca bölge renklerini
listeliyordu, oysa araç noktaları ayrı bir skala kullanıyor ve o skaladaki mavi
(30-49 km/h) efsanede hiç geçmiyordu. Üstelik aynı yeşil bölgede "40+", araçta "50+"
anlamına geliyordu. Üç değişiklik yapıldı:

- Efsane iki başlığa ayrıldı (*Bölge · ortalama hız* / *Araç · anlık hız*) ve her satıra
  eşik sayıları yazıldı — hangi rengin neyi anlattığı artık ima değil, yazılı
- **Anomali renk kanalından çıkarıldı.** Önce `zoneColor()` anomaliyi düz kırmızıya
  boyuyordu; bu, akşam trafiğinde doğal olarak yavaşlayan bir bölge ile gerçekten
  anormal bir bölgeyi ayırt edilemez hale getiriyordu. Renk artık yalnızca tıkanıklık
  şiddetini gösteriyor, anomali ise onun *üstüne* binen kesikli kalın halka + `⚠`
  işareti olarak çiziliyor. İki farklı soru, iki farklı görsel kanal
- **Anomali animasyonu ilk kez gerçekten çalışıyor:** harita `preferCanvas: true` ile
  açıldığı ve bölge çemberlerine ayrı renderer verilmediği için çemberler canvas'a
  çiziliyordu; `circle._path` (SVG elemanı) hiç oluşmadığından `.anomaly-ring` sınıfı
  hiçbir zaman uygulanmıyordu. Bölge çemberleri artık SVG renderer kullanıyor (9 çember,
  maliyeti önemsiz; 1500 araç noktası canvas'ta kalmaya devam ediyor). Kesikli çizgi
  ayrıca ekran görüntüsünde de durur — sinyalin yalnızca animasyona bağlı kalmaması için

### Faz 12 — Canlıya Alma Sertleştirmesi ✅
Faz 11'e kadar sistem *çalışıyordu* ama production'da ilk gerçek arızada kırılacak
beş boşluk vardı. Hepsi kod okunarak bulundu, hiçbiri README'nin "bilinen
sınırlamalar" listesinde yazmıyordu.

| Boşluk | Neden ciddiydi | Çözüm |
|---|---|---|
| **Kalıcı disk yoktu** | Kafka ve ScyllaDB verisi container'ın yazılabilir katmanındaydı; bir `docker compose down`, recreate ya da imaj güncellemesi topic'leri, commit edilmiş offset'leri ve tüm ping geçmişini siliyordu | `kafka-data` ve `scylla-data` named volume'leri |
| **Çifte hata anında sessiz veri kaybı** | `writeBatchToScylla` void'di. ScyllaDB **ve** DLQ aynı anda yazılamadığında yalnızca bir log satırı basılıyor, offset yine de commit ediliyordu — zero-loss iddiasını tam olarak bu senaryoda bozan bir açık | Fonksiyon artık hata döndürüyor; hata varsa **commit edilmiyor**, Kafka mesajları yeniden teslim ediyor. DLQ yazımı ayrıca 3 kez retry ediliyor |
| **Prometheus 3 replikanın 1'ini ölçüyordu** | Prod 3 consumer replikası çalıştırıyor ama scrape hedefi `consumer:2113` statikti; Docker DNS bu adı round-robin çözdüğü için her scrape rastgele bir replikaya gidiyordu. Throughput, DLQ ve anomali metrikleri eksik ve zıplayan değerlerdi | `dns_sd_configs` ile A kaydındaki tüm replikalar. Grafana panelleri ve webviz'in Prometheus okuması replika-güvenli hale getirildi (`sum`/`max by (zone_id)`) |
| **Health/readiness sondası yoktu** | `connectWithRetry` yalnızca **açılışta** çalışıyor. ScyllaDB bağlantısı çalışma sırasında koparsa consumer çökmeden her batch'i DLQ'ya yazmaya devam eder ve dışarıdan bakan hiçbir şey bunu fark etmezdi | Üç serviste `/healthz` (liveness) + `/readyz` (bağımlılık kontrolü) ve compose healthcheck'leri |
| **Graceful shutdown yoktu** | Her deploy Docker'ın SIGTERM→SIGKILL akışına tabi; consumer batch ortasında kesiliyordu | `signal.NotifyContext` ile SIGTERM yakalama, son batch'i temiz bir context ile flush, WebSocket istemcilerine `CloseGoingAway` |

**Readiness sondalarının sınırları bilinçli:** webviz'in `/readyz`'ı ScyllaDB'ye
**bakmaz** — geçmişe bakma kritik yol değil, Scylla düşse bile canlı harita sağlıklı
sayılmalı (Faz 11'deki `depends_on` kararıyla aynı mantık). Producer'ın `/readyz`'ı
"son başarılı Kafka yazmasının üzerinden 30sn geçti mi"ye bakar; eşik
`writeWithRetry`'ın toplam backoff'undan belirgin biçimde büyük seçildi ki geçici bir
broker hiccup'ı restart tetiklemesin.

**Ölçülen sonuçlar** (CI profili, ayakta duran stack üzerinde):

| Doğrulama | Sonuç |
|---|---|
| ScyllaDB durdurulduğunda consumer `/readyz` | **503**, Docker healthcheck 3 denemede `unhealthy`'ye döndü |
| Aynı anda consumer `/healthz` | **200** — liveness readiness'tan ayrı, tasarlandığı gibi |
| Aynı anda webviz `/readyz` | **200** — Scylla'ya bilerek bakmıyor, harita sağlıklı sayıldı |
| ScyllaDB geri açıldığında | gocql session'ı kendiliğinden bağlandı, `/readyz` → 200, restart gerekmedi |
| SIGTERM sonrası çıkış kodları | üçü de **0** (temiz kapanış); değişiklikten önce 137 = SIGKILL olurdu |
| `down` + `up` sonrası ScyllaDB | 153.430 satır **korundu** (volume öncesi sıfırlanırdı) |
| `down` + `up` sonrası Kafka | `pulsecity-consumers` grubunun commit edilmiş offset'leri **korundu** |
| Prometheus hedefleri | consumer artık DNS ile IP bazlı keşfediliyor (`172.19.0.6:2113`), 3 replikada 3 hedef |

**Hâlâ eksik olan:** alerting kuralı (Grafana `provisioning/alerting/` yok — DLQ
patlasa kimse haberdar olmaz), ScyllaDB TTL ve Kafka `retention.bytes` politikası
(veri artık kalıcı olduğu için sınırsız büyür), yapılandırılmış log (`log/slog`).
Ayrıca çifte-hata anında commit etmeme davranışının **birim testi yok**:
`writeBatchToScylla` somut `*gocql.Session` ve `*kafka.Writer` aldığı için
sahtelenemiyor; Faz 11'in `historyQuerier` deseni buraya da uygulanabilir.

### Faz 13 — DevOps Sertleştirmesi ✅
Faz 12 sistemi *dayanıklı* hale getirdi. Bu faz onu *işletilebilir* yapar:
tedarik zinciri, log hijyeni, veri yaşam döngüsü ve alarm zinciri.

**Tedarik zinciri (supply chain)**

- **Konteynerler artık root çalışmıyor.** Üç imaj da sabit UID 10001 ile
  ayrıcalıksız bir kullanıcıya geçti. UID'nin *sayısal* olması bilinçli:
  Kubernetes'in `runAsNonRoot` admission kontrolü isim çözümlemesine güvenmez.
- **Base imajlar digest ile sabitlendi.** `alpine:3.22` gibi bir etiket
  değişebilir bir işaretçidir; upstream aynı etikete yeni imaj push'ladığında
  build girdisi sessizce değişir. Digest içeriğin kendisini adresler.
  Bedeli, güvenlik yamalarının otomatik gelmemesi — bu yüzden
  `.github/dependabot.yml` digest'leri haftalık PR olarak açar: yama akışı
  kaybolmuyor, **görünür ve gözden geçirilebilir** hale geliyor.
- **CI'da Trivy + SBOM.** Yayınlanacak imajın tam olarak kendisi taranıyor ve
  tarama yayın yolunda bir **kapı** (`docker-push`, `security` job'ına bağlı) —
  sadece rapor üretmiyor. SPDX formatında SBOM artifact olarak saklanıyor.
  `ignore-unfixed: true` bilinçli: yaması olmayan bir CVE için build'i kırmak,
  yapılabilecek bir şey yokken boru hattını durdurur ve zamanla "kırmızıyı
  görmezden gelme" alışkanlığı yaratır.

**Taramanın ilk çalıştırmada bulduğu şey — ve neden önemli:** tarama eklendiği
anda **22 bulgu** çıkardı (21 HIGH + 1 CRITICAL). Kök neden uygulama kodu
değil, **EOL olmuş Go 1.22 toolchain'iydi**: `CVE-2025-68121`, `crypto/tls`
içinde hatalı sertifika doğrulaması. Ayrıca webviz'de transitif
`golang.org/x/net v0.20.0` 7 HIGH taşıyordu. Doğru cevap bunları bastırmak
değil, kaynağı düzeltmekti — Go 1.22→1.26, Alpine 3.19→3.22,
`x/net` 0.20→0.58. **Sonuç: 22 → 0.** Bu, taramanın dekoratif olmadığının
kanıtı; gerçek bir açığı yakalayıp kapattı.

**Yapılandırılmış log**

Üç servis de `log/slog` ile JSON yazıyor (`LOG_FORMAT=text` yerel okuma için,
`LOG_LEVEL` ile seviye). Önceki sürümde her satır elle biçimlenmiş düz metindi:
bölgeye göre filtrelemek ya da hız eşiğine göre alarm kurmak için o metni
regex'le parse etmek gerekirdi. Artık `{"zone_id":"istanbul-sisli-1"}` doğrudan
sorgulanabilir. Seviye ayrımı da yeni — önceden ANOMALİ ile "batch işlendi"
aynı seviyedeydi, "sadece hataları göster" diye bir filtre kurulamıyordu.
Yüksek hacimli rutin satırlar (batch/saniyelik tur) `Debug`'a indirildi;
ilerlemenin asıl ölçüsü zaten Prometheus sayaçları.

`slog.SetDefault` standart `log` paketini de bu handler'a yönlendirdiği için
gocql ve kafka-go'nun kendi logları da otomatik olarak yapılandırılmış hale
geliyor.

**Veri yaşam döngüsü**

Faz 12'de kalıcı disk eklenince veri artık restart'ları aşıyor — yani sınırsız
büyüyor. Bu faz politikayı açık yazar:

| Ne | Politika | Gerekçe |
|---|---|---|
| Kafka `vehicle-pings` | `retention.ms` = 1 saat, `retention.bytes` = 512 MiB/partition (≈6 GiB toplam) | Broker varsayılanı 7 gün + sınırsız boyut. 50k msg/sn ≈ saatte 36 GB |
| Kafka `segment.bytes` | 128 MiB | Kafka yalnızca **kapanmış** segmentleri siler; 1 GiB'lik varsayılan segmentle 512 MiB'lik tavan asla devreye giremez, politika sessizce etkisiz kalırdı |
| Kafka DLQ | 7 gün | Oradaki mesajlar incelenmek için var; ana topic ile aynı politika, hata ayıklanacak kanıtı bir saat sonra silmek olurdu |
| ScyllaDB `vehicle_pings` | `default_time_to_live=3600` | Haritanın geriye bakabildiği pencerenin (30 dk) iki katı |
| ScyllaDB compaction | `TimeWindowCompactionStrategy` (10 dk pencere) | Zaman serisi + TTL için doğru strateji. Varsayılan SizeTiered farklı zamanlara ait SSTable'ları birleştirir; süresi dolan satırlar taze verinin içine karışır ve alan ancak pahalı bir compaction sonrası geri alınır. TWCS'te tamamen dolmuş pencere **dosya silinerek** geri kazanılır |
| ScyllaDB `gc_grace_seconds` | 3600 (varsayılan 10 gün) | Varsayılan, node'lar arası repair penceresi bırakır. RF=1 tek node'da replika yok, dolayısıyla o risk de yok — 10 günlük varsayılan sadece tombstone biriktirirdi. RF=3'e çıkılırsa yükseltilmeli |

**Alarm zinciri**

Faz 12'ye kadar sistem saf gözlemlenebilirdi: metrikler toplanıyor, çizdiriliyor
ama birinin ekrana bakması gerekiyordu. Artık sinyal operatörü arıyor —
Prometheus kuralları + Alertmanager (gruplama, susturma, inhibition).

8 kural, `critical`/`warning` ayrımıyla. En yüksek öncelikli olan
`ConsumerBatchCommitEdilemiyor`: Faz 12'de eklenen
`pulsecity_consumer_uncommitted_batches_total` sayacını izler, yani **zero-loss
yolunun tıkandığı anı** yakalar.

- **`for` süreleri kritik.** Batch'li bir sistemde anlık değerler testere dişi
  oynar (bkz. Faz 3 lag grafiği); `for` olmadan sağlıklı bir sistem sürekli
  alarm verirdi. Lag eşiği (200k) ölçülen tepe değerin (48k) çok üzerinde
  seçildi — kritik olan mutlak değer değil, **lag'in düzenli olarak
  sıfırlanmaması**.
- **Inhibition kuralları** türev alarmları bastırır: ScyllaDB erişilemezken
  DLQ'nun dolması beklenen davranıştır, ayrı bir olay değil.
- **Consumer lag artık bir zaman serisi.** Önceden yalnızca
  `kafka-consumer-groups` CLI'sıyla, `benchmark.sh` içinden okunuyordu — yani
  sadece elle ölçüm sırasında görülebiliyor, üzerine alarm kurulamıyordu.
  `kafka-exporter` bunu sürekli ölçülen bir metriğe çevirdi.
- **Bildirim kanalı bilerek boş**: gerçek bir Slack webhook'u bu depoya sabit
  yazılamaz. Alarmların üretildiği ve doğru gruplandığı Alertmanager arayüzünden
  (`:9093`) doğrulanabilir; `alertmanager.yml` içinde Slack örneği yorumda.

**Ölçülen sonuçlar** (ayakta duran stack üzerinde):

| Doğrulama | Sonuç |
|---|---|
| Konteyner kullanıcısı | üçü de `uid=10001(pulsecity)` — root değil |
| Trivy imaj taraması | **22 bulgu → 0** (Go 1.26 + Alpine 3.22 + x/net 0.58 sonrası) |
| Log formatı | üçü de JSON, `service`/`level`/alan bazlı |
| Kafka `vehicle-pings` | `retention.ms=3600000`, `retention.bytes=536870912`, `segment.bytes=134217728` uygulandı (varsayılan `log.retention.bytes=-1` override edildi) |
| Kafka DLQ | `retention.ms=604800000` (7 gün) |
| ScyllaDB tablosu | `default_time_to_live=3600`, `gc_grace_seconds=3600`, TWCS 10 dk pencere |
| Prometheus | 8 kural yüklendi, Alertmanager bağlı |
| `kafka_consumergroup_lag` | 24 seri, toplam lag okunabiliyor |
| **Uçtan uca alarm testi** | ScyllaDB durduruldu → `ConsumerScyllaBaglantisiYok` (critical) **aktif**, `DLQDoluyor` (warning) **suppressed** — inhibition kuralı türev semptomu bastırdı. ScyllaDB geri açıldığında sekiz kural da `inactive`'e döndü |

**Yol boyunca yakalanan bir tuzak:** Kafka retention'ı ilk sürümde `--config`
bayraklarıyla, ters bölü satır devamı kullanılarak yazılmıştı. Compose bu bloğu
işlerken ters bölüleri yutuyor ve `--config ...` ayrı bir komut olarak
çalıştırılmaya kalkıyor (`--config: command not found`). **Topic yine oluşuyor,
komut sıfır dönüyor, ama retention politikası sessizce uygulanmıyordu.**
Doğrulama adımı olmasa fark edilmezdi. Çözüm: komutlar tek satıra alındı ve
oluşturma (`--create`) ile yapılandırma (`--alter`) ayrıldı — `--alter` her
çalışmada politikayı yeniden dayattığı için bu dosya mevcut bir kuruluma da
güvenle uygulanabiliyor.

**Hâlâ eksik olan:** IaC (Terraform — `deploy/DEPLOY.md` hâlâ elle SSH adımları),
secret yönetimi (`.env` seviyesinde, SOPS/Vault yok), TLS, log toplama
(Loki/Promtail), ve çifte-hata anında commit etmeme davranışının birim testi.

### Faz 14 — Altyapı Kod Olarak (Terraform) ✅
Faz 13'e kadar `deploy/DEPLOY.md` bir **runbook**'tu: SSH ile bağlan, `apt
install` yap, `.env`'i `nano` ile doldur. Elle uygulanan adımlar her seferinde
biraz farklı uygulanır ve "sunucuda ne olduğunu" kimse tam bilemez. Bu faz
sunucuyu *onarılan* bir şey olmaktan çıkarıp **yeniden üretilen** bir şey
yapar.

`infra/terraform/` — VPC + public subnet + IGW, Security Group, EC2
(gp3 + EBS şifreleme), Elastic IP, SSM Parameter, IAM rolü, VPC Flow Logs.
Sunucu hazırlığı `cloud-init.yaml` ile.

**Secret'lar `user_data`'ya gömülmüyor.** En kolay yol Grafana parolasını
cloud-init içine yazmaktı, ama EC2 user-data gizli bir kanal değildir:
instance üzerindeki herhangi bir süreç IMDS'ten okuyabilir
(`curl http://169.254.169.254/latest/user-data`), AWS konsolunda düz metin
görünür ve Terraform state'ine de düz metin yazılır. Parola bunun yerine SSM'de
**SecureString** olarak durur; instance onu *kendi* IAM kimliğiyle, yalnızca o
tek parametre yoluna erişim veren bir politikayla çeker. Parola makineye
gönderilmiyor — makine onu yetkisi olduğu için okuyor.

**IMDSv2 zorunlu, `hop_limit = 1`.** IMDSv1'de metadata servisi kimlik
doğrulamasız bir GET ile okunur; uygulamada bir SSRF açığı varsa saldırgan
instance rolünün geçici kimlik bilgilerini sızdırabilir — gerçek ihlallerde
defalarca kullanılmış bir zincir. `hop_limit = 1` ayrıca konteyner ağ
katmanından IMDS'e erişimi keser.

**`allowed_ssh_cidr`'ın varsayılanı yok.** En güvenli değeri bile varsayılan
yapmak, bu alanı "düşünülmesi gerekmeyen" bir alan haline getirir. Zorunlu
bırakmak her `apply`'da bilinçli bir karar dayatıyor; `0.0.0.0/0` verilmesi
`validation` bloğuyla reddediliyor.

**Taramanın bulduğu ve nasıl davranıldığı.** Terraform'a da imajlara
uyguladığım kapının aynısını uyguladım (`trivy config`). İki bulgu çıktı ve
ikisine farklı davranıldı:

| Bulgu | Karar |
|---|---|
| `AWS-0178` (MEDIUM) — VPC Flow Logs yok | **Düzeltildi.** Haklı bir bulguydu: proje baştan sona gözlemlenebilirlik üzerine kurulu ama ağ katmanı görünmezdi. 7 gün saklama ile eklendi |
| `AWS-0104` (CRITICAL) — sınırsız egress | **Gerekçesiyle kabul edildi.** Docker Hub/GHCR/apt/Let's Encrypt CDN arkasında ve değişken IP aralıklarında; daraltmak ya kırılgan bir liste ya da ~35 USD/ay NAT + egress proxy gerektirir. `security.tf` içinde `trivy:ignore` ile **yazılı olarak** işaretlendi |

İkinci satır bilinçli: bir bulguyu *sessizce bastırmak* ile *yazılı olarak
kabul etmek* arasındaki fark, "farkında değil" ile "farkında ve karar verdi"
arasındaki farktır.

**CI'da `apply` yok, bilerek.** `iac` job'ı her push'ta `fmt -check`,
`validate` ve Trivy taraması çalıştırır — ama `plan`/`apply` çalıştırmaz, yani
iş akışının bulut kimlik bilgisi tutması gerekmez. Uygulamayı operatör kendi
kimliğiyle yapar.

**Ölçülen sonuçlar:** `terraform fmt -check` temiz · `terraform validate`
başarılı · `trivy config` **0 bulgu** (bir istisna gerekçeli).

Ayrıntı ve maliyet notları: [`infra/terraform/README.md`](infra/terraform/README.md).

## Sonuçları doğrulama sırası

Projeyi baştan sona kendi ortamında doğrulamak istersen:

```bash
# 0. Birim testler (Go kurulu değilse Docker içinde de çalışır)
cd producer && go test ./... -race && cd ..
cd consumer && go test ./... -race && cd ..
cd webviz   && go test ./... -race && cd ..

# 1. Temel doğrulama
docker compose up -d --build
docker compose logs -f producer consumer
# Canlı harita: http://localhost:8080

# 2. Zero-loss kanıtı
./scripts/verify-zero-loss.sh 60

# 3. Performans ölçümü (hedef hıza çıkarır, ~4 dk sürer)
./scripts/benchmark.sh 180

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
- `ping_time` milisaniye hassasiyetinde saklanır; aynı aracın aynı milisaniyedeki iki
  ping'i tek satıra iner (~%7). Boru hattı bunları kaybetmez (Kafka'da ve DLQ hesabında
  tam olarak dururlar), ancak ScyllaDB satır sayısı ham mesaj sayısından düşüktür. Her
  ping'in ayrı satır olması gerekiyorsa birincil anahtara bir ayırt edici bileşen
  (ör. ping sıra numarası) eklenmelidir.

## Teknoloji yığını

Go · Apache Kafka (KRaft mode) · ScyllaDB · Prometheus · Grafana · WebSocket (gorilla) ·
Leaflet · Docker Compose · Nginx · GitHub Actions

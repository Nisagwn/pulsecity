# Faz 6 — Canlıya Alma (Deployment)

PulseCity'i tek bir sunucuya taşımak için adımlar. Hetzner/DigitalOcean gibi
klasik bir VPS için 1-7. adımları, AWS EC2 için önce "AWS EC2 kurulumu"
bölümünü izle.

## Sunucu boyutlandırma

`deploy/docker-compose.prod.yml` içindeki memory limitlerinin toplamı:

| Servis | Limit |
|---|---|
| ScyllaDB | 2.5G |
| Kafka | 1.5G |
| Consumer × 3 replika | 1.5G |
| Producer | 512M |
| Prometheus | 512M |
| webviz | 512M |
| Grafana | 512M |
| **Toplam** | **~7.5G** |

Yani **en az 8GB RAM** gerekiyor; 4GB'lık bir makinede consumer replikaları OOM
ile ölür. 50k/sn hedefini gerçekten ölçmek istiyorsan 4 vCPU / 16GB tercih et.

## AWS EC2 kurulumu

### 1. Instance seç

| Amaç | Instance tipi | Not |
|---|---|---|
| Sadece demo (link paylaşmak) | `t3.large` (2 vCPU / 8GB) | Yeterli, en ucuz seçenek |
| Benchmark de koşacaksan | `t3.xlarge` veya `m5.xlarge` (4 vCPU / 16GB) | Önerilen |

Free tier `t2.micro`/`t3.micro` (1GB RAM) bu proje için **çalışmaz** — tek başına
ScyllaDB bile sığmaz.

`t3` ailesi burstable'dır: CPU kredisi tükenince throttle olur ve uzun
benchmark'ta throughput düşer. `./scripts/benchmark.sh 300` koşacaksan ya
instance'ı "Unlimited" moda al (ek ücretli) ya da `m5.xlarge` seç.

Diğer ayarlar:
- **AMI:** Ubuntu Server 24.04 LTS (x86_64)
- **Storage:** gp3, en az 30GB (Docker imajları + Kafka log'ları + Scylla verisi)
- **Key pair:** yeni bir key pair oluştur, `.pem` dosyasını sakla

### 2. Security Group

Inbound kuralları — sadece iki port:

| Tip | Port | Kaynak | Amaç |
|---|---|---|---|
| SSH | 22 | **kendi IP'in** (`My IP`) | Yönetim |
| HTTP | 80 | `0.0.0.0/0` | Nginx (harita + Grafana) |

Kafka (9092), ScyllaDB (9042), Prometheus (9090) ve webviz (8080) portlarını
**açma**. `docker-compose.prod.yml` bunları zaten host'a bind etmiyor; Security
Group'ta da kapalı tutmak ikinci savunma katmanı.

SSH'ı `0.0.0.0/0` yapma — sunucu dakikalar içinde brute-force taramasına düşer.

### 3. Bağlan

```bash
chmod 400 pulsecity-key.pem
ssh -i pulsecity-key.pem ubuntu@<EC2_PUBLIC_IP>
```

Sonra aşağıdaki 1. adımdan devam et. AWS'te firewall'u Security Group hallettiği
için 2. adımdaki `ufw` bölümünü atlayabilirsin.

### 4. Elastic IP (opsiyonel ama önerilir)

EC2'nin public IP'si instance her durdurulup başlatıldığında **değişir** —
demo linkin bozulur ve `GRAFANA_ROOT_URL`'ü her seferinde güncellemen gerekir.
EC2 konsolundan bir Elastic IP alıp instance'a bağlarsan IP sabitlenir
(instance çalışırken ücretsizdir).

## 1. Sunucu hazırlığı

```bash
# VPS'e SSH ile bağlan, ardından:
sudo apt update && sudo apt install -y docker.io docker-compose-plugin
sudo usermod -aG docker $USER
# oturumu yeniden aç (grupların etkin olması için)
```

## 2. Firewall

AWS kullanıyorsan bu adımı atla — Security Group aynı işi yapıyor.

```bash
sudo ufw allow OpenSSH
sudo ufw allow 80/tcp     # sadece Nginx (harita + Grafana) dışa açık
sudo ufw enable
```

Not: Kafka (9092), ScyllaDB (9042), Prometheus (9090) portlarını **dışarı açmıyoruz** —
`deploy/docker-compose.prod.yml` bu portları zaten kapatıyor, servisler sadece
Docker internal network üzerinden birbirine erişiyor.

## 3. Projeyi VPS'e taşı

```bash
git clone https://github.com/Nisagwn/pulsecity.git
cd pulsecity
cp .env.example .env
nano .env
```

`.env` dosyası `docker-compose.yml` ile **aynı dizinde** durmalı; Docker Compose
onu otomatik okur, ayrıca bir flag vermene gerek yok. `.gitignore` tarafından
dışlanmıştır, asla commit etme.

Doldurman gereken iki değer:

```bash
GRAFANA_ADMIN_PASSWORD=<güçlü-bir-şifre>
GRAFANA_ROOT_URL=http://<EC2_PUBLIC_IP>/grafana/
```

`GRAFANA_ROOT_URL` sondaki `/grafana/` olmadan yazılırsa Grafana asset
linklerini yanlış üretir ve dashboard CSS'siz/bozuk açılır. Domain aldıysan
IP yerine domain'i yaz.

## 4. Ayağa kaldır

```bash
docker compose -f docker-compose.yml -f deploy/docker-compose.prod.yml up -d --build
```

## 5. Doğrula

```bash
docker compose ps
curl -I http://localhost            # Nginx -> canlı harita (webviz)
curl -I http://localhost/grafana/   # Nginx -> Grafana
```

Tarayıcıdan:

- `http://SUNUCU_IP_ADRESIN/` → **canlı trafik haritası** (demo linki bu)
- `http://SUNUCU_IP_ADRESIN/grafana/` → Grafana dashboard'u, anonim viewer
  erişimiyle (login istemez)

## 6. Throughput'u prod ortamda doğrula

```bash
./scripts/benchmark.sh 300
```

VPS'in gerçek kaynak sınırları local'den farklı olacağı için, 50k/sn hedefine
gerçekten ulaşıp ulaşmadığını burada tekrar ölçmek önemli — sonuçları
Faz 7'deki README'ye ekle.

## 7. Güncelleme akışı

```bash
git pull
docker compose -f docker-compose.yml -f deploy/docker-compose.prod.yml up -d --build
```

## Bilinen sınırlamalar (v1 deployment)

- Tek sunucu = tek hata noktası (SPOF). Multi-node/multi-region Faz kapsamı dışında (kimlik kartında belirtildi).
- TLS/HTTPS yok — v1'de hem harita hem Grafana salt-okunur olarak sadece HTTP
  üzerinden açılıyor. Gerçek bir domain edinirsen `certbot` ile kolayca
  Let's Encrypt eklenebilir.
- AWS'te instance sürekli açık kalır: `t3.large` ~60 USD/ay, `m5.xlarge` ~140 USD/ay
  (+ EBS ve trafik). Demo bittiğinde instance'ı **durdurmayı unutma**; Elastic IP
  aldıysan, instance kapalıyken IP için ücret işlediğini de hatırla.
- Kafka/ScyllaDB tek node (RF=1) — Faz 5'teki chaos test sonuçları bu sınırlamayı
  zaten belgeliyor; production-grade bir kurulumda RF=3 + multi-broker gerekir.

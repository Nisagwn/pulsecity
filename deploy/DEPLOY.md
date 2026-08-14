# Faz 6 — Canlıya Alma (Deployment)

PulseCity'i tek, ucuz bir VPS'e (Hetzner CX22, DigitalOcean Basic Droplet vb. —
2 vCPU / 4GB RAM yeterli) taşımak için adımlar.

## 1. VPS hazırlığı

```bash
# VPS'e SSH ile bağlan, ardından:
sudo apt update && sudo apt install -y docker.io docker-compose-plugin
sudo usermod -aG docker $USER
# oturumu yeniden aç (grupların etkin olması için)
```

## 2. Firewall

```bash
sudo ufw allow OpenSSH
sudo ufw allow 80/tcp     # sadece Nginx (Grafana) dışa açık
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
nano .env   # GRAFANA_ADMIN_PASSWORD'ü mutlaka değiştir
```

## 4. Ayağa kaldır

```bash
docker compose -f docker-compose.yml -f deploy/docker-compose.prod.yml up -d --build
```

## 5. Doğrula

```bash
docker compose ps
curl -I http://localhost   # Nginx -> Grafana yanıt vermeli
```

Tarayıcıdan `http://SUNUCU_IP_ADRESIN/` adresine gidip Grafana dashboard'unu
(anonim viewer erişimiyle) görebilmen gerekir.

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

- Tek VPS = tek hata noktası (SPOF). Multi-node/multi-region Faz kapsamı dışında (kimlik kartında belirtildi).
- TLS/HTTPS yok — v1 için Grafana'yı sadece HTTP üzerinden salt-okunur (viewer) olarak açıyoruz.
  Gerçek bir domain edinirsen `certbot` ile kolayca Let's Encrypt eklenebilir.
- Kafka/ScyllaDB tek node (RF=1) — Faz 5'teki chaos test sonuçları bu sınırlamayı
  zaten belgeliyor; production-grade bir kurulumda RF=3 + multi-broker gerekir.

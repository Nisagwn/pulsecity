#!/bin/bash
# Faz 3 — Performans Benchmark Scripti
#
# Sistemi hedef hızda (50.000 msg/sn) çalıştırır, Prometheus'tan throughput,
# gecikme ve consumer lag metriklerini çeker, sonucu bir markdown tabloya yazar.
#
# Kullanım: ./scripts/benchmark.sh [olcum_suresi_saniye]
#
# Not: docker-compose.yml günlük geliştirme için hafif bir profille (5000 msg/sn)
# gelir. Bu script deploy/docker-compose.bench.yml override'ını kullanarak hedef
# hıza çıkar ve anomali demo modunu kapatır (yapay tıkanıklık ölçümü kirletir).

set -uo pipefail
DURATION=${1:-300}
WARMUP=${WARMUP:-45}
PROM_URL="http://localhost:9090"
OUT_FILE="benchmark-results.md"
COMPOSE="docker compose -f docker-compose.yml -f deploy/docker-compose.bench.yml"

echo "=== Faz 3: Performans Benchmark ==="
echo "Isınma: ${WARMUP}sn, ölçüm: ${DURATION}sn"
echo

# Prometheus'tan tek bir skaler değer çeker.
q() {
  curl -s "${PROM_URL}/api/v1/query" --data-urlencode "query=$1" \
    | grep -oE '"value":\[[0-9.]+,"[^"]*"' | grep -oE '"[^"]*"$' | tr -d '"' | head -1
}

# İnsan tarafından okunabilir sayı (ondalık kırpma)
fmt() { printf "%.0f" "${1:-0}" 2>/dev/null || echo "N/A"; }

echo "[1/4] Hedef hızda ayağa kaldırılıyor (50.000 msg/sn)..."
$COMPOSE up -d --build
echo "Servislerin ısınması için ${WARMUP}sn bekleniyor..."
sleep "$WARMUP"

echo "[2/4] ${DURATION} saniye boyunca yük altında ölçülüyor..."
sleep "$DURATION"

echo "[3/4] Metrikler toplanıyor..."

PRODUCED_RATE=$(q 'rate(pulsecity_producer_messages_total[1m])')
PROCESSED_RATE=$(q 'rate(pulsecity_consumer_messages_processed_total[1m])')
P50_WRITE=$(q 'histogram_quantile(0.50,rate(pulsecity_consumer_batch_write_seconds_bucket[1m]))')
P95_WRITE=$(q 'histogram_quantile(0.95,rate(pulsecity_consumer_batch_write_seconds_bucket[1m]))')
P99_WRITE=$(q 'histogram_quantile(0.99,rate(pulsecity_consumer_batch_write_seconds_bucket[1m]))')
ERROR_RATE=$(q 'rate(pulsecity_producer_errors_total[1m])')
DLQ_RATE=$(q 'rate(pulsecity_consumer_dlq_total[1m])')

# Consumer geride mi: tüketilmeyi bekleyen mesaj sayısı. Throughput rakamından
# daha dürüst bir gösterge - producer hedefe ulaşsa bile consumer yetişemiyorsa
# lag büyür ve sistem gerçekte o hızı taşımıyor demektir.
LAG=$(docker exec pulsecity-kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 --describe --group pulsecity-consumers 2>/dev/null \
  | awk '$2 == "vehicle-pings" && $6 ~ /^[0-9]+$/ { s += $6 } END { print s+0 }')

# Donanım bağlamı olmadan bir benchmark sonucu yorumlanamaz.
CPUS=$(docker info --format '{{.NCPU}}' 2>/dev/null)
DOCKER_MEM_GB=$(docker info --format '{{.MemTotal}}' 2>/dev/null | awk '{printf "%.1f", $1/1024/1024/1024}')

echo "[4/4] Rapor yazılıyor..."

TARGET=50000
ACHIEVED_PCT=$(awk -v p="${PROCESSED_RATE:-0}" -v t="$TARGET" 'BEGIN{ printf "%.0f", p*100/t }')

cat > "$OUT_FILE" << REPORT
# PulseCity — Faz 3 Benchmark Sonuçları

**Test tarihi**: $(date -u +"%Y-%m-%d %H:%M UTC")
**Ölçüm süresi**: ${DURATION} saniye (+ ${WARMUP}sn ısınma)
**Hedef throughput**: ${TARGET} msg/sn

## Test ortamı

| | |
|---|---|
| Docker'a ayrılan CPU | ${CPUS} çekirdek |
| Docker'a ayrılan bellek | ${DOCKER_MEM_GB} GB |
| Kafka | tek broker (KRaft), 12 partition |
| ScyllaDB | tek node, RF=1, 4 shard / 2.5 GB |
| Consumer | 1 replika, batch 2000 |

## Sonuçlar

| Metrik | Değer |
|---|---|
| Üretilen throughput | $(fmt "$PRODUCED_RATE") msg/sn |
| İşlenen throughput | $(fmt "$PROCESSED_RATE") msg/sn |
| Hedefe ulaşma | %${ACHIEVED_PCT} |
| Consumer lag (biriken) | $(fmt "$LAG") mesaj |
| ScyllaDB batch yazma p50 | $(awk -v v="${P50_WRITE:-0}" 'BEGIN{printf "%.1f", v*1000}') ms |
| ScyllaDB batch yazma p95 | $(awk -v v="${P95_WRITE:-0}" 'BEGIN{printf "%.1f", v*1000}') ms |
| ScyllaDB batch yazma p99 | $(awk -v v="${P99_WRITE:-0}" 'BEGIN{printf "%.1f", v*1000}') ms |
| Producer hata oranı | $(fmt "$ERROR_RATE") msg/sn |
| DLQ oranı | $(fmt "$DLQ_RATE") msg/sn |

## Nasıl okunmalı

- **Üretilen ≈ İşlenen ve lag ~0** ise sistem bu hızı gerçekten taşıyor demektir.
- **Lag sürekli büyüyorsa** producer hedefe ulaşsa bile sistem o hızı taşımıyordur;
  consumer'ı \`--scale consumer=3\` ile ölçeklendirip tekrar deneyin.
- **p99 yazma süresi yüksekse** darboğaz ScyllaDB'dedir; \`deploy/docker-compose.bench.yml\`
  içindeki \`--smp\` / \`--memory\` değerlerini artırın.
- Ölçüm sırasında makinede yeterli boş RAM yoksa sonuçlar donanımın değil,
  işletim sisteminin bellek baskısının göstergesidir.

Ham veri: Grafana http://localhost:3000 · canlı harita http://localhost:8080
REPORT

echo
echo "Sonuçlar ${OUT_FILE} dosyasına yazıldı."
echo
cat "$OUT_FILE"

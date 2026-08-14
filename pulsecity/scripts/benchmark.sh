#!/bin/bash
# Faz 3 — Performans Benchmark Scripti
#
# Sistemi belirli bir sürede çalıştırır, Prometheus'tan throughput ve
# consumer lag metriklerini çeker, sonucu bir markdown tabloya yazar.
#
# Kullanım: ./scripts/benchmark.sh [test_suresi_saniye]

set -e
DURATION=${1:-300}   # varsayılan 5 dakika
PROM_URL="http://localhost:9090"
OUT_FILE="benchmark-results.md"

echo "=== Faz 3: Performans Benchmark ==="
echo "Test süresi: ${DURATION}sn"
echo

echo "[1/3] Ortamı ayağa kaldırıyorum..."
docker compose up -d
echo "Servislerin ısınması için 20sn bekleniyor..."
sleep 20

echo "[2/3] ${DURATION} saniye boyunca yük altında bekleniyor..."
sleep "$DURATION"

echo "[3/3] Prometheus'tan metrikleri çekiyorum..."

PRODUCED_RATE=$(curl -s "${PROM_URL}/api/v1/query?query=rate(pulsecity_producer_messages_total[1m])" \
  | grep -oP '"value":\[[0-9.]+,"\K[0-9.]+' | head -1)

PROCESSED_RATE=$(curl -s "${PROM_URL}/api/v1/query?query=rate(pulsecity_consumer_messages_processed_total[1m])" \
  | grep -oP '"value":\[[0-9.]+,"\K[0-9.]+' | head -1)

P99_WRITE=$(curl -s "${PROM_URL}/api/v1/query?query=histogram_quantile(0.99,rate(pulsecity_consumer_batch_write_seconds_bucket[1m]))" \
  | grep -oP '"value":\[[0-9.]+,"\K[0-9.]+' | head -1)

ERROR_RATE=$(curl -s "${PROM_URL}/api/v1/query?query=rate(pulsecity_producer_errors_total[1m])" \
  | grep -oP '"value":\[[0-9.]+,"\K[0-9.]+' | head -1)

cat > "$OUT_FILE" << REPORT
# PulseCity — Faz 3 Benchmark Sonuçları

**Test tarihi**: $(date -u +"%Y-%m-%d %H:%M UTC")
**Test süresi**: ${DURATION} saniye
**Hedef throughput**: 50.000 msg/sn (docker-compose.yml -> TARGET_RATE_PER_SEC)

| Metrik | Değer |
|---|---|
| Üretilen throughput (msg/sn) | ${PRODUCED_RATE:-N/A} |
| İşlenen throughput (msg/sn) | ${PROCESSED_RATE:-N/A} |
| ScyllaDB batch yazma p99 (sn) | ${P99_WRITE:-N/A} |
| Producer hata oranı (msg/sn) | ${ERROR_RATE:-N/A} |

## Yorum
- Üretilen ve işlenen throughput birbirine yakınsa, consumer darboğaz oluşturmuyor demektir.
- Fark büyükse: consumer'ı \`docker compose up -d --scale consumer=3\` ile ölçeklendirip tekrar dene.
- p99 yazma süresi yüksekse: ScyllaDB \`--smp\` / \`--memory\` ayarlarını (docker-compose.yml) artırmayı düşün.

Ham veri için Grafana'yı ziyaret et: http://localhost:3000 (admin / pulsecity)
REPORT

echo
echo "Sonuçlar ${OUT_FILE} dosyasına yazıldı."
cat "$OUT_FILE"

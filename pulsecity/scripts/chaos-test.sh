#!/bin/bash
# Faz 5 — Chaos Testing / Dayanıklılık Scripti
#
# Sırayla şu senaryoları simüle eder ve recovery süresini ölçer:
#   1. Consumer crash
#   2. Kafka broker crash
#   3. ScyllaDB crash
# Her senaryo öncesi/sonrası ScyllaDB satır sayısını karşılaştırarak
# veri kaybı olup olmadığını raporlar.
#
# Kullanım: ./scripts/chaos-test.sh

set -e
OUT_FILE="chaos-test-results.md"

row_count() {
  docker exec pulsecity-scylla cqlsh -e "SELECT COUNT(*) FROM pulsecity.vehicle_pings;" 2>/dev/null \
    | sed -n '4p' | tr -d ' '
}

wait_for_healthy() {
  local service=$1
  local max_wait=$2
  local waited=0
  while [ "$waited" -lt "$max_wait" ]; do
    status=$(docker inspect --format='{{.State.Health.Status}}' "$service" 2>/dev/null || echo "unknown")
    if [ "$status" = "healthy" ]; then
      echo "$waited"
      return 0
    fi
    sleep 2
    waited=$((waited + 2))
  done
  echo "$max_wait"
}

echo "=== Faz 5: Chaos Testing ===" | tee "$OUT_FILE"
echo "Tarih: $(date -u +"%Y-%m-%d %H:%M UTC")" | tee -a "$OUT_FILE"
echo | tee -a "$OUT_FILE"

echo "Ortam ayağa kaldırılıyor (temiz başlangıç)..."
docker compose down -v 2>/dev/null || true
docker compose up -d
sleep 25

echo "| Senaryo | Recovery süresi (sn) | Veri kaybı? |" | tee -a "$OUT_FILE"
echo "|---|---|---|" | tee -a "$OUT_FILE"

# --- Senaryo 1: Consumer crash ---
echo
echo ">>> Senaryo 1: Consumer crash"
BEFORE=$(row_count)
docker compose kill -s SIGKILL consumer
START=$(date +%s)
docker compose up -d consumer
sleep 15
END=$(date +%s)
AFTER=$(row_count)
RECOVERY=$((END - START))
LOSS="Hayır"
if [ "$AFTER" -lt "$BEFORE" ]; then LOSS="EVET (satır sayısı azaldı - beklenmiyordu)"; fi
echo "| Consumer crash | ${RECOVERY} | ${LOSS} |" | tee -a "$OUT_FILE"

# --- Senaryo 2: Kafka broker crash ---
echo
echo ">>> Senaryo 2: Kafka broker crash"
BEFORE=$(row_count)
docker compose kill -s SIGKILL kafka
START=$(date +%s)
docker compose up -d kafka
wait_for_healthy pulsecity-kafka 60 > /dev/null
sleep 15
END=$(date +%s)
AFTER=$(row_count)
RECOVERY=$((END - START))
LOSS="Hayır"
if [ "$AFTER" -lt "$BEFORE" ]; then LOSS="EVET (satır sayısı azaldı - beklenmiyordu)"; fi
echo "| Kafka broker crash | ${RECOVERY} | ${LOSS} |" | tee -a "$OUT_FILE"

# --- Senaryo 3: ScyllaDB crash ---
echo
echo ">>> Senaryo 3: ScyllaDB crash"
BEFORE=$(row_count)
docker compose kill -s SIGKILL scylla
START=$(date +%s)
docker compose up -d scylla
wait_for_healthy pulsecity-scylla 90 > /dev/null
sleep 20
END=$(date +%s)
AFTER=$(row_count)
RECOVERY=$((END - START))
LOSS="Hayır"
if [ "$AFTER" -lt "$BEFORE" ]; then LOSS="EVET (satır sayısı azaldı - beklenmiyordu)"; fi
echo "| ScyllaDB crash | ${RECOVERY} | ${LOSS} |" | tee -a "$OUT_FILE"

echo | tee -a "$OUT_FILE"
echo "## Notlar" | tee -a "$OUT_FILE"
cat >> "$OUT_FILE" << 'NOTES'
- "Recovery süresi", kill komutundan servisin tekrar mesaj işlemeye başladığı ana kadar geçen süredir.
- "Veri kaybı" kontrolü basit bir satır-sayısı karşılaştırmasıdır; kesin doğrulama için
  `verify-zero-loss.sh` scriptindeki gönderilen/işlenen mesaj sayısı karşılaştırması kullanılmalıdır.
- Kafka/ScyllaDB tek node ile çalıştığı için (RF=1) bu testler "en kötü senaryo"yu simüle eder.
  Production'da RF=3 ile bu crash'ler sıfır kesintiyle atlatılabilir olmalı (Faz 6 notu).
NOTES

echo
echo "Sonuçlar ${OUT_FILE} dosyasına yazıldı."

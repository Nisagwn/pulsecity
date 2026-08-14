#!/bin/bash
# Faz 2 — Zero-Loss Doğrulama Scripti
#
# Bu script şunu kanıtlar: producer belli bir sürede kaç mesaj gönderdiyse,
# consumer crash/restart olsa bile ScyllaDB'de tam olarak o kadar satır olmalı.
#
# Kullanım: ./scripts/verify-zero-loss.sh [test_suresi_saniye]

set -e
TEST_DURATION=${1:-60}

echo "=== Faz 2: Zero-Loss Doğrulama Testi ==="
echo "Test süresi: ${TEST_DURATION}sn"
echo

echo "[1/5] Ortamı temiz başlatıyorum..."
docker compose down -v 2>/dev/null || true
docker compose up -d kafka kafka-init scylla scylla-init
echo "Kafka + ScyllaDB hazır olana kadar bekleniyor..."
sleep 15

echo "[2/5] Producer'ı başlatıyorum, consumer'ı henüz BAŞLATMIYORUM (kuyrukta birikim test etmek için)..."
echo "(Not: docker-compose.yml'deki TARGET_RATE_PER_SEC değerinde çalışır - varsayılan 50000/sn."
echo " Daha hafif/hızlı bir test istersen: TARGET_RATE_PER_SEC=1000 docker compose up -d producer)"
docker compose up -d producer
sleep "$TEST_DURATION"

echo "[3/5] Producer'ı durduruyorum ve gönderilen toplam mesaj sayısını logdan okuyorum..."
docker compose stop producer
SENT_TOTAL=$(docker compose logs producer | grep -oP 'toplam: \K[0-9]+' | tail -1)
echo "Producer'ın gönderdiği toplam mesaj: ${SENT_TOTAL}"

echo "[4/5] Consumer'ı başlatıyorum, işleme sırasında KASITLI olarak crash simüle ediyorum..."
docker compose up -d consumer
sleep 5
echo "Consumer'ı ortasında öldürüyorum (kill -9, graceful shutdown YOK)..."
docker compose kill -s SIGKILL consumer
sleep 2
echo "Consumer'ı yeniden başlatıyorum (commit edilmemiş offsetlerden devam etmeli)..."
docker compose up -d consumer

echo "İşlemenin tamamlanması için bekleniyor..."
sleep 30

echo "[5/5] ScyllaDB'deki satır sayısını sayıyorum..."
DB_COUNT=$(docker exec pulsecity-scylla cqlsh -e "SELECT COUNT(*) FROM pulsecity.vehicle_pings;" 2>/dev/null | sed -n '4p' | tr -d ' ')
DLQ_COUNT=$(docker exec pulsecity-scylla cqlsh -e "SELECT COUNT(*) FROM pulsecity.vehicle_pings_dlq;" 2>/dev/null | sed -n '4p' | tr -d ' ')

echo
echo "=== SONUÇ ==="
echo "Gönderilen mesaj sayısı (producer log):   ${SENT_TOTAL}"
echo "ScyllaDB'de yazılan satır sayısı:          ${DB_COUNT}"
echo "DLQ'ya yönlendirilen mesaj sayısı:         ${DLQ_COUNT}"
echo "Toplam işlenen (DB + DLQ):                 $((DB_COUNT + DLQ_COUNT))"
echo

if [ "$SENT_TOTAL" -eq "$((DB_COUNT + DLQ_COUNT))" ]; then
  echo "✅ BAŞARILI: Gönderilen mesaj sayısı ile işlenen (DB+DLQ) sayısı eşit. Veri kaybı YOK."
else
  echo "❌ BAŞARISIZ: Sayılar eşleşmiyor. Fark: $((SENT_TOTAL - DB_COUNT - DLQ_COUNT)) mesaj kayıp olabilir."
  echo "   Not: Consumer henüz batch'i işlerken yakalanmışsa bu normal olabilir - birkaç dakika"
  echo "   daha bekleyip DB_COUNT'u tekrar kontrol edin (asenkron flush interval nedeniyle gecikme olabilir)."
fi

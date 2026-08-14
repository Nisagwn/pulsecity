#!/bin/bash
# Faz 2 — Zero-Loss Doğrulama Scripti
#
# Bu script şunu kanıtlar: consumer işlemenin ortasında SIGKILL ile öldürülse
# bile, Kafka'ya giren hiçbir mesaj kaybolmaz.
#
# ÖNEMLİ — neyin neyle karşılaştırıldığı:
# Gönderilen ham mesaj sayısı ile ScyllaDB satır sayısı DOĞRUDAN karşılaştırılamaz.
# Tablonun birincil anahtarı (zone_id, ping_time, vehicle_id) ve ScyllaDB'nin
# `timestamp` tipi milisaniye hassasiyetinde; aynı aracın aynı milisaniyeye düşen
# iki ping'i ikinci bir satır DEĞİL, üzerine yazma üretir. Ölçülen çakışma oranı
# ~%7. Dolayısıyla doğru karşılaştırma şudur:
#
#   Kafka'daki TEKİL birincil anahtar sayısı  ==  ScyllaDB satırı + DLQ mesajı
#
# Ayrıca boru hattının hiçbir şeyi atlamadığı, consumer group lag'inin 0 olması
# (yani her offset'in commit edilmiş olması) ile ayrıca doğrulanır.
#
# Kullanım: ./scripts/verify-zero-loss.sh [test_suresi_saniye]

set -uo pipefail
TEST_DURATION=${1:-60}
COMPOSE="docker compose"

echo "=== Faz 2: Zero-Loss Doğrulama Testi ==="
echo "Test süresi: ${TEST_DURATION}sn"
echo

# --- yardımcılar ---------------------------------------------------------

# Bir topic'teki toplam mesaj sayısı (tüm partition'ların son offset toplamı).
kafka_msg_count() {
  MSYS_NO_PATHCONV=1 docker exec pulsecity-kafka \
    kafka-run-class kafka.tools.GetOffsetShell \
    --bootstrap-server localhost:9092 --topic "$1" --time -1 2>/dev/null \
    | awk -F: '{ s += $3 } END { print s+0 }'
}

# Consumer group'un COMMIT ETTİĞİ toplam offset.
#
# Neden lag değil de bu: `--describe` çıktısı rebalance sırasında boş gelir,
# boş çıktıdan hesaplanan lag 0 görünür ve "her şey bitti" sanılır - consumer
# daha hiçbir şey yazmamışken sayıma geçilir. Commit edilen offset toplamı ise
# boş çıktıda 0 kalır, yani hedefe (KAFKA_TOTAL) eşit olmaz ve döngü doğru
# şekilde beklemeye devam eder.
#
# Ayrıca: consumer offset'i ScyllaDB'ye YAZDIKTAN SONRA commit ettiği için,
# commit toplamı == Kafka toplamı olduğu anda tüm satırlar zaten diske inmiştir.
consumer_committed() {
  MSYS_NO_PATHCONV=1 docker exec pulsecity-kafka \
    kafka-consumer-groups --bootstrap-server localhost:9092 \
    --describe --group pulsecity-consumers 2>/dev/null \
    | awk '$2 == "vehicle-pings" && $4 ~ /^[0-9]+$/ { s += $4 } END { print s+0 }'
}

# Topic'teki mesajları okuyup ScyllaDB birincil anahtarına göre TEKİL sayar.
# ping_time milisaniyeye kırpılır - Go'nun RFC3339Nano çıktısı ondalık
# saniyedeki sondaki sıfırları kırptığı için ondalık kısım sağdan sıfırla
# 3 haneye tamamlanır (".8Z" -> ".800").
kafka_distinct_keys() {
  local total=$1
  MSYS_NO_PATHCONV=1 docker exec pulsecity-kafka \
    kafka-console-consumer --bootstrap-server localhost:9092 \
    --topic vehicle-pings --from-beginning \
    --max-messages "$total" --timeout-ms 120000 2>/dev/null \
  | awk '
      {
        if (!match($0, /"vehicle_id":"[^"]*"/)) next
        v = substr($0, RSTART+14, RLENGTH-15)
        if (!match($0, /"zone_id":"[^"]*"/))    next
        z = substr($0, RSTART+11, RLENGTH-12)
        if (!match($0, /"timestamp":"[^"]*"/))  next
        t = substr($0, RSTART+13, RLENGTH-14)

        sub(/Z$/, "", t)
        dot = index(t, ".")
        if (dot == 0) { sec = t; frac = "" }
        else          { sec = substr(t, 1, dot-1); frac = substr(t, dot+1) }
        frac = substr(frac "000", 1, 3)
        print z "|" sec "." frac "|" v
      }' \
  | sort -u | wc -l | tr -d ' '
}

scylla_row_count() {
  MSYS_NO_PATHCONV=1 docker exec pulsecity-scylla \
    cqlsh --request-timeout=600 \
    -e "SELECT COUNT(*) FROM pulsecity.vehicle_pings;" 2>/dev/null \
    | grep -E '^\s+[0-9]+\s*$' | tr -d ' '
}

# --- test ----------------------------------------------------------------

echo "[1/6] Ortamı temiz başlatıyorum..."
$COMPOSE down -v >/dev/null 2>&1
$COMPOSE up -d kafka kafka-init scylla scylla-init
echo "Kafka + ScyllaDB hazır olana kadar bekleniyor..."
sleep 15

echo "[2/6] Producer'ı başlatıyorum (consumer henüz KAPALI - kuyrukta birikim olsun)..."
echo "(Hız docker-compose.yml'deki TARGET_RATE_PER_SEC değerinden gelir)"
$COMPOSE up -d producer
sleep "$TEST_DURATION"

echo "[3/6] Producer'ı durduruyorum..."
$COMPOSE stop producer >/dev/null
sleep 3
KAFKA_TOTAL=$(kafka_msg_count vehicle-pings)
echo "Kafka'ya giren toplam mesaj: ${KAFKA_TOTAL}"

echo "[4/6] Consumer'ı başlatıyorum ve işlemenin ORTASINDA SIGKILL ile öldürüyorum..."
# webviz de birlikte ayağa kaldırılıyor: Faz 9'da eklenen harita servisi AYNI
# topic'i okuyor ama KENDİ consumer group'unda. Testin bu koşulda da geçmesi,
# webviz'in ana consumer'dan mesaj çalmadığının ve offset'lerine dokunmadığının
# kanıtı - yani sunum katmanı zero-loss garantisini bozmuyor.
$COMPOSE up -d consumer webviz
sleep 5
$COMPOSE kill -s SIGKILL consumer >/dev/null
sleep 2
echo "Consumer'ı yeniden başlatıyorum (commit edilmemiş offsetlerden devam etmeli)..."
$COMPOSE up -d consumer

echo "[5/6] Tüm mesajlar tüketilene kadar bekleniyor..."
COMMITTED=0
for _ in $(seq 1 60); do
  sleep 5
  COMMITTED=$(consumer_committed)
  echo "  commit edilen: ${COMMITTED} / ${KAFKA_TOTAL}"
  [ "$COMMITTED" -ge "$KAFKA_TOTAL" ] && break
done
REMAINING=$((KAFKA_TOTAL - COMMITTED))

echo "[6/6] Sayımlar yapılıyor (tekil anahtar hesabı büyük topiklerde biraz sürer)..."
EXPECTED=$(kafka_distinct_keys "$KAFKA_TOTAL")
DB_COUNT=$(scylla_row_count)
DLQ_COUNT=$(kafka_msg_count vehicle-pings-dlq)
COLLAPSED=$((KAFKA_TOTAL - EXPECTED))

echo
echo "=== SONUÇ ==="
printf "Kafka'ya giren ham mesaj              : %s\n" "$KAFKA_TOTAL"
printf "  bunlardan aynı anahtara düşen       : %s (üzerine yazılır, kayıp DEĞİL)\n" "$COLLAPSED"
printf "  beklenen tekil satır                : %s\n" "$EXPECTED"
echo   "---"
printf "ScyllaDB'deki satır                   : %s\n" "$DB_COUNT"
printf "DLQ topic'indeki mesaj                : %s\n" "$DLQ_COUNT"
printf "Toplam hesaba katılan (DB + DLQ)      : %s\n" "$((DB_COUNT + DLQ_COUNT))"
printf "Tüketilmeyen kalan mesaj              : %s\n" "$REMAINING"
echo

FAIL=0
if [ "$REMAINING" -ne 0 ]; then
  echo "⚠️  UYARI: Consumer hâlâ geride (${REMAINING} mesaj). Sonuç kesin değil, süreyi uzatın."
  FAIL=1
fi

if [ "$((DB_COUNT + DLQ_COUNT))" -eq "$EXPECTED" ]; then
  echo "✅ BAŞARILI: Beklenen tekil satır sayısı ile (DB + DLQ) birebir eşit. Veri kaybı YOK."
  echo "   Consumer SIGKILL ile öldürülmesine rağmen commit edilmemiş offsetler yeniden işlendi."
else
  DIFF=$((EXPECTED - DB_COUNT - DLQ_COUNT))
  echo "❌ BAŞARISIZ: ${DIFF} kayıt eksik."
  echo "   (Ham mesaj sayısıyla değil, TEKİL anahtar sayısıyla karşılaştırıldı -"
  echo "    yani bu fark milisaniye çakışmasından değil, gerçek kayıptan geliyor.)"
  FAIL=1
fi

exit $FAIL

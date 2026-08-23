#!/bin/bash
# Faz 18 — webviz için blue-green deploy, alarm tabanlı otomatik rollback
#
# NEDEN SADECE WEBVIZ:
# Consumer zaten `--scale consumer=3` ile yatay ve Kafka consumer group'u
# rebalance'ı kendisi yönetiyor; bir replika inip kalktığında diğerleri
# devralıyor. Stateful bileşenler (Kafka, ScyllaDB) blue-green'e hiç girmiyor —
# iki kopya aynı veriyi paylaşamaz. Geriye gerçekten kesintisiz deploy'a
# ihtiyaç duyan tek katman kalıyor: kullanıcıya bakan webviz.
#
# NASIL ÇALIŞIYOR:
# Caddy iki upstream'i birden tanıyor (`lb_policy first`, bkz. deploy/Caddyfile)
# ve her zaman sıradaki İLK SAĞLIKLI olanı seçiyor. Geçiş bu yüzden bir
# konteyner işlemi: yeşili ayağa kaldır, hazır olduğunu doğrula, maviyi durdur.
# Caddy yapılandırması hiç değişmiyor, reload yok.
#
# ROLLBACK KARARI ALARMA DAYANIYOR:
# Geçişten sonra bir gözlem penceresi boyunca Alertmanager'daki `critical`
# alarmlara ve webviz'in kendi metriklerine bakılıyor. Bu, Faz 13'teki alarm
# zinciri olmadan yapılamazdı — "kötü gidiyor"un makine tarafından okunabilir
# bir tanımı olmadan otomatik rollback, tahmin olurdu.
#
# Kullanım:
#   ./scripts/deploy-webviz.sh [gozlem_suresi_saniye]

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

WATCH_SECONDS="${1:-60}"

# Hangi compose dosyalarıyla çalışılacağı dışarıdan değiştirilebilir.
# Varsayılan prod; test/doğrulama için hafif bir profil verilebilir:
#   PULSECITY_COMPOSE_FILES="-f docker-compose.yml -f deploy/docker-compose.ci.yml" ./scripts/deploy-webviz.sh
COMPOSE_FILES="${PULSECITY_COMPOSE_FILES:--f docker-compose.yml -f deploy/docker-compose.prod.yml}"
COMPOSE="docker compose $COMPOSE_FILES"
BLUE="pulsecity-webviz"
GREEN="pulsecity-webviz-green"
# Prometheus ve Alertmanager PROD'da dışa port AÇMIYOR
# (deploy/docker-compose.prod.yml -> ports: []). Bu yüzden sorgular host'tan
# değil, konteynerin KENDİ içinden yapılıyor — aksi halde bu betik tam da
# çalışması gereken yerde, sunucuda çalışmazdı.
PROM_C="pulsecity-prometheus"
ALERTMANAGER_C="pulsecity-alertmanager"

log()  { printf '\n\033[1m[deploy] %s\033[0m\n' "$*"; }
warn() { printf '\033[33m[deploy] %s\033[0m\n' "$*"; }
err()  { printf '\033[31m[deploy] %s\033[0m\n' "$*" >&2; }

# --- Prometheus/Alertmanager yardımcıları -----------------------------------

am_get() {
  docker exec "$ALERTMANAGER_C" wget -qO- "http://localhost:9093$1" 2>/dev/null || true
}

# Alertmanager'da o an aktif olan critical alarm sayısı.
#
# DİKKAT: `grep` eşleşme bulamadığında 1 döner ve `set -o pipefail` altında
# tüm boru hattını başarısız kılar. Yani "hiç alarm yok" — asıl beklenen ve
# istenen durum — betiği çökertirdi. `{ grep || true; }` bunu yutuyor.
critical_alerts() {
  local body
  body="$(am_get '/api/v2/alerts?active=true')"
  printf '%s' "$body" \
    | { grep -o '"severity":"critical"' || true; } \
    | wc -l | tr -d ' '
}

firing_alert_names() {
  local body
  body="$(am_get '/api/v2/alerts?active=true')"
  printf '%s' "$body" \
    | { grep -oE '"alertname":"[^"]+"' || true; } \
    | sed 's/"alertname":"//;s/"//' | sort -u
}

# wget --data-urlencode desteklemediği için sorgu elle kodlanıyor.
#
# `|| true`: Prometheus erişilemezse (konteyner durmuş, henüz ayağa kalkmamış)
# bu fonksiyon boş dönmeli, betiği ÖLDÜRMEMELİ. İlk sürümde bu koruma yoktu ve
# `set -e` altında betik geçişten hemen sonra sessizce sonlanıyordu — blue
# durdurulmuş, green doğrulanmamış, ortada servis veren hiçbir şey kalmamış
# halde. Bir deploy betiğinin yapabileceği en kötü şey budur.
promq() {
  local q
  q="$(printf '%s' "$1" | sed 's/%/%25/g; s/ /%20/g; s/{/%7B/g; s/}/%7D/g; s/"/%22/g; s/=/%3D/g; s/(/%28/g; s/)/%29/g; s/|/%7C/g; s/,/%2C/g')"
  { docker exec "$PROM_C" wget -qO- "http://localhost:9090/api/v1/query?query=$q" 2>/dev/null || true; } \
    | { sed -n 's/.*"value":\[[0-9.]*,"\([^"]*\)"\].*/\1/p' || true; }
}

# --- Güvenli tarafa dönüş ----------------------------------------------------
#
# SWITCHED, blue durdurulduktan sonra 1 olur. Betik o noktadan sonra HERHANGI
# bir nedenle beklenmedik şekilde sonlanırsa (bir komutun `set -e` ile
# tetiklenmesi, Ctrl-C, kill) bu tuzak blue'yu geri getirir.
#
# Gerekçesi yukarıdaki `promq` notuyla aynı olay: ilk sürümde betik gözlem
# döngüsünün ilk adımında öldü ve sistemi TAMAMEN kapalı bıraktı. Doğrulama
# mantığını düzeltmek yeterli değil — betiğin nasıl öldüğünden bağımsız olarak
# sistemin ayakta kalması gerekiyor.
SWITCHED=0
on_exit() {
  local code=$?
  if [ "$code" -ne 0 ] && [ "$SWITCHED" -eq 1 ]; then
    err ""
    err "Betik beklenmedik şekilde sonlandı (çıkış kodu $code)."
    err "Güvenli tarafa dönülüyor: blue geri getiriliyor."
    $COMPOSE up -d webviz >/dev/null 2>&1 || true
    $COMPOSE --profile bluegreen stop webviz-green >/dev/null 2>&1 || true
    err "Blue geri alındı."
  fi
}
trap on_exit EXIT

# --- Ön kontrol --------------------------------------------------------------

log "Ön kontrol: dağıtım öncesi sistem sağlıklı mı?"

PRE_CRITICAL="$(critical_alerts)"
if [ "${PRE_CRITICAL:-0}" -gt 0 ]; then
  err "Dağıtımdan ÖNCE $PRE_CRITICAL critical alarm var:"
  firing_alert_names | sed 's/^/    /'
  err "Önce mevcut sorunu çöz. Bozuk bir sistemin üzerine deploy etmek,"
  err "rollback kararını da anlamsız kılar (neyin bozulduğu ayırt edilemez)."
  exit 1
fi
echo "  critical alarm yok — devam."

# --- 1) Yeşili ayağa kaldır --------------------------------------------------

log "1/5 Yeni sürüm (green) build ediliyor ve başlatılıyor"
$COMPOSE --profile bluegreen up -d --build webviz-green

log "2/5 Green'in hazır olması bekleniyor"
for i in $(seq 1 60); do
  status="$(docker inspect -f '{{.State.Health.Status}}' "$GREEN" 2>/dev/null || echo "yok")"
  if [ "$status" = "healthy" ]; then
    echo "  green healthy (${i}. deneme)"
    break
  fi
  if [ "$i" -eq 60 ]; then
    err "Green 5 dakikada hazır olmadı (durum: $status). Trafik hiç aktarılmadı."
    docker logs "$GREEN" --tail 30 || true
    $COMPOSE --profile bluegreen stop webviz-green || true
    exit 1
  fi
  sleep 5
done

# --- 2) Trafiği aktarmadan önce doğrudan test et -----------------------------

log "3/5 Green doğrudan test ediliyor (trafik hâlâ blue'da)"
if ! docker exec "$GREEN" wget -qO- http://localhost:8080/readyz >/dev/null 2>&1; then
  err "Green /readyz yanıt vermiyor. Geçiş yapılmadı."
  $COMPOSE --profile bluegreen stop webviz-green || true
  exit 1
fi
if ! docker exec "$GREEN" wget -qO- http://localhost:8080/ 2>/dev/null | grep -qi "<!doctype\|<html"; then
  err "Green harita sayfasını servis etmiyor. Geçiş yapılmadı."
  $COMPOSE --profile bluegreen stop webviz-green || true
  exit 1
fi
echo "  green /readyz ve / yanıt veriyor."

# --- 3) Geçiş ----------------------------------------------------------------

log "4/5 Trafik green'e aktarılıyor (blue durduruluyor)"
# Caddy `lb_policy first` kullandığı için blue durunca sıradaki sağlıklı
# upstream (green) devralıyor. Konfigürasyon değişmiyor.
$COMPOSE stop webviz
SWITCHED=1   # bu noktadan sonra beklenmedik çıkışta trap blue'yu geri getirir
echo "  blue durduruldu — Caddy artık green'e yönlendiriyor."

# --- 4) Gözlem penceresi -----------------------------------------------------

log "5/5 ${WATCH_SECONDS}sn gözlem — alarmlar izleniyor"

rollback() {
  # Trap'in aynı işi ikinci kez yapmasını engelle: buradan sonra geri dönüş
  # bilinçli ve kontrollü, "beklenmedik çıkış" değil.
  SWITCHED=0
  err "ROLLBACK: blue geri getiriliyor"
  $COMPOSE up -d webviz
  for i in $(seq 1 30); do
    if [ "$(docker inspect -f '{{.State.Health.Status}}' "$BLUE" 2>/dev/null)" = "healthy" ]; then
      break
    fi
    sleep 3
  done
  $COMPOSE --profile bluegreen stop webviz-green || true
  err "Blue geri alındı, green durduruldu."
  err "Green loglarının son 40 satırı:"
  docker logs "$GREEN" --tail 40 2>&1 | sed 's/^/    /' || true
  exit 1
}

elapsed=0
step=5
while [ "$elapsed" -lt "$WATCH_SECONDS" ]; do
  sleep "$step"
  elapsed=$((elapsed + step))

  # a) critical alarm çıktı mı
  now_critical="$(critical_alerts)"
  if [ "${now_critical:-0}" -gt 0 ]; then
    err "Geçişten sonra $now_critical critical alarm ateşlendi:"
    firing_alert_names | sed 's/^/    /'
    rollback
  fi

  # b) green hâlâ ÇALIŞIYOR ve sağlıklı mı
  #
  # İki ayrı kontrol: durmuş/öldürülmüş bir konteynerde
  # `.State.Health.Status` son bilinen değeri döndürebilir ya da hiç
  # dönmeyebilir — tek başına güvenilmez. Önce gerçekten ayakta mı diye bak.
  grunning="$(docker inspect -f '{{.State.Running}}' "$GREEN" 2>/dev/null || echo "false")"
  if [ "$grunning" != "true" ]; then
    err "Green artık çalışmıyor (çıkış kodu: $(docker inspect -f '{{.State.ExitCode}}' "$GREEN" 2>/dev/null || echo '?'))."
    rollback
  fi

  gstatus="$(docker inspect -f '{{.State.Health.Status}}' "$GREEN" 2>/dev/null || echo "yok")"
  if [ "$gstatus" != "healthy" ]; then
    err "Green sağlıksız duruma düştü (durum: $gstatus)."
    rollback
  fi

  # c) green gerçekten Kafka'dan okuyor mu (sessizce boş bir harita servis
  #    etmesin — ayakta ama işe yaramaz bir sürüm en sinsi başarısızlıktır)
  consumed="$(promq 'sum(pulsecity_webviz_messages_consumed_total)')"
  echo "  ${elapsed}sn — critical alarm: 0, green: healthy, tüketilen mesaj: ${consumed:-?}"
done

# --- 5) Onayla ---------------------------------------------------------------

log "Gözlem penceresi temiz geçti."
warn "Green şu an trafiği taşıyor ama adı hâlâ 'webviz-green'."
warn "Kalıcı hale getirmek için (kısa bir kesinti ile):"
warn "    $COMPOSE up -d --build webviz && \\"
warn "    $COMPOSE --profile bluegreen stop webviz-green"
warn ""
warn "Bu son adım bilerek otomatik DEĞİL: asıl amaç olan doğrulama tamamlandı,"
warn "geri kalanı isim düzeltmesi ve ne zaman yapılacağına operatör karar vermeli."
echo ""
echo "Rollback gerekirse:"
echo "    $COMPOSE up -d webviz && $COMPOSE --profile bluegreen stop webviz-green"

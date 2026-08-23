#!/bin/bash
# Faz 15 — SOPS + age ile sır yönetimi
#
# Depoda durması gereken ama düz metin olamayacak yapılandırmayı yönetir
# (bkz. .sops.yaml — SOPS ile SSM'in neden ikisi birden var olduğu orada).
#
# sops yerel olarak kuruluysa o kullanılır; değilse Docker'a düşer, yani
# kurulum zorunlu değil.
#
# Kullanım:
#   ./scripts/secrets.sh init             age anahtarı üret, .sops.yaml'a yaz
#   ./scripts/secrets.sh encrypt <dosya>  yerinde şifrele
#   ./scripts/secrets.sh decrypt <dosya>  stdout'a çöz
#   ./scripts/secrets.sh edit <dosya>     çöz, editörde aç, tekrar şifrele
#   ./scripts/secrets.sh check            secrets/ altındakiler gerçekten
#                                         şifreli mi (CI bunu kullanır)

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

SOPS_IMAGE="ghcr.io/getsops/sops:v3.9.0"
AGE_KEY_FILE="${SOPS_AGE_KEY_FILE:-$HOME/.config/sops/age/keys.txt}"

# --- sops çağrısı: yerel varsa yerel, yoksa Docker --------------------------
sops_run() {
  if command -v sops >/dev/null 2>&1; then
    SOPS_AGE_KEY_FILE="$AGE_KEY_FILE" sops "$@"
  else
    # Anahtar dosyası ve depo konteynere bağlanır. MSYS_NO_PATHCONV Git Bash'in
    # /sops gibi yolları Windows yoluna çevirmesini engeller.
    MSYS_NO_PATHCONV=1 docker run --rm -i \
      -v "$(pwd)":/repo \
      -v "$AGE_KEY_FILE":/age/keys.txt:ro \
      -e SOPS_AGE_KEY_FILE=/age/keys.txt \
      -w /repo \
      "$SOPS_IMAGE" "$@"
  fi
}

usage() { sed -n '2,20p' "$0"; exit 1; }

cmd="${1:-}"
[ -z "$cmd" ] && usage

case "$cmd" in

  init)
    if [ -f "$AGE_KEY_FILE" ]; then
      echo "Anahtar zaten var: $AGE_KEY_FILE"
      echo "Yeniden üretmek istersen önce onu taşı/sil."
    else
      mkdir -p "$(dirname "$AGE_KEY_FILE")"
      echo "age anahtarı üretiliyor..."
      if command -v age-keygen >/dev/null 2>&1; then
        age-keygen -o "$AGE_KEY_FILE"
      elif command -v go >/dev/null 2>&1; then
        # age-keygen kurulu değilse Go ile çalıştır (ek kurulum gerekmez).
        go run filippo.io/age/cmd/age-keygen@latest -o "$AGE_KEY_FILE"
      else
        echo "HATA: age-keygen de Go da bulunamadı." >&2
        echo "  https://github.com/FiloSottile/age adresinden age kur." >&2
        exit 1
      fi
      chmod 600 "$AGE_KEY_FILE"
    fi

    PUBKEY="$(grep -oE 'age1[a-z0-9]+' "$AGE_KEY_FILE" | head -1)"
    if [ -z "$PUBKEY" ]; then
      echo "HATA: açık anahtar okunamadı: $AGE_KEY_FILE" >&2
      exit 1
    fi

    echo ""
    echo "Açık anahtar : $PUBKEY"
    echo "Özel anahtar : $AGE_KEY_FILE  (ASLA commit etme)"

    # .sops.yaml'daki placeholder'ı gerçek anahtarla değiştir.
    if grep -q "age1PLACEHOLDER" .sops.yaml; then
      sed -i.bak "s|age1PLACEHOLDER[0-9]*|$PUBKEY|" .sops.yaml && rm -f .sops.yaml.bak
      echo ""
      echo ".sops.yaml güncellendi. Şimdi şablonları doldurup şifreleyebilirsin:"
      echo "  cp secrets/prod.env.example        secrets/prod.env.enc"
      echo "  cp secrets/slack_api_url.example   secrets/slack_api_url.enc"
      echo "  # ikisini de gerçek değerlerle doldur, sonra:"
      echo "  ./scripts/secrets.sh encrypt secrets/prod.env.enc"
      echo "  ./scripts/secrets.sh encrypt secrets/slack_api_url.enc"
    else
      echo ""
      echo ".sops.yaml'da placeholder yok - anahtar zaten yapılandırılmış."
      echo "Bu anahtarı da eklemek istersen .sops.yaml'daki age: listesine"
      echo "yeni bir satır olarak koy, sonra: ./scripts/secrets.sh rekey"
    fi
    ;;

  encrypt)
    f="${2:-}"; [ -z "$f" ] && usage
    sops_run --encrypt --in-place "$f"
    echo "şifrelendi: $f"
    ;;

  decrypt)
    f="${2:-}"; [ -z "$f" ] && usage
    sops_run --decrypt "$f"
    ;;

  edit)
    f="${2:-}"; [ -z "$f" ] && usage
    if ! command -v sops >/dev/null 2>&1; then
      echo "HATA: 'edit' etkileşimli bir editör açar, Docker'a düşerken çalışmaz." >&2
      echo "  sops'u yerel kur, ya da decrypt > düzenle > encrypt akışını kullan." >&2
      exit 1
    fi
    sops_run "$f"
    ;;

  rekey)
    # .sops.yaml'daki alıcı listesi değiştiğinde mevcut dosyaları yeni listeye
    # göre yeniden şifreler. Ekibe biri katıldığında/ayrıldığında gerekir.
    shopt -s nullglob
    for f in secrets/*.enc secrets/*.enc.*; do
      sops_run updatekeys --yes "$f"
      echo "yeniden anahtarlandı: $f"
    done
    ;;

  check)
    # CI koruması: secrets/ altında YANLIŞLIKLA düz metin commit edilmiş bir
    # dosya var mı. Şifreleme disiplininin tek bir unutkanlıkla delinmesini
    # engeller - asıl değeri olan kısım budur, şifrelemenin kendisi değil.
    shopt -s nullglob
    fail=0
    found=0
    for f in secrets/*.enc secrets/*.enc.*; do
      found=1
      if grep -q "sops" "$f" 2>/dev/null && grep -qE "ENC\[|encrypted_regex|lastmodified" "$f" 2>/dev/null; then
        echo "  OK        $f"
      else
        echo "  DÜZ METİN $f  <-- ŞİFRELENMEMİŞ"
        fail=1
      fi
    done

    if [ "$found" -eq 0 ]; then
      echo "secrets/ altında şifrelenmiş dosya yok (henüz kurulmamış olabilir)."
      exit 0
    fi
    if [ "$fail" -eq 1 ]; then
      echo ""
      echo "HATA: secrets/ altında şifrelenmemiş dosya var." >&2
      echo "  ./scripts/secrets.sh encrypt <dosya>" >&2
      exit 1
    fi
    echo ""
    echo "Tüm sır dosyaları şifreli."
    ;;

  *)
    usage
    ;;
esac

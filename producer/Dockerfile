# Base imajlar hem etiket hem digest ile sabitlenir.
#
# Neden digest: `golang:1.22-alpine` etiketi degisebilir bir isaretcidir - upstream
# ayni etikete yeni bir imaj push'ladiginda build girdisi sessizce degisir ve
# "ayni commit, farkli imaj" durumu olusur. Digest icerigin kendisini adresler,
# yani build tekrarlanabilir hale gelir ve CI'daki guvenlik taramasi tam olarak
# yayina gidecek katmanlari tarar.
#
# Bedeli: digest sabit oldugu icin base imaj guvenlik yamalari OTOMATIK gelmez.
# Bu yuzden .github/dependabot.yml digest'leri haftalik olarak PR ile gunceller -
# yama akisi kaybolmaz, sadece gorunur ve gozden gecirilebilir hale gelir.
FROM golang:1.27-alpine@sha256:4c9fe60190a2a3350ddc51de80d0224b8a6698d12bdfc999fee45ea9d6c46dbc AS build
WORKDIR /app
# Bagimliliklar once kopyalanir: kod degistiginde bu katman onbellekten gelir,
# modul indirme her build'de tekrarlanmaz. Ayrica go.sum sayesinde build
# tekrarlanabilir - `go mod tidy` gibi her seferinde agdan cozum yapmaz.
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /producer .

FROM alpine:3.24@sha256:28bd5fe8b56d1bd048e5babf5b10710ebe0bae67db86916198a6eec434943f8b

# Ayricaliksiz kullanici. Onceki surumde uc servis de root olarak calisiyordu:
# konteynerden kacis zinciri kurulabilen bir acikta root, host'a dogru bir adim
# daha yakin demek.
#
# Sabit UID (10001) bilerek: Kubernetes'te runAsNonRoot admission kontrolu
# imajin sayisal bir UID bildirmesini ister, isim cozumlemesine guvenmez.
RUN addgroup -g 10001 -S pulsecity \
 && adduser  -u 10001 -S pulsecity -G pulsecity

# Binary root'a ait ve salt-okunur: uygulama kendi calistirdigi dosyayi
# degistiremesin.
COPY --from=build --chown=root:root --chmod=0755 /producer /producer

USER 10001:10001

# 2112 > 1024 oldugu icin ayricaliksiz kullanici sorunsuz bind edebilir.
EXPOSE 2112
ENTRYPOINT ["/producer"]

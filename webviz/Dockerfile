# Base imajlar hem etiket hem digest ile sabitlenir - gerekce icin
# bkz. producer/Dockerfile. Digest guncellemesi .github/dependabot.yml ile
# haftalik PR olarak gelir.
FROM golang:1.27-alpine@sha256:4c9fe60190a2a3350ddc51de80d0224b8a6698d12bdfc999fee45ea9d6c46dbc AS build
WORKDIR /app
# Bagimliliklar once kopyalanir: kod degistiginde bu katman onbellekten gelir,
# modul indirme her build'de tekrarlanmaz.
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# static/index.html binary'ye go:embed ile gomulur, ayrica kopyalanmaz.
RUN CGO_ENABLED=0 go build -o /webviz .

FROM alpine:3.22@sha256:14358309a308569c32bdc37e2e0e9694be33a9d99e68afb0f5ff33cc1f695dce

# Ayricaliksiz kullanici, sabit UID (Kubernetes runAsNonRoot icin sayisal deger
# gerekir). Bkz. producer/Dockerfile.
RUN addgroup -g 10001 -S pulsecity \
 && adduser  -u 10001 -S pulsecity -G pulsecity

COPY --from=build --chown=root:root --chmod=0755 /webviz /webviz

USER 10001:10001

EXPOSE 8080
ENTRYPOINT ["/webviz"]

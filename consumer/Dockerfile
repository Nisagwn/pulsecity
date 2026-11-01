# Base imajlar hem etiket hem digest ile sabitlenir - gerekce icin
# bkz. producer/Dockerfile. Digest guncellemesi .github/dependabot.yml ile
# haftalik PR olarak gelir.
FROM golang:1.27-alpine@sha256:d200d2dd76b03a99b52651fd1325aff7dcd35422c35c01bac56571b0507f3deb AS build
WORKDIR /app
# Bagimliliklar once kopyalanir: kod degistiginde bu katman onbellekten gelir,
# modul indirme her build'de tekrarlanmaz. Ayrica go.sum sayesinde build
# tekrarlanabilir - `go mod tidy` gibi her seferinde agdan cozum yapmaz.
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /consumer .

FROM alpine:3.24@sha256:28bd5fe8b56d1bd048e5babf5b10710ebe0bae67db86916198a6eec434943f8b

# Ayricaliksiz kullanici, sabit UID (Kubernetes runAsNonRoot icin sayisal deger
# gerekir). Bkz. producer/Dockerfile.
# apk upgrade: digest sabitlemesi ile guvenlik yamasi arasindaki aralik icin.
# Tam gerekce: bkz. producer/Dockerfile.
RUN apk --no-cache upgrade \
 && addgroup -g 10001 -S pulsecity \
 && adduser  -u 10001 -S pulsecity -G pulsecity

COPY --from=build --chown=root:root --chmod=0755 /consumer /consumer

USER 10001:10001

EXPOSE 2113
ENTRYPOINT ["/consumer"]

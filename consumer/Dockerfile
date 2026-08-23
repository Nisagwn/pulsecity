# Base imajlar hem etiket hem digest ile sabitlenir - gerekce icin
# bkz. producer/Dockerfile. Digest guncellemesi .github/dependabot.yml ile
# haftalik PR olarak gelir.
FROM golang:1.26-alpine@sha256:28d89ee9cc0ff9fec75c82ca201e6bf7fdf9a679d4b7b24dfa04f2bb766bb468 AS build
WORKDIR /app
# Bagimliliklar once kopyalanir: kod degistiginde bu katman onbellekten gelir,
# modul indirme her build'de tekrarlanmaz. Ayrica go.sum sayesinde build
# tekrarlanabilir - `go mod tidy` gibi her seferinde agdan cozum yapmaz.
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /consumer .

FROM alpine:3.22@sha256:14358309a308569c32bdc37e2e0e9694be33a9d99e68afb0f5ff33cc1f695dce

# Ayricaliksiz kullanici, sabit UID (Kubernetes runAsNonRoot icin sayisal deger
# gerekir). Bkz. producer/Dockerfile.
RUN addgroup -g 10001 -S pulsecity \
 && adduser  -u 10001 -S pulsecity -G pulsecity

COPY --from=build --chown=root:root --chmod=0755 /consumer /consumer

USER 10001:10001

EXPOSE 2113
ENTRYPOINT ["/consumer"]

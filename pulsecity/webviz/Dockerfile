FROM golang:1.22-alpine AS build
WORKDIR /app
# Bagimliliklar once kopyalanir: kod degistiginde bu katman onbellekten gelir,
# modul indirme her build'de tekrarlanmaz.
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /webviz .

FROM alpine:3.19
COPY --from=build /webviz /webviz
EXPOSE 8080
ENTRYPOINT ["/webviz"]

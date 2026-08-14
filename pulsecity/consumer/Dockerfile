FROM golang:1.22-alpine AS build
WORKDIR /app
# Bagimliliklar once kopyalanir: kod degistiginde bu katman onbellekten gelir,
# modul indirme her build'de tekrarlanmaz. Ayrica go.sum sayesinde build
# tekrarlanabilir - `go mod tidy` gibi her seferinde agdan cozum yapmaz.
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /consumer .

FROM alpine:3.19
COPY --from=build /consumer /consumer
EXPOSE 2113
ENTRYPOINT ["/consumer"]

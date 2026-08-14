FROM golang:1.22-alpine AS build
WORKDIR /app
COPY go.mod main.go ./
RUN go mod tidy && CGO_ENABLED=0 go build -o /producer .

FROM alpine:3.19
COPY --from=build /producer /producer
EXPOSE 2112
ENTRYPOINT ["/producer"]

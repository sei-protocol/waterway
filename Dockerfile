FROM docker.io/golang:1.25 as BUILD

WORKDIR /go/src/sluice

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -o /go/bin/sluice .

FROM gcr.io/distroless/static-debian12
COPY --from=BUILD /go/bin/sluice /usr/bin/

ENTRYPOINT ["/usr/bin/sluice"]
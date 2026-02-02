FROM docker.io/golang:1.25 as BUILD

WORKDIR /go/src/waterway

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -o /go/bin/waterway .

FROM gcr.io/distroless/static-debian12
COPY --from=BUILD /go/bin/waterway /usr/bin/

ENTRYPOINT ["/usr/bin/waterway"]
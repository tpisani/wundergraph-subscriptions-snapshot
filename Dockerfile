FROM --platform=${BUILDPLATFORM} golang:1.26 AS builder

ARG TARGETOS
ARG TARGETARCH

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go test ./... \
 && CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags "-s -w -extldflags -static" -o /router ./cmd/router

FROM gcr.io/distroless/static:nonroot

COPY --from=builder /router /router

USER nonroot:nonroot
EXPOSE 3002

ENTRYPOINT ["/router"]

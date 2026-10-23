# Builds the Cosmo Router with the subscription snapshot module compiled in.
# Based on https://github.com/wundergraph/router-examples.
FROM --platform=${BUILDPLATFORM} golang:1.26 AS builder

ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev
ARG COMMIT
ARG DATE

WORKDIR /app

# Copy only the files required for go mod download
COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags "-s -w -extldflags=-static \
    -X 'github.com/wundergraph/cosmo/router/core.Version=${VERSION}' \
    -X 'github.com/wundergraph/cosmo/router/core.Commit=${COMMIT}' \
    -X 'github.com/wundergraph/cosmo/router/core.Date=${DATE}'" \
    -o /router ./cmd/router

FROM gcr.io/distroless/static:nonroot

COPY --from=builder /router /router

USER nonroot:nonroot
EXPOSE 3002

ENTRYPOINT ["/router"]

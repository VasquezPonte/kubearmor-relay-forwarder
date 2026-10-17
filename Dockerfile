ARG GO_BUILDER_IMAGE=golang:1.27.2-alpine
ARG RUNTIME_IMAGE=gcr.io/distroless/static-debian13:nonroot

# ============================================================
# Builder stage
# ============================================================
FROM ${GO_BUILDER_IMAGE} AS build
WORKDIR /src
RUN apk add --no-cache ca-certificates git
COPY go.mod go.sum ./
RUN go mod download && go mod verify
COPY . .
RUN CGO_ENABLED=0 GOOS=linux \
    go build \
        -mod=readonly \
        -trimpath \
        -buildvcs=false \
        -ldflags="-s -w" \
        -o /out/forwarder .

# ============================================================
# Runtime stage
# ============================================================
FROM ${RUNTIME_IMAGE}
COPY --from=build --chown=65532:65532 \
    /out/forwarder /forwarder
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/forwarder"]

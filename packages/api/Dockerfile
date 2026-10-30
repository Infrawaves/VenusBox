ARG GOLANG_VERSION=1.26.10
ARG ALPINE_VERSION=3.24

FROM --platform=$BUILDPLATFORM golang:${GOLANG_VERSION}-alpine${ALPINE_VERSION} AS builder
# Fall through to a retry on any module proxy error; see
# .github/actions/go-setup-cache/action.yml for the mechanism.
ENV GOPROXY=https://proxy.golang.org|https://proxy.golang.org:443|https://proxy.golang.org/|direct
ARG TARGETARCH

ENV GOSUMDB=sum.golang.org
RUN sed -i 's/dl-cdn.alpinelinux.org/mirrors.aliyun.com/g' /etc/apk/repositories

RUN apk add --no-cache make

# Copy all module dependency files at once for better Docker layer caching
WORKDIR /build
COPY ./shared/go.mod ./shared/go.sum ./shared/
COPY ./clickhouse/go.mod ./clickhouse/go.sum ./clickhouse/
COPY ./db/go.mod ./db/go.sum ./db/
COPY ./auth/go.mod ./auth/go.sum ./auth/
COPY ./api/go.mod ./api/go.sum ./api/

# Download all module dependencies
RUN --mount=type=cache,target=/go/pkg/mod cd /build/shared && go mod download && \
    cd /build/clickhouse && go mod download && \
    cd /build/db && go mod download && \
    cd /build/auth && go mod download && \
    cd /build/api && go mod download


# Cached golang dependencies
WORKDIR /build/shared
COPY ./shared/go.mod ./shared/go.sum ./
RUN go mod download

WORKDIR /build/clickhouse
COPY ./clickhouse/go.mod ./clickhouse/go.sum ./
RUN go mod download

WORKDIR /build/db
COPY ./db/go.mod ./db/go.sum ./
RUN go mod download

WORKDIR /build/auth
COPY ./auth/go.mod ./auth/go.sum ./
RUN go mod download

WORKDIR /build/api
COPY ./api/go.mod ./api/go.sum ./
RUN go mod download

# Copy source code
WORKDIR /build

COPY . .

WORKDIR /build/api

ARG COMMIT_SHA
ARG VERSION
ARG EXPECTED_MIGRATION_TIMESTAMP
RUN --mount=type=cache,target=/root/.cache/go-build --mount=type=cache,target=/go/pkg/mod GOARCH=${TARGETARCH} make build COMMIT_SHA=${COMMIT_SHA} VERSION=${VERSION} EXPECTED_MIGRATION_TIMESTAMP=${EXPECTED_MIGRATION_TIMESTAMP}

RUN chmod +x /build/api/bin/api

FROM alpine:${ALPINE_VERSION}
# Bump this date to force a rebuild from here down, so the upgrade below
# resolves against the current Alpine package index. Nothing reads the value.
ENV LAST_FORCED_UPDATE=2026-10-20

RUN apk upgrade --no-cache

COPY --from=builder /build/api/bin/api .

# Set Gin server to the production mode
ENV GIN_MODE=release

ENTRYPOINT [ "./api"]

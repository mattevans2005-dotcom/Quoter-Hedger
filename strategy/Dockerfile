# Your strategy goes here. Replace this file (and add your source) so that the
# container builds and runs your market maker.
#
# Requirements:
#   - The container must connect to the exchange's NATS at $NATS_URL
#     (inside the compose network that is nats://nats:4222).
#   - It must run your strategy as its CMD/ENTRYPOINT (no host dependencies).
#
# Example skeletons (delete what you don't use):
#
#   # Rust
#   FROM rust:1 AS build
#   WORKDIR /app
#   COPY . .
#   RUN cargo build --release
#   FROM debian:stable-slim
#   COPY --from=build /app/target/release/<your-bin> /usr/local/bin/strategy
#   ENV NATS_URL=nats://nats:4222
#   CMD ["strategy"]
#
#   # Go
#   FROM golang:1 AS build
#   WORKDIR /app
#   COPY . .
#   RUN go build -o /strategy .
#   FROM gcr.io/distroless/base-debian12
#   COPY --from=build /strategy /strategy
#   ENV NATS_URL=nats://nats:4222
#   CMD ["/strategy"]
#
# Placeholder so `docker compose --profile strategy up` works before you start:
# -----------------------------
# Build stage
# -----------------------------

FROM debian:bookworm AS build

RUN apt-get update && \
    apt-get install -y \
        build-essential \
        cmake \
        git \
        ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Clone NATS C client
RUN git clone https://github.com/nats-io/nats.c.git /tmp/nats.c

# Build NATS C client
RUN cmake -S /tmp/nats.c -B /tmp/nats-build \
        -DNATS_BUILD_STREAMING=OFF \
        -DNATS_BUILD_WITH_TLS=OFF \
        -DNATS_BUILD_EXAMPLES=OFF \
        -DNATS_BUILD_TESTS=OFF

RUN cmake --build /tmp/nats-build
RUN cmake --install /tmp/nats-build

# Copy our strategy
COPY . /app

# Build our strategy
RUN cmake -S /app -B /app/build
RUN cmake --build /app/build


# -----------------------------
# Runtime stage
# -----------------------------

FROM debian:bookworm-slim

COPY --from=build /app/build/strategy /usr/local/bin/strategy
COPY --from=build /usr/local/lib/libnats* /usr/local/lib/
COPY --from=build /usr/local/include/nats /usr/local/include/nats

RUN ldconfig

ENV NATS_URL=nats://nats:4222

CMD ["/usr/local/bin/strategy"]
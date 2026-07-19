# --- Build Stage ---
FROM rust:1.81-slim AS builder

WORKDIR /usr/src/zydecodb

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy the entire workspace
COPY . .

# Build the release binary
RUN cargo build --release -p zydecodb --bin zydecodb

# --- Runtime Stage ---
FROM debian:bookworm-slim

WORKDIR /usr/local/bin

# Install runtime dependencies (like CA certificates, openssl)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

# Copy the compiled binary from the builder stage
COPY --from=builder /usr/src/zydecodb/target/release/zydecodb /usr/local/bin/zydecodb

# Create directories for data, wal, and configuration
RUN mkdir -p /var/lib/zydecodb/data /var/lib/zydecodb/wal /etc/zydecodb

# Expose database port and metrics port
EXPOSE 9470 9471

# Set the entrypoint to run the server
ENTRYPOINT ["zydecodb"]
CMD ["serve", "--config", "/etc/zydecodb/zydecodb.docker.toml"]

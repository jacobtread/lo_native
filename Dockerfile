#  Builder part
FROM rust:1.80.0-slim-bookworm AS builder

WORKDIR /app

# Dependency precachng
COPY Cargo.toml .
COPY Cargo.lock .
COPY client/Cargo.toml ./client/Cargo.toml
RUN mkdir src && echo "fn main() {}" >src/main.rs
RUN mkdir client/src && echo "fn main() {}" >client/src/main.rs
RUN cargo build --target x86_64-unknown-linux-gnu --release

COPY src src
COPY client/src client/src
RUN touch src/main.rs

RUN cargo build --target x86_64-unknown-linux-gnu --release

# ----------------------------------------
# Runner part
# ----------------------------------------
FROM debian:bookworm-slim AS runner

# Set environment variables to avoid interaction during installation
ENV DEBIAN_FRONTEND=noninteractive

RUN echo 'deb http://deb.debian.org/debian/ bookworm main' > /etc/apt/sources.list.d/debian.list

# Bookworm backports for latest available LibreOffice version
RUN echo 'deb http://deb.debian.org/debian bookworm-backports main' > /etc/apt/sources.list.d/bookworm-backports.list


WORKDIR /app

# Install dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl ca-certificates
    
# Install stable libreoffice from backports
RUN apt-get install -t bookworm-backports -y libreoffice

# Cleanup
RUN apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Copy the built binary
COPY --from=builder /app/target/x86_64-unknown-linux-gnu/release/office-convert-server ./

ENV LIBREOFFICE_SDK_PATH=/usr/lib/libreoffice/program
ENV SERVER_ADDRESS=0.0.0.0:3000 

EXPOSE 8080

CMD ["/app/office-convert-server"]

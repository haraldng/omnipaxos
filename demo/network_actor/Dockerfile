FROM rust:1.67 as builder
WORKDIR /usr/src/network_actor

# cache dependencies
RUN cargo init --bin
COPY Cargo.toml ./
# COPY Cargo.lock ./
# RUN --mount=type=cache,target=/usr/local/cargo/registry cargo build --release
RUN cargo build --release
RUN rm ./src/*.rs ./target/release/deps/network_actor*

# build
COPY . .
RUN cargo install --path .

FROM debian:bullseye-slim
COPY --from=builder /usr/local/cargo/bin/network_actor /usr/local/bin/network_actor
EXPOSE 8000-8099
CMD ["network_actor"]

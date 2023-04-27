FROM rust:1.67 as builder
WORKDIR /usr/src/kv_demo

# cache dependencies
RUN cargo init --bin
COPY Cargo.toml ./
# COPY Cargo.lock ./
# RUN --mount=type=cache,target=/usr/local/cargo/registry cargo build --release
RUN cargo build --release
RUN rm ./src/*.rs ./target/release/deps/kv_demo*

# build
COPY . .
RUN cargo install --path .

FROM debian:bullseye-slim
COPY --from=builder /usr/local/cargo/bin/kv_demo /usr/local/bin/kv_demo
CMD ["kv_demo"]

FROM golang:1.17 AS builder

WORKDIR /lmstfy

ADD ./ /lmstfy
RUN apt update -y && apt install -y netcat
RUN cd /lmstfy && make

FROM ubuntu:20.04
COPY --from=builder /lmstfy /lmstfy
EXPOSE 7777:7777
ENTRYPOINT ["/lmstfy/_build/lmstfy-server", "-c", "/lmstfy/config/docker-image-conf.toml"]


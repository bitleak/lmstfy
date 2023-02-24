FROM golang:1.17

WORKDIR /lmstfy

ADD ./ /lmstfy
RUN apt update -y && apt install -y netcat
RUN cd /lmstfy && make
EXPOSE 7777:7777
ENTRYPOINT ["/lmstfy/_build/lmstfy-server", "-c", "/lmstfy/config/docker-image-conf.toml"]


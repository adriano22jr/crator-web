FROM debian:latest

RUN apt-get update && \
    apt-get install -y tor && \
    rm -rf /var/lib/apt/lists/*

COPY torrc /etc/tor/torrc
EXPOSE 9050

CMD ["tor", "-f", "/etc/tor/torrc"]
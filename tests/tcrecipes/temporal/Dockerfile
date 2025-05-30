FROM alpine:3.17.0

ARG TARGETARCH=amd64
ARG TEMPORALITE_VERSION=0.3.0

RUN wget -q https://github.com/temporalio/temporalite/releases/download/v${TEMPORALITE_VERSION}/temporalite_${TEMPORALITE_VERSION}_linux_${TARGETARCH}.tar.gz -O temporalite.tar.gz \
 && tar -xf temporalite.tar.gz temporalite -C /usr/local/bin/ \
 && rm -rf temporalite.tar.gz

EXPOSE 7233 8233

ENTRYPOINT ["temporalite", "start", "--ip=0.0.0.0"]

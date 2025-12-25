FROM ubuntu:jammy

ENV TZ=Etc/UTC

ENV ROTATION_TZ=Etc/UTC

ENV DEBIAN_FRONTEND=noninteractive

RUN echo $TZ > /etc/timezone && \
  ln -snf /usr/share/zoneinfo/$TZ /etc/localtime

# Install required packages
RUN DEBIAN_FRONTEND=noninteractive apt-get update -qq && \
  apt-get install -y gnupg wget lsb-release curl ca-certificates openssh-client \
  iptables supervisor apt-transport-https dirmngr nano vim telnet less tcpdump net-tools \
  tzdata lsof libaio1 unzip git && \
  # Add PostgreSQL official repository
  curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc | gpg --dearmor -o /usr/share/keyrings/postgresql.gpg && \
  echo "deb [signed-by=/usr/share/keyrings/postgresql.gpg] http://apt.postgresql.org/pub/repos/apt jammy-pgdg main" > /etc/apt/sources.list.d/pgdg.list && \
  apt-get update -qq && \
  apt-get install -y postgresql-client-16 && \
  rm -rf /var/lib/apt/lists/*

# Pre-configure debconf to avoid ClickHouse prompts (password or confirmation)
RUN echo "clickhouse-client clickhouse-server/root_password password root" | debconf-set-selections && \
  echo "clickhouse-client clickhouse-server/root_password_again password root" | debconf-set-selections

# Install ClickHouse
RUN DEBIAN_FRONTEND=noninteractive apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 8919F6BD2B48D754 && \
  echo "deb https://packages.clickhouse.com/deb stable main" >> /etc/apt/sources.list && \
  apt-get update -qq && \
  apt-get install -y clickhouse-server=23.5.3.24 clickhouse-client=23.5.3.24 clickhouse-common-static=23.5.3.24 && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/*

ENV DOCKER_CHANNEL=stable \
  DOCKER_VERSION=23.0.6 \
  DOCKER_COMPOSE_VERSION=1.29.2 \
  DEBUG=false

# Docker installation
RUN set -eux; \
  arch="$(uname -m)"; \
  case "$arch" in \
  x86_64) dockerArch='x86_64';; \
  armhf) dockerArch='armel';; \
  armv7) dockerArch='armhf';; \
  aarch64) dockerArch='aarch64';; \
  *) echo >&2 "error: unsupported architecture ($arch)"; exit 1;; \
  esac; \
  curl -fsSL "https://download.docker.com/linux/static/${DOCKER_CHANNEL}/${dockerArch}/docker-${DOCKER_VERSION}.tgz" -o docker.tgz; \
  tar --extract --file docker.tgz --strip-components 1 --directory /usr/local/bin/; \
  rm docker.tgz; \
  dockerd --version; \
  docker --version

VOLUME /var/lib/docker

# Create a non-root user and group
RUN addgroup --system trcligroup && adduser --system --ingroup trcligroup trcliuser

# Copy the Go binary
COPY trcli /usr/local/bin/trcli

RUN chmod +x /usr/local/bin/trcli

# Switch to the non-root user
USER trcliuser

# Supervisor configuration to start Docker daemon and application
USER root
RUN apt-get install -y supervisor

RUN echo "[supervisord]\n\
  nodaemon=true\n\
  \n\
  [program:dockerd]\n\
  command=/usr/local/bin/dockerd --host=unix:///var/run/docker.sock\n\
  autostart=true\n\
  autorestart=true\n\
  stdout_logfile=/var/log/dockerd.out.log\n\
  stderr_logfile=/var/log/dockerd.err.log\n" \
  > /etc/supervisor/supervisord.conf

ENV SUPERVISORD_PATH=/etc/supervisor/supervisord.conf

ENTRYPOINT ["/usr/local/bin/trcli"]

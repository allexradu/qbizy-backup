FROM debian:bullseye

RUN apt-get update && \
    apt-get install -y curl unzip gnupg && \
    echo "deb http://apt.postgresql.org/pub/repos/apt bullseye-pgdg main" > /etc/apt/sources.list.d/pgdg.list && \
    curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc | gpg --dearmor > /etc/apt/trusted.gpg.d/postgresql.gpg && \
    apt-get update && \
    apt-get install -y postgresql-client-17 && \
    curl -fsSL https://bun.sh/install | bash

ENV PATH="/root/.bun/bin:${PATH}"
WORKDIR /app
COPY . .
RUN bun install
RUN mkdir -p /tmp/backup
CMD ["bun", "restore.ts"]

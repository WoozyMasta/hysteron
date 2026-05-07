# Copyright 2026 WoozyMasta
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied
# See the License for the specific language governing permissions and
# limitations under the License.

ARG GO_VERSION=1.26.3
FROM docker.io/library/golang:$GO_VERSION-bookworm

ARG PG_MAJOR=18
ARG ETCD_VERSION=3.6.11

# hadolint ignore=DL3008,DL4006
RUN set -eux; \
	apt-get update; \
	apt-get install -y --no-install-recommends \
		ca-certificates curl gnupg make tar; \
	install -d -m 0755 /etc/apt/keyrings; \
	curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc \
		| gpg --dearmor -o /etc/apt/keyrings/postgresql.gpg; \
	echo "deb [signed-by=/etc/apt/keyrings/postgresql.gpg] http://apt.postgresql.org/pub/repos/apt bookworm-pgdg main" \
		> /etc/apt/sources.list.d/pgdg.list; \
	apt-get update; \
	apt-get install -y --no-install-recommends "postgresql-$PG_MAJOR"; \
	rm -rf /var/lib/apt/lists/*

# hadolint ignore=DL4006
RUN set -eux; \
	arch="$(dpkg --print-architecture)"; \
	case "$arch" in \
		amd64) etcd_arch="amd64" ;; \
		arm64) etcd_arch="arm64" ;; \
		*) echo "unsupported architecture for etcd binary: $arch" >&2; exit 1 ;; \
	esac; \
	url="https://github.com/etcd-io/etcd/releases/download/v$ETCD_VERSION/etcd-v$ETCD_VERSION-linux-$etcd_arch.tar.gz"; \
	tmp_dir="$(mktemp -d)"; \
	curl -fsSL "$url" -o "$tmp_dir/etcd.tar.gz"; \
	tar -C "$tmp_dir" -xzf "$tmp_dir/etcd.tar.gz"; \
	install -m 0755 "$tmp_dir/etcd-v$ETCD_VERSION-linux-$etcd_arch/etcd" /usr/local/bin/etcd; \
	rm -rf "$tmp_dir"

ENV PATH="/usr/lib/postgresql/$PG_MAJOR/bin:$PATH"
ENV INTEGRATION_STORE_BACKEND=etcdv3

WORKDIR /workspace

ENTRYPOINT ["make"]
CMD ["integration"]

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

ARG GO_VERSION=1.26
FROM docker.io/library/golang:$GO_VERSION

ARG PG_MAJOR=18

# hadolint ignore=DL3008,DL4006
RUN set -eux; \
	apt-get update; \
	apt-get install -y --no-install-recommends \
		ca-certificates curl etcd-server gnupg make; \
	install -d -m 0755 /etc/apt/keyrings; \
	curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc \
		| gpg --dearmor -o /etc/apt/keyrings/postgresql.gpg; \
	echo "deb [signed-by=/etc/apt/keyrings/postgresql.gpg] http://apt.postgresql.org/pub/repos/apt bookworm-pgdg main" \
		> /etc/apt/sources.list.d/pgdg.list; \
	apt-get update; \
	apt-get install -y --no-install-recommends "postgresql-$PG_MAJOR"; \
	rm -rf /var/lib/apt/lists/*

ENV PATH="/usr/lib/postgresql/$PG_MAJOR/bin:$PATH"
ENV INTEGRATION_STORE_BACKEND=etcdv3

WORKDIR /workspace

ENTRYPOINT ["make"]
CMD ["integration"]

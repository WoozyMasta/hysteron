ARG POSTGRES_VERSION=17
ARG HYSTERON_IMAGE=docker.io/hysteron/hysteron
ARG HYSTERON_TAG=latest

FROM $HYSTERON_IMAGE:$HYSTERON_TAG AS hysteron

FROM docker.io/library/postgres:$POSTGRES_VERSION

COPY --from=hysteron /bin/hysteron /usr/local/bin/hysteron
USER postgres
ENTRYPOINT ["/usr/local/bin/hysteron"]
CMD ["keeper"]

ARG POSTGRES_VERSION=17
ARG HYSTERON_IMAGE=docker.io/hysteron/hysteron

FROM $HYSTERON_IMAGE AS hysteron

FROM docker.io/library/postgres:$POSTGRES_VERSION

COPY --from=hysteron /bin/hysteron /usr/local/bin/hysteron
USER postgres
ENTRYPOINT ["/usr/local/bin/hysteron"]
CMD ["keeper"]

FROM docker.io/library/golang:1.26-trixie AS builder

WORKDIR /src
COPY go.mod go.sum Makefile ./
RUN make download

COPY .git ./.git
COPY cmd ./cmd
COPY internal ./internal
RUN make tool-mockgen build

FROM scratch

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /src/build/hysteron /bin/hysteron

ENTRYPOINT ["/bin/hysteron"]
CMD ["version"]

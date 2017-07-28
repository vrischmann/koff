FROM golang:1.9-alpine

COPY . /go/src/github.com/vrischmann/koff

RUN go install -v github.com/vrischmann/koff/cmd/koff

FROM alpine

COPY --from=0 /go/bin/koff /usr/bin/koff

ENTRYPOINT ["/usr/bin/koff"]

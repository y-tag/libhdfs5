FROM golang:1.18

RUN go install golang.org/x/tools/cmd/goimports@latest
RUN curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(go env GOPATH)/bin v1.45.2

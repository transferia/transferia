This package runs both unit and integration tests. Integration tests against Kubernetes (using kind) are executed only if the environment variable TEST_KUBERNETES_INTEGRATION is set.

## Prerequisites

- Go (v1.18+)
- Docker
- kind (for Kubernetes integration tests)

## Installing kind

To install kind, run:

go install sigs.k8s.io/kind@v0.26.0

Make sure your GOPATH/bin is in your PATH.

More information on kind can be found [here](https://kind.sigs.k8s.io/).

## Running Tests

### Unit Tests

By default, integration tests are skipped. Run all tests with:

go test -v ./...

### Integration Tests

To run integration tests:

1. Set the environment variable:
     export TEST_KUBERNETES_INTEGRATION=1
2. Run the tests:
     go test -v ./...

The integration tests will create a temporary kind cluster, perform the tests, and then clean up the cluster.

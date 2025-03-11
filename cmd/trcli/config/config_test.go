package config

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/pkg/providers/mongo"
	_ "github.com/transferria/transferria/pkg/transformer/registry"
)

func TestParseTransferYaml_WithEnvSubstitution(t *testing.T) {
	t.Setenv("FOO", "secret1")
	t.Setenv("BAR", "secret2")

	transfer, err := ParseTransferYaml([]byte(`
src:
  type: src_type
  params: |
    {"Password": "${FOO}"}
dst:
  type: dst_type
  params: |
    {"Password": "${BAR}"}
`))
	require.NoError(t, err)

	assert.Equal(t, "{\"Password\":\"secret1\"}", transfer.Src.Params)
	assert.Equal(t, "{\"Password\":\"secret2\"}", transfer.Dst.Params)
}

func TestParserTransferYaml_WithRawYaml(t *testing.T) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}

	privateKeyPEM := &pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(privateKey),
	}

	t.Setenv("FOO", "secret1")
	t.Setenv("BAR", "secret2")
	t.Setenv("PRIVATE_KEY", string(pem.EncodeToMemory(privateKeyPEM)))

	transfer, err := ParseTransferYaml([]byte(`
src:
  type: src_type
  params: |
    Password: ${FOO}
dst:
  type: dst_type
  params: |
    Password: ${BAR}
    tlsfile: ${PRIVATE_KEY}
`))
	require.NoError(t, err)

	assert.Equal(t, "{\"Password\":\"secret1\"}", transfer.Src.Params)

	expectTLSParams := strings.ReplaceAll(string(pem.EncodeToMemory(privateKeyPEM)), "\n", "\\n")
	assert.Equal(t, fmt.Sprintf("{\"Password\":\"secret2\",\"tlsfile\":\"%s\"}", expectTLSParams), transfer.Dst.Params)
}

func TestParserTransferYaml_WithYaml(t *testing.T) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}

	privateKeyPEM := &pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(privateKey),
	}

	t.Setenv("FOO", "secret1")
	t.Setenv("BAR", "secret2")
	t.Setenv("PRIVATE_KEY", string(pem.EncodeToMemory(privateKeyPEM)))

	transfer, err := ParseTransferYaml([]byte(`
src:
  type: src_type
  params:
    Password: ${FOO}
dst:
  type: dst_type
  params:
    Password: ${BAR}
    tlsfile: ${PRIVATE_KEY}
`))
	require.NoError(t, err)

	assert.Equal(t, "{\"Password\":\"secret1\"}", transfer.Src.Params)

	expectTLSParams := strings.ReplaceAll(string(pem.EncodeToMemory(privateKeyPEM)), "\n", "\\n")
	assert.Equal(t, fmt.Sprintf("{\"Password\":\"secret2\",\"tlsfile\":\"%s\"}", expectTLSParams), transfer.Dst.Params)
}

func TestYamlDuration(t *testing.T) {
	transfer, err := ParseTransferYaml([]byte(`
src:
  type: mongo
  params:
    BatchingParams:
      BatchFlushInterval: 10s
dst:
  type: stdout
  params:
    ShowData: false
`))
	require.NoError(t, err)
	src, err := source(transfer)
	require.NoError(t, err)
	msrc, ok := src.(*mongo.MongoSource)
	require.True(t, ok)
	require.Equal(t, msrc.BatchingParams.BatchFlushInterval, 10*time.Second)
}

func TestTransformer(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		transfer, err := ParseTransferYaml([]byte(`
src:
  type: mongo
  params:
    BatchingParams:
      BatchFlushInterval: 10s
dst:
  type: stdout
  params:
    ShowData: false
regular_snapshot:
  enabled: false
  interval: 0s
  cron_expression: ""
  increment_delay_seconds: 0
  incremental: []
transformation:
  debugmode: false
  transformers:
  - renameTables:
      renameTables:
      - newName:
          name: a
          nameSpace: ""
        originalName:
          name: a
          nameSpace: a_namespace
    transformerId: ""
  errorsoutput: null
data_objects:
  include_objects:
  - sgd6096.order
type_system_version: 9
`))
		require.NoError(t, err)
		require.Len(t, transfer.Transformation.Transformers, 1)
		require.NoError(t, transfer.Validate())
	})
	t.Run("invalid", func(t *testing.T) {
		transfer, err := ParseTransferYaml([]byte(`
src:
  type: mango
  params:
    BatchingParams:
      BatchFlushInterval: 10s
dst:
  type: stdout
  params:
    ShowData: false
transformation:
  transformers:
  - boboTables:
      renameTables:
      - newName:
          name: a
          nameSpace: ""
        originalName:
          name: a
          nameSpace: a_namespace
    transformerId: ""
data_objects:
  include_objects:
  - sgd6096.order
type_system_version: 9
`))
		require.NoError(t, err)
		require.Error(t, transfer.Validate())
	})
}

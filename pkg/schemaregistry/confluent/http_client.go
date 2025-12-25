package confluent

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
)

type subjectAndVersions struct {
	subject string
	version int32
}

type SchemaRegistryClient struct {
	logger log.Logger

	schemaRegistryURL string
	credentials       *credentials
	httpClient        httpDoClient
	cachingEnabled    bool

	subjectAndVersionLock sync.RWMutex
	subjectAndVersions    map[subjectAndVersions]*Schema

	pushedSchemaIDCacheLock sync.RWMutex
	pushedSchemaIDCache     map[string]int

	idSchemaCacheLock sync.RWMutex
	idSchemaCache     map[int]*Schema
}

type credentials struct {
	// Username and Password for Schema Basic Auth
	username string
	password string

	// Bearer Authorization token
	bearerToken string
}

type schemaRequest struct {
	Schema     string `json:"schema"`
	SchemaType string `json:"schemaType"`
}

type schemaReference struct {
	Name        string `json:"name"`
	SubjectName string `json:"subject"`
	Version     int32  `json:"version"`
}

type schemaResponse struct {
	Version    int               `json:"version"`
	Schema     string            `json:"schema"`
	SchemaType SchemaType        `json:"schemaType"`
	ID         int               `json:"id"`
	References []schemaReference `json:"references"`
}

func schemaResponseReferencesToSchemaReference(in []schemaReference) []SchemaReference {
	result := make([]SchemaReference, 0, len(in))
	for _, el := range in {
		result = append(result, SchemaReference(el))
	}
	return result
}

const (
	subjects        = "/subjects"
	schemaByID      = "/schemas/ids/%d"
	subjectVersions = "/subjects/%s/versions"
	subjectVersion  = "/subjects/%s/versions/%d"

	contentType = "application/vnd.schemaregistry.v1+json"
)

func NewSchemaRegistryClientWithTransport(schemaRegistryRawURLs string, caCert string, logger log.Logger) (*SchemaRegistryClient, error) {
	var useSsl bool
	var urls []*url.URL

	rawURLs := strings.Split(schemaRegistryRawURLs, ",")
	for i := range rawURLs {
		parsedURL, err := url.Parse(strings.TrimSpace(rawURLs[i]))
		if err != nil {
			return nil, xerrors.Errorf("can't parse raw URL %q: %w", rawURLs[i], err)
		}
		useSsl = useSsl || parsedURL.Scheme == "https"
		urls = append(urls, parsedURL)
	}

	httpClient := &http.Client{Timeout: 25 * time.Second}
	if useSsl {
		tlsConfig := &tls.Config{}
		if caCert != "" {
			rootCertPool := x509.NewCertPool()
			if ok := rootCertPool.AppendCertsFromPEM([]byte(caCert)); !ok {
				return nil, xerrors.New("Unable to add TLS to cert pool")
			}
			tlsConfig.RootCAs = rootCertPool
		} else {
			tlsConfig.InsecureSkipVerify = true
		}
		httpClient.Transport = &http.Transport{TLSClientConfig: tlsConfig}
	}

	if len(rawURLs) == 1 {
		return newSchemaRegistryClient(rawURLs[0], httpClient, logger), nil
	} else {
		return newSchemaRegistryClient(rawURLs[0], newBalancedClient(newRoundRobinLoadBalancer(urls), httpClient), logger), nil
	}
}

func newSchemaRegistryClient(schemaRegistryURL string, client httpDoClient, logger log.Logger) *SchemaRegistryClient {
	return &SchemaRegistryClient{
		logger: logger,

		schemaRegistryURL: schemaRegistryURL,
		credentials:       nil,
		httpClient:        client,

		cachingEnabled: true,

		subjectAndVersionLock: sync.RWMutex{},
		subjectAndVersions:    make(map[subjectAndVersions]*Schema),

		pushedSchemaIDCacheLock: sync.RWMutex{},
		pushedSchemaIDCache:     make(map[string]int),

		idSchemaCacheLock: sync.RWMutex{},
		idSchemaCache:     make(map[int]*Schema),
	}
}

func (c *SchemaRegistryClient) ResetCache() {
	c.idSchemaCacheLock.Lock()
	defer c.idSchemaCacheLock.Unlock()
	c.idSchemaCache = make(map[int]*Schema)
}

func (c *SchemaRegistryClient) IsAuthorized() (bool, error) {
	_, err := c.httpRequest(http.MethodGet, subjects, nil)
	if err != nil {
		if strings.Contains(err.Error(), "Unauthorized") || strings.Contains(err.Error(), "Permission denied") || strings.Contains(err.Error(), "token is invalid") {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (c *SchemaRegistryClient) GetSchema(schemaID int) (*Schema, error) {
	if c.getCachingEnabled() {
		c.idSchemaCacheLock.RLock()
		cachedSchema := c.idSchemaCache[schemaID]
		c.idSchemaCacheLock.RUnlock()
		if cachedSchema != nil {
			return cachedSchema, nil
		}
	}

	resp, err := c.httpRequest(http.MethodGet, fmt.Sprintf(schemaByID, schemaID), nil)
	if err != nil {
		return nil, xerrors.Errorf("Can't get schema by id: %w", err)
	}

	var schemaResp = new(schemaResponse)
	err = json.Unmarshal(resp, schemaResp)
	if err != nil {
		return nil, xerrors.Errorf("Can't unmarshal schema: %w", err)
	}

	var schema = &Schema{
		ID:         schemaID,
		Schema:     schemaResp.Schema,
		SchemaType: schemaResp.SchemaType,
		References: schemaResponseReferencesToSchemaReference(schemaResp.References),
	}

	if c.getCachingEnabled() {
		c.idSchemaCacheLock.Lock()
		c.idSchemaCache[schemaID] = schema
		c.idSchemaCacheLock.Unlock()
	}

	return schema, nil
}

func (c *SchemaRegistryClient) ResolveReferencesRecursive(references []SchemaReference) (map[string]Schema, error) {
	referencedSchemas := newSchemasContainer(references)

	for {
		referenceName, subject, version := referencedSchemas.getTask()
		if subject == "" {
			break
		}
		schema, err := c.GetSchemaBySubjectVersion(subject, version)
		if err != nil {
			return nil, xerrors.Errorf("unable to GetSchemaBySubjectVersion - err: %w", err)
		}
		for _, ref := range schema.References {
			referencedSchemas.addTask(referenceName, ref.SubjectName, ref.Version)
		}
		err = referencedSchemas.doneTask(referenceName, subject, version, *schema)
		if err != nil {
			return nil, xerrors.Errorf("unable to doneTask - err: %w", err)
		}
	}

	return referencedSchemas.references(), nil
}

func (c *SchemaRegistryClient) GetSchemaBySubjectVersion(subject string, version int32) (*Schema, error) {
	key := subjectAndVersions{subject: subject, version: version}

	if c.getCachingEnabled() {
		c.subjectAndVersionLock.RLock()
		schemaObj, ok := c.subjectAndVersions[key]
		c.subjectAndVersionLock.RUnlock()
		if ok {
			return schemaObj, nil
		}
	}

	resp, err := c.httpRequest(http.MethodGet, fmt.Sprintf(subjectVersion, url.QueryEscape(subject), version), nil)
	if err != nil {
		return nil, xerrors.Errorf("Create schema request error: %w", err)
	}

	schemaResp := new(schemaResponse)
	if err = json.Unmarshal(resp, schemaResp); err != nil {
		return nil, xerrors.Errorf("Can't unmarshal a response with created schemaID: %w", err)
	}

	var schema = &Schema{
		ID:         -1,
		Schema:     schemaResp.Schema,
		SchemaType: schemaResp.SchemaType,
		References: schemaResponseReferencesToSchemaReference(schemaResp.References),
	}

	if c.getCachingEnabled() {
		c.subjectAndVersionLock.Lock()
		c.subjectAndVersions[key] = schema
		c.subjectAndVersionLock.Unlock()
	}
	return schema, nil
}

func (c *SchemaRegistryClient) CreateSchema(subject string, schema string, schemaType SchemaType) (int, error) {
	switch schemaType {
	case AVRO, JSON:
		compiledRegex := regexp.MustCompile(`\r?\n`)
		schema = compiledRegex.ReplaceAllString(schema, " ")
	case PROTOBUF:
		break
	default:
		return 0, xerrors.Errorf("Invalid schema type %q. Valid values are Avro, Json, or Protobuf", schemaType)
	}

	if c.getCachingEnabled() {
		c.pushedSchemaIDCacheLock.RLock()
		id, ok := c.pushedSchemaIDCache[util.Hash(schema)]
		c.pushedSchemaIDCacheLock.RUnlock()
		if ok {
			return id, nil
		}
	}

	schemaReq := schemaRequest{Schema: schema, SchemaType: schemaType.String()}
	schemaReqBytes, err := json.Marshal(schemaReq)
	if err != nil {
		return 0, xerrors.Errorf("Can't marshal a schema request: %w", err)
	}
	resp, err := c.httpRequest(http.MethodPost, fmt.Sprintf(subjectVersions, url.QueryEscape(subject)), bytes.NewBuffer(schemaReqBytes))
	if err != nil {
		return 0, xerrors.Errorf("Create schema request error: %w", err)
	}

	schemaResp := new(schemaResponse)
	if err = json.Unmarshal(resp, schemaResp); err != nil {
		return 0, xerrors.Errorf("Can't unmarshal a response with created schemaID: %w", err)
	}

	if c.getCachingEnabled() {
		c.pushedSchemaIDCacheLock.Lock()
		c.pushedSchemaIDCache[util.Hash(schema)] = schemaResp.ID
		c.pushedSchemaIDCacheLock.Unlock()
	}
	return schemaResp.ID, nil
}

func (c *SchemaRegistryClient) SetCredentials(username string, password string) {
	finalUserName := username
	if isYSRHost(c.schemaRegistryURL) {
		finalUserName = "OAuth"
		c.logger.Info("SchemaRegistryClient - username change to OAuth, because schemaRegistryURL is YandexSchemaRegistry")
	}
	if len(username) > 0 && len(password) > 0 {
		c.credentials = &credentials{username: finalUserName, password: password, bearerToken: ""}
	}
}

func (c *SchemaRegistryClient) SetBearerToken(token string) {
	if len(token) > 0 {
		c.credentials = &credentials{username: "", password: "", bearerToken: token}
	}
}

func (c *SchemaRegistryClient) CachingEnabled(value bool) {
	c.cachingEnabled = value
}

func (c *SchemaRegistryClient) httpRequest(method, uri string, payload io.Reader) ([]byte, error) {
	url := fmt.Sprintf("%s%s", c.schemaRegistryURL, uri)
	req, err := http.NewRequest(method, url, payload)
	if err != nil {
		return nil, xerrors.Errorf("Can't create http request: %w", err)
	}
	if c.credentials != nil {
		if len(c.credentials.username) > 0 && len(c.credentials.password) > 0 {
			req.SetBasicAuth(c.credentials.username, c.credentials.password)
		} else if len(c.credentials.bearerToken) > 0 {
			req.Header.Add("Authorization", "Bearer "+c.credentials.bearerToken)
		}
	}
	req.Header.Set("Content-Type", contentType)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, xerrors.Errorf("Can't get http response: %w", err)
	}

	if resp != nil {
		defer resp.Body.Close()
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return nil, xerrors.Errorf("Invalid response http status code (%v): %w", resp.StatusCode, createError(resp))
	}

	return io.ReadAll(resp.Body)
}

func (c *SchemaRegistryClient) getCachingEnabled() bool {
	return c.cachingEnabled
}

// Error implements error, encodes HTTP errors from Schema Registry.
type Error struct {
	Code    int    `json:"error_code"`
	Message string `json:"message"`
}

func (e Error) Error() string {
	return fmt.Sprintf("Error: %v. Error code: %v", e.Message, e.Code)
}

func createError(resp *http.Response) error {
	var err Error
	if json.NewDecoder(resp.Body).Decode(&err) != nil {
		return xerrors.New(resp.Status)
	}
	return err
}

package client

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/library/go/slices"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/errors/coded"
	"github.com/transferria/transferria/pkg/providers"
	"github.com/transferria/transferria/pkg/util/set"
	"go.ytsaurus.tech/library/go/core/log"
)

const (
	requestTimeout = 5 * time.Minute
)

type Client struct {
	brokers   []string
	mechanism sasl.Mechanism
	tlsConfig *tls.Config
}

func NewClient(brokers []string, mechanism sasl.Mechanism, tlsConfig *tls.Config) (*Client, error) {
	if len(brokers) == 0 {
		return nil, abstract.NewFatalError(xerrors.New("expected at least one broker url"))
	}
	return &Client{
		brokers:   brokers,
		mechanism: mechanism,
		tlsConfig: tlsConfig,
	}, nil
}

func (c *Client) broker() string {
	// TM-7644
	return c.brokers[0]
}

func (c *Client) CreateBrokerConn() (*kafka.Conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()

	dialer := kafka.Dialer{
		TLS:           c.tlsConfig,
		SASLMechanism: c.mechanism,
		Timeout:       requestTimeout,
	}
	brokerConn, err := dialer.DialContext(ctx, "tcp", c.broker())
	if err != nil {
		return nil, coded.Errorf(providers.NetworkUnreachable, "unable to DialContext broker, err: %w", err)
	}
	return brokerConn, nil
}

func (c *Client) CreateControllerConn() (*kafka.Conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()

	dialer := kafka.Dialer{
		TLS:           c.tlsConfig,
		SASLMechanism: c.mechanism,
		Timeout:       requestTimeout,
	}
	brokerConn, err := dialer.DialContext(ctx, "tcp", c.broker())
	if err != nil {
		return nil, coded.Errorf(providers.NetworkUnreachable, "unable to DialContext broker, err: %w", err)
	}
	defer brokerConn.Close()

	controller, err := brokerConn.Controller()
	if err != nil {
		return nil, xerrors.Errorf("unable get controller address, err: %w", err)
	}

	controllerConn, err := dialer.DialContext(ctx, "tcp", fmt.Sprintf("%s:%d", controller.Host, controller.Port))
	if err != nil {
		return nil, coded.Errorf(providers.NetworkUnreachable, "unable to DialContext controller, err: %w", err)
	}

	return controllerConn, nil
}

func (c *Client) ListTopics() ([]string, error) {
	conn, err := c.CreateControllerConn()
	if err != nil {
		return nil, xerrors.Errorf("unable to createConn, err: %w", err)
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions()
	if err != nil {
		return nil, xerrors.Errorf("unable to list topics: %w", err)
	}

	return set.New(slices.Map(partitions, func(t kafka.Partition) string {
		return t.Topic
	})...).Slice(), nil
}

func (c *Client) topicExists(topic string) (bool, error) {
	conn, err := c.CreateControllerConn()
	if err != nil {
		return false, xerrors.Errorf("unable to createConn, err: %w", err)
	}
	defer conn.Close()

	_, err = conn.ReadPartitions(topic)
	if err == kafka.UnknownTopicOrPartition {
		return false, nil
	}
	if err != nil {
		return false, xerrors.Errorf("unable to ReadPartitions, err: %w", err)
	}
	return true, nil
}

func (c *Client) CreateTopicIfNotExist(
	lgr log.Logger,
	topic string,
	entries [][2]string,
) error {
	lgr.Infof("check topic exists: %s", topic)
	exists, err := c.topicExists(topic)
	if err != nil {
		return xerrors.Errorf("unable to check if topic exists: %w", err)
	}
	if exists {
		lgr.Infof("topic exists: %s", topic)
		return nil
	} else {
		lgr.Infof("topic not exist: %s", topic)
	}

	conn, err := c.CreateControllerConn()
	if err != nil {
		return xerrors.Errorf("unable to createConn, err: %w", err)
	}
	defer conn.Close()

	lgr.Infof("topic not exists: %s, create", topic)
	return conn.CreateTopics(kafka.TopicConfig{
		Topic:              topic,
		NumPartitions:      -1,
		ReplicationFactor:  -1,
		ReplicaAssignments: nil,
		ConfigEntries: slices.Map(entries, func(t [2]string) kafka.ConfigEntry {
			return kafka.ConfigEntry{
				ConfigName:  t[0],
				ConfigValue: t[1],
			}
		}),
	})
}

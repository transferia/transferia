package logbroker

import (
	"path"
	"strings"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract/model"
)

type LogbrokerCluster string
type LogbrokerInstance string
type TLSMode = model.TLSMode

const (
	Logbroker            LogbrokerCluster = "logbroker"
	Lbkx                 LogbrokerCluster = "lbkx"
	Messenger            LogbrokerCluster = "messenger"
	LogbrokerPrestable   LogbrokerCluster = "logbroker-prestable"
	Lbkxt                LogbrokerCluster = "lbkxt"
	YcLogbroker          LogbrokerCluster = "yc-logbroker"
	YcLogbrokerPrestable LogbrokerCluster = "yc-logbroker-prestable"
	LogbrokerSerbia      LogbrokerCluster = "logbroker-serbia"
	LbkxSerbia           LogbrokerCluster = "lbkx-serbia"
)

const (
	DefaultTLS  = model.DefaultTLS
	EnabledTLS  = model.EnabledTLS
	DisabledTLS = model.DisabledTLS
)

const (
	defaultLogbrokerDatabase = "/Root"
	serbiaDatabasePrefix     = "/Root/logbroker-federation"
	serbiaLogbrokerPort      = 2136
)

// ClusterInstances returns a copy of the instances belonging to cluster.
func ClusterInstances(cluster LogbrokerCluster) ([]LogbrokerInstance, bool) {
	installation, ok := installationByCluster(cluster)
	if !ok {
		return nil, false
	}
	return append([]LogbrokerInstance(nil), installation.instances...), true
}

// UIClusters returns clusters available in the internal UI in display order.
func UIClusters() []LogbrokerCluster {
	result := make([]LogbrokerCluster, 0, len(installations))
	for i := range installations {
		if installations[i].uiVisible {
			result = append(result, installations[i].cluster)
		}
	}
	return result
}

type installationConfig struct {
	cluster   LogbrokerCluster
	instances []LogbrokerInstance

	// port overrides the endpoint port; zero preserves it (effective zero defaults to 2135).
	port int
	// tls overrides the endpoint TLS mode; empty and DefaultTLS preserve it.
	tls TLSMode

	// defaultDatabase is the fallback for multi-DC sources without an explicit database.
	// It may coexist with databasePrefix, but the prefix-derived database wins.
	// If the effective database remains empty, source/sink config builders use /Root.
	defaultDatabase string
	// databasePrefix derives the database as <prefix>/<account> and makes topics account-relative.
	// When nonempty, it overrides explicit/default databases independently of port and TLS.
	// Mutual exclusion with defaultDatabase is not enforced.
	databasePrefix string

	uiVisible bool
}

var installations = []installationConfig{
	{
		cluster: Logbroker,
		instances: []LogbrokerInstance{
			"sas.logbroker.yandex.net",
			"vla.logbroker.yandex.net",
			"klg.logbroker.yandex.net",
		},
		port:            0,
		tls:             DefaultTLS,
		defaultDatabase: "",
		databasePrefix:  "",
		uiVisible:       true,
	},
	{
		cluster: LogbrokerPrestable,
		instances: []LogbrokerInstance{
			"vla.logbroker-prestable.yandex.net",
			"klg.logbroker-prestable.yandex.net",
			"sas.logbroker-prestable.yandex.net",
		},
		port:            0,
		tls:             DefaultTLS,
		defaultDatabase: "",
		databasePrefix:  "",
		uiVisible:       true,
	},
	{
		cluster:         Lbkx,
		instances:       []LogbrokerInstance{"lbkx.logbroker.yandex.net"},
		port:            0,
		tls:             DefaultTLS,
		defaultDatabase: "",
		databasePrefix:  "",
		uiVisible:       true,
	},
	{
		cluster:         Messenger,
		instances:       []LogbrokerInstance{"messenger.logbroker.yandex.net"},
		port:            0,
		tls:             DefaultTLS,
		defaultDatabase: "",
		databasePrefix:  "",
		uiVisible:       false,
	},
	{
		cluster:         Lbkxt,
		instances:       []LogbrokerInstance{"lbkxt.logbroker.yandex.net"},
		port:            0,
		tls:             DefaultTLS,
		defaultDatabase: "",
		databasePrefix:  "",
		uiVisible:       true,
	},
	{
		cluster:         YcLogbroker,
		instances:       []LogbrokerInstance{"lb.etn03iai600jur7pipla.ydb.mdb.yandexcloud.net"},
		port:            0,
		tls:             DefaultTLS,
		defaultDatabase: "/global/b1gvcqr959dbmi1jltep/etn03iai600jur7pipla",
		databasePrefix:  "",
		uiVisible:       false,
	},
	{
		cluster:         YcLogbrokerPrestable,
		instances:       []LogbrokerInstance{"lb.cc8035oc71oh9um52mv3.ydb.mdb.cloud-preprod.yandex.net"},
		port:            0,
		tls:             DefaultTLS,
		defaultDatabase: "/pre-prod_global/aoeb66ftj1tbt1b2eimn/cc8035oc71oh9um52mv3",
		databasePrefix:  "",
		uiVisible:       false,
	},
	{
		cluster: LogbrokerSerbia,
		instances: []LogbrokerInstance{
			"dct.logbroker.yango.tech",
			"orn.logbroker.yango.tech",
		},
		port:            serbiaLogbrokerPort,
		tls:             EnabledTLS,
		defaultDatabase: "",
		databasePrefix:  serbiaDatabasePrefix,
		uiVisible:       true,
	},
	{
		cluster:         LbkxSerbia,
		instances:       []LogbrokerInstance{"lbkx.yango.tech"},
		port:            serbiaLogbrokerPort,
		tls:             EnabledTLS,
		defaultDatabase: "",
		databasePrefix:  serbiaDatabasePrefix,
		uiVisible:       true,
	},
}

type installationSourceConfig struct {
	port     int
	tls      TLSMode
	database string
	topics   []string
}

type installationDestinationConfig struct {
	port        int
	tls         TLSMode
	database    string
	topic       string
	topicPrefix string
}

func alignSourceConfigWithKnownInstallations(
	cluster LogbrokerCluster,
	instance LogbrokerInstance,
	config installationSourceConfig,
) (installationSourceConfig, error) {
	installation, found := resolveInstallation(cluster, instance)
	if !found {
		return config, nil
	}

	if installation.port != 0 {
		config.port = installation.port
	}

	if installation.tls != "" && installation.tls != DefaultTLS {
		config.tls = installation.tls
	}

	if installation.databasePrefix != "" {
		account, relativeTopics, err := extractAccountAndRelativeTopics(config.topics)
		if err != nil {
			return config, xerrors.Errorf("unable to configure cluster %q: unable to split source topics: %w", installation.cluster, err)
		}
		config.database = path.Join(installation.databasePrefix, account)
		config.topics = relativeTopics
	}

	return config, nil
}

func alignDestinationConfigWithKnownInstallations(
	instance LogbrokerInstance,
	config installationDestinationConfig,
) (installationDestinationConfig, error) {
	installation, found := installationByInstance(instance)
	if !found {
		return config, nil
	}

	if installation.port != 0 {
		config.port = installation.port
	}

	if installation.tls != "" && installation.tls != DefaultTLS {
		config.tls = installation.tls
	}

	if installation.databasePrefix != "" {
		topic := config.topic
		if topic == "" {
			topic = config.topicPrefix
		}
		account, relativeTopic, err := extractAccountAndRelativeTopic(topic)
		if err != nil {
			return config, xerrors.Errorf("unable to configure cluster %q: unable to derive account from target topic: %w", installation.cluster, err)
		}
		config.database = path.Join(installation.databasePrefix, account)
		if config.topic != "" {
			config.topic = relativeTopic
		} else {
			config.topicPrefix = relativeTopic
		}
	}

	return config, nil
}

func clusterDefaultDatabase(cluster LogbrokerCluster) (string, bool) {
	installation, ok := installationByCluster(cluster)
	if !ok || installation.defaultDatabase == "" {
		return "", false
	}
	return installation.defaultDatabase, true
}

func resolveInstallation(cluster LogbrokerCluster, instance LogbrokerInstance) (installationConfig, bool) {
	if cluster != "" {
		return installationByCluster(cluster)
	}
	return installationByInstance(instance)
}

func installationByCluster(cluster LogbrokerCluster) (installationConfig, bool) {
	for i := range installations {
		if installations[i].cluster == cluster {
			return installations[i], true
		}
	}
	var config installationConfig
	return config, false
}

func installationByInstance(instance LogbrokerInstance) (installationConfig, bool) {
	for i := range installations {
		for _, knownInstance := range installations[i].instances {
			if knownInstance == instance {
				return installations[i], true
			}
		}
	}
	var config installationConfig
	return config, false
}

func extractAccountAndRelativeTopics(topics []string) (string, []string, error) {
	if len(topics) == 0 {
		return "", nil, xerrors.New("topics must not be empty")
	}

	account, relativeTopic, err := extractAccountAndRelativeTopic(topics[0])
	if err != nil {
		return "", nil, xerrors.Errorf("unable to derive account from topic: %w", err)
	}
	relativeTopics := make([]string, len(topics))
	relativeTopics[0] = relativeTopic
	for i, topic := range topics[1:] {
		currentAccount, currentRelativeTopic, err := extractAccountAndRelativeTopic(topic)
		if err != nil {
			return "", nil, xerrors.Errorf("unable to derive account from topic: %w", err)
		}
		if currentAccount != account {
			return "", nil, xerrors.Errorf("topics must belong to one account: got %q and %q", account, currentAccount)
		}
		relativeTopics[i+1] = currentRelativeTopic
	}
	return account, relativeTopics, nil
}

func extractAccountAndRelativeTopic(topic string) (string, string, error) {
	cleanTopic := strings.Trim(topic, "/")
	account, relativeTopic, found := strings.Cut(cleanTopic, "/")
	if !found || account == "" || relativeTopic == "" {
		return "", "", xerrors.Errorf("topic %q must contain an account and a topic name", topic)
	}
	return account, relativeTopic, nil
}

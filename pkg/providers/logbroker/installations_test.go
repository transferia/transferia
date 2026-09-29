package logbroker

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInstallationCatalog(t *testing.T) {
	t.Run("LogbrokerSerbiaExposesBothDataPlaneInstances", func(t *testing.T) {
		instances, ok := ClusterInstances(LogbrokerSerbia)
		require.True(t, ok)
		require.Equal(t, []LogbrokerInstance{
			"dct.logbroker.yango.tech",
			"orn.logbroker.yango.tech",
		}, instances)
		require.True(t, checkInstanceValidity("dct.logbroker.yango.tech"))
		require.True(t, checkInstanceValidity("orn.logbroker.yango.tech"))
	})

	t.Run("LbkxSerbiaExposesItsGlobalInstance", func(t *testing.T) {
		instances, ok := ClusterInstances(LbkxSerbia)
		require.True(t, ok)
		require.Equal(t, []LogbrokerInstance{"lbkx.yango.tech"}, instances)
		require.True(t, checkInstanceValidity("lbkx.yango.tech"))
	})

	t.Run("ReturnedInstancesDoNotMutateCatalog", func(t *testing.T) {
		instances, ok := ClusterInstances(LogbrokerSerbia)
		require.True(t, ok)
		instances[0] = "modified"

		actual, ok := ClusterInstances(LogbrokerSerbia)
		require.True(t, ok)
		require.Equal(t, []LogbrokerInstance{
			"dct.logbroker.yango.tech",
			"orn.logbroker.yango.tech",
		}, actual)
	})

	t.Run("UnknownClusterHasNoInstances", func(t *testing.T) {
		instances, ok := ClusterInstances("unknown")
		require.False(t, ok)
		require.Nil(t, instances)
	})

	t.Run("UIContainsOnlyVisibleClustersInDisplayOrder", func(t *testing.T) {
		require.Equal(t, []LogbrokerCluster{
			Logbroker,
			LogbrokerPrestable,
			Lbkx,
			Lbkxt,
			LogbrokerSerbia,
			LbkxSerbia,
		}, UIClusters())
	})
}

func TestClusterDefaultDatabase(t *testing.T) {
	t.Run("YandexCloudProduction", func(t *testing.T) {
		database, ok := clusterDefaultDatabase(YcLogbroker)
		require.True(t, ok)
		require.Equal(t, "/global/b1gvcqr959dbmi1jltep/etn03iai600jur7pipla", database)
	})

	t.Run("YandexCloudPrestable", func(t *testing.T) {
		database, ok := clusterDefaultDatabase(YcLogbrokerPrestable)
		require.True(t, ok)
		require.Equal(t, "/pre-prod_global/aoeb66ftj1tbt1b2eimn/cc8035oc71oh9um52mv3", database)
	})

	t.Run("ClusterWithoutDatabaseOverride", func(t *testing.T) {
		database, ok := clusterDefaultDatabase(Logbroker)
		require.False(t, ok)
		require.Empty(t, database)
	})

	t.Run("UnknownCluster", func(t *testing.T) {
		database, ok := clusterDefaultDatabase("unknown")
		require.False(t, ok)
		require.Empty(t, database)
	})
}

func TestSourceInstallationConnectionConfig(t *testing.T) {
	t.Run("LogbrokerSerbiaClusterOverridesConnectionSettings", func(t *testing.T) {
		source := &LfSource{
			Cluster:  LogbrokerSerbia,
			Instance: "dct.logbroker.yango.tech",
			Topics:   []string{"/account/topic", "account/other-topic"},
			Port:     2135,
			TLS:      DisabledTLS,
			Database: "/Root",
			Consumer: "account/consumer",
		}
		requireSourceConnection(
			t,
			source,
			"dct.logbroker.yango.tech:2136",
			"/Root/logbroker-federation/account",
			[]string{"topic", "other-topic"},
			true,
		)
	})

	t.Run("DctInstanceUsesProvenSourceParameters", func(t *testing.T) {
		source := &LfSource{
			Instance: "dct.logbroker.yango.tech",
			Topics:   []string{"/transfer-test-srb/test-topic"},
			Consumer: "transfer-test-srb/test-consumer",
		}
		requireSourceConnection(
			t,
			source,
			"dct.logbroker.yango.tech:2136",
			"/Root/logbroker-federation/transfer-test-srb",
			[]string{"test-topic"},
			true,
		)
	})

	t.Run("NestedTopicUsesFirstSegmentAsAccount", func(t *testing.T) {
		source := &LfSource{
			Instance: "orn.logbroker.yango.tech",
			Topics:   []string{"team/account/topic"},
			Consumer: "team/account/consumer",
		}
		requireSourceConnection(
			t,
			source,
			"orn.logbroker.yango.tech:2136",
			"/Root/logbroker-federation/team",
			[]string{"account/topic"},
			true,
		)
	})

	t.Run("LbkxSerbiaUsesFederationConnection", func(t *testing.T) {
		source := &LfSource{
			Cluster:  LbkxSerbia,
			Instance: "lbkx.yango.tech",
			Topics:   []string{"account/topic"},
			Consumer: "account/consumer",
		}
		requireSourceConnection(
			t,
			source,
			"lbkx.yango.tech:2136",
			"/Root/logbroker-federation/account",
			[]string{"topic"},
			true,
		)
	})

	t.Run("LegacyInstallationPreservesExplicitSettings", func(t *testing.T) {
		source := &LfSource{
			Cluster:  Logbroker,
			Instance: "sas.logbroker.yandex.net",
			Topics:   []string{"account/topic"},
			Port:     1234,
			TLS:      DisabledTLS,
			Database: "/custom",
			Consumer: "account/consumer",
		}
		requireSourceConnection(
			t,
			source,
			"sas.logbroker.yandex.net:1234",
			"/custom",
			[]string{"account/topic"},
			false,
		)
	})
}

func TestDestinationInstallationConnectionConfig(t *testing.T) {
	t.Run("TopicDeterminesAccount", func(t *testing.T) {
		destination := &LbDestination{
			Instance: "dct.logbroker.yango.tech",
			Topic:    "/account/topic",
			Port:     2135,
			TLS:      DisabledTLS,
			Database: "/Root",
		}
		requireDestinationConnection(
			t,
			destination,
			"dct.logbroker.yango.tech:2136",
			"/Root/logbroker-federation/account",
			"topic",
			"",
			true,
		)
	})

	t.Run("TopicPrefixDeterminesAccount", func(t *testing.T) {
		destination := &LbDestination{
			Instance:    "orn.logbroker.yango.tech",
			TopicPrefix: "team/account/topic-prefix",
		}
		requireDestinationConnection(
			t,
			destination,
			"orn.logbroker.yango.tech:2136",
			"/Root/logbroker-federation/team",
			"",
			"account/topic-prefix",
			true,
		)
	})

	t.Run("LbkxSerbiaUsesFederationConnection", func(t *testing.T) {
		destination := &LbDestination{
			Instance: "lbkx.yango.tech",
			Topic:    "account/topic",
		}
		requireDestinationConnection(
			t,
			destination,
			"lbkx.yango.tech:2136",
			"/Root/logbroker-federation/account",
			"topic",
			"",
			true,
		)
	})

	t.Run("LegacyInstallationPreservesExplicitSettings", func(t *testing.T) {
		destination := &LbDestination{
			Instance: "sas.logbroker.yandex.net",
			Topic:    "account/topic",
			Port:     1234,
			TLS:      DisabledTLS,
			Database: "/custom",
		}
		requireDestinationConnection(
			t,
			destination,
			"sas.logbroker.yandex.net:1234",
			"/custom",
			"account/topic",
			"",
			false,
		)
	})
}

func TestInstallationTransportOverridesWithoutDatabasePrefix(t *testing.T) {
	const endpointPort = 1234
	testCases := []struct {
		name         string
		installation installationConfig
		endpointTLS  TLSMode
		wantTLS      TLSMode
		wantPort     int
	}{
		{
			name:        "EmptySettingsPreserveEndpoint",
			endpointTLS: EnabledTLS,
			wantTLS:     EnabledTLS,
			wantPort:    endpointPort,
		},
		{
			name:         "DefaultTLSPreservesEnabledEndpoint",
			installation: installationConfig{tls: DefaultTLS},
			endpointTLS:  EnabledTLS,
			wantTLS:      EnabledTLS,
			wantPort:     endpointPort,
		},
		{
			name:         "DefaultTLSPreservesDisabledEndpoint",
			installation: installationConfig{tls: DefaultTLS},
			endpointTLS:  DisabledTLS,
			wantTLS:      DisabledTLS,
			wantPort:     endpointPort,
		},
		{
			name:         "RequiredTLSOverridesDisabledEndpoint",
			installation: installationConfig{tls: EnabledTLS},
			endpointTLS:  DisabledTLS,
			wantTLS:      EnabledTLS,
			wantPort:     endpointPort,
		},
		{
			name:         "DisabledTLSOverridesEnabledEndpoint",
			installation: installationConfig{tls: DisabledTLS},
			endpointTLS:  EnabledTLS,
			wantTLS:      DisabledTLS,
			wantPort:     endpointPort,
		},
		{
			name:         "PortOverridePreservesTLS",
			installation: installationConfig{port: serbiaLogbrokerPort},
			endpointTLS:  EnabledTLS,
			wantTLS:      EnabledTLS,
			wantPort:     serbiaLogbrokerPort,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			installation := registerTestInstallation(t, tc.installation)
			source := installationSourceConfig{
				port: endpointPort, tls: tc.endpointTLS, database: "/custom", topics: []string{"topic"},
			}
			wantSource := source
			wantSource.port, wantSource.tls = tc.wantPort, tc.wantTLS
			actualSource, err := alignSourceConfigWithKnownInstallations(installation.cluster, "", source)
			require.NoError(t, err)
			require.Equal(t, wantSource, actualSource)

			destination := installationDestinationConfig{
				port: endpointPort, tls: tc.endpointTLS, database: "/custom", topic: "topic",
			}
			wantDestination := destination
			wantDestination.port, wantDestination.tls = tc.wantPort, tc.wantTLS
			actualDestination, err := alignDestinationConfigWithKnownInstallations(installation.instances[0], destination)
			require.NoError(t, err)
			require.Equal(t, wantDestination, actualDestination)
		})
	}
}

func TestInstallationDatabasePrefixPreservesTransport(t *testing.T) {
	installation := registerTestInstallation(t, installationConfig{databasePrefix: serbiaDatabasePrefix})
	source, err := alignSourceConfigWithKnownInstallations(installation.cluster, "", installationSourceConfig{
		port: 1234, tls: DisabledTLS, database: "/custom", topics: []string{"account/topic"},
	})
	require.NoError(t, err)
	require.Equal(t, installationSourceConfig{
		port: 1234, tls: DisabledTLS, database: serbiaDatabasePrefix + "/account", topics: []string{"topic"},
	}, source)

	destination, err := alignDestinationConfigWithKnownInstallations(installation.instances[0], installationDestinationConfig{
		port: 1234, tls: DisabledTLS, database: "/custom", topicPrefix: "account/prefix",
	})
	require.NoError(t, err)
	require.Equal(t, installationDestinationConfig{
		port: 1234, tls: DisabledTLS, database: serbiaDatabasePrefix + "/account", topicPrefix: "prefix",
	}, destination)
}

func TestInstallationConnectionConfigErrors(t *testing.T) {
	t.Run("SourceHasNoTopics", func(t *testing.T) {
		source := &LfSource{Cluster: LogbrokerSerbia}

		_, err := source.buildTopicSourceConfig()
		require.ErrorContains(t, err, "topics must not be empty")
	})

	t.Run("SourceTopicHasNoAccount", func(t *testing.T) {
		source := &LfSource{
			Cluster: LogbrokerSerbia,
			Topics:  []string{"topic"},
		}

		_, err := source.buildTopicSourceConfig()
		require.ErrorContains(t, err, "must contain an account and a topic name")
	})

	t.Run("SourceTopicHasNoName", func(t *testing.T) {
		source := &LfSource{
			Cluster: LogbrokerSerbia,
			Topics:  []string{"account/"},
		}

		_, err := source.buildTopicSourceConfig()
		require.ErrorContains(t, err, "must contain an account and a topic name")
	})

	t.Run("SourceTopicsBelongToDifferentAccounts", func(t *testing.T) {
		source := &LfSource{
			Cluster: LogbrokerSerbia,
			Topics:  []string{"first/topic", "second/topic"},
		}

		_, err := source.buildTopicSourceConfig()
		require.ErrorContains(t, err, "topics must belong to one account")
	})

	t.Run("DestinationTopicHasNoAccount", func(t *testing.T) {
		destination := &LbDestination{
			Instance: "lbkx.yango.tech",
			Topic:    "topic",
		}

		_, err := destination.TopicSinkConfig()
		require.ErrorContains(t, err, "must contain an account and a topic name")
	})
}

func registerTestInstallation(t *testing.T, installation installationConfig) installationConfig {
	t.Helper()
	installation.cluster = "test-installation"
	installation.instances = []LogbrokerInstance{"test.logbroker.invalid"}
	previous := installations
	installations = append([]installationConfig{installation}, installations...)
	t.Cleanup(func() {
		installations = previous
	})
	return installation
}

func requireSourceConnection(
	t *testing.T,
	source *LfSource,
	expectedEndpoint string,
	expectedDatabase string,
	expectedTopics []string,
	expectedTLS bool,
) {
	t.Helper()
	original := *source

	config, err := source.buildTopicSourceConfig()
	require.NoError(t, err)
	require.Equal(t, expectedEndpoint, config.Connection.Endpoint)
	require.Equal(t, expectedDatabase, config.Connection.Database)
	require.Equal(t, expectedTopics, config.Topics)
	require.Equal(t, source.Consumer, config.Consumer)
	require.Equal(t, expectedTLS, config.Connection.TLSEnabled)
	require.Equal(t, original, *source)
}

func requireDestinationConnection(
	t *testing.T,
	destination *LbDestination,
	expectedEndpoint string,
	expectedDatabase string,
	expectedTopic string,
	expectedTopicPrefix string,
	expectedTLS bool,
) {
	t.Helper()
	original := *destination

	config, err := destination.TopicSinkConfig()
	require.NoError(t, err)
	require.Equal(t, expectedEndpoint, config.Connection.Endpoint)
	require.Equal(t, expectedDatabase, config.Connection.Database)
	require.Equal(t, expectedTopic, config.Topic)
	require.Equal(t, expectedTopicPrefix, config.TopicPrefix)
	require.Equal(t, expectedTLS, config.Connection.TLSEnabled)
	require.Equal(t, original, *destination)
}

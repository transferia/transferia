package engines

const (
	mergeTree                    = engineType("MergeTree")
	replacingMergeTree           = engineType("ReplacingMergeTree")
	summingMergeTree             = engineType("SummingMergeTree")
	aggregatingMergeTree         = engineType("AggregatingMergeTree")
	collapsingMergeTree          = engineType("CollapsingMergeTree")
	versionedCollapsingMergeTree = engineType("VersionedCollapsingMergeTree")
	graphiteMergeTree            = engineType("GraphiteMergeTree")

	replicatedMergeTree                    = engineType("ReplicatedMergeTree")
	replicatedReplacingMergeTree           = engineType("ReplicatedReplacingMergeTree")
	replicatedSummingMergeTree             = engineType("ReplicatedSummingMergeTree")
	replicatedAggregatingMergeTree         = engineType("ReplicatedAggregatingMergeTree")
	replicatedCollapsingMergeTree          = engineType("ReplicatedCollapsingMergeTree")
	replicatedVersionedCollapsingMergeTree = engineType("ReplicatedVersionedCollapsingMergeTree")
	replicatedGraphiteMergeTree            = engineType("ReplicatedGraphiteMergeTree")

	// SharedMergeTree family is available on ch.inc exclusively
	sharedMergeTree                    = engineType("SharedMergeTree")
	sharedReplacingMergeTree           = engineType("SharedReplacingMergeTree")
	sharedSummingMergeTree             = engineType("SharedSummingMergeTree")
	sharedAggregatingMergeTree         = engineType("SharedAggregatingMergeTree")
	sharedCollapsingMergeTree          = engineType("SharedCollapsingMergeTree")
	sharedVersionedCollapsingMergeTree = engineType("SharedVersionedCollapsingMergeTree")
	sharedGraphiteMergeTree            = engineType("SharedGraphiteMergeTree")
)

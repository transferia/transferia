//go:build !disable_greenplum_provider

package greenplum

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func checkGpHPWithMDBReplacement(t *testing.T, host string, expectedHost string, port int) {
	hp := NewGpHpWithMDBReplacement(host, port)
	require.Equal(t, expectedHost, hp.Host)
	require.Equal(t, port, hp.Port)
}

func checkGpHPWithMDBReplacementHostUnchanged(t *testing.T, host string, port int) {
	checkGpHPWithMDBReplacement(t, host, host, port)
}

func TestNewGpHPWithMDBReplacementPreprodCommon(t *testing.T) {
	checkGpHPWithMDBReplacement(t, "rc1b-2mmt8eqi3uas7e0u.mdb.cloud-preprod.yandex.net", "rc1b-2mmt8eqi3uas7e0u.db.yandex.net", 6000)
}

func TestNewGpHPWithMDBReplacementPreprodOnPremises(t *testing.T) {
	checkGpHPWithMDBReplacementHostUnchanged(t, "gpseg0.mdb.cloud-preprod.onpremises.net", 6000)
}

func TestNewGpHPWithMDBReplacementPreprodStrangeName(t *testing.T) {
	checkGpHPWithMDBReplacementHostUnchanged(t, "rc1b-2mmt8eqi3uas7e0u.mdb.cloud-preprod.yandex.net.nic.ru", 12345)
}

func TestNewGpHPWithMDBReplacementProdCommon(t *testing.T) {
	checkGpHPWithMDBReplacement(t, "rc1b-o7rjkubsbekh2itt.mdb.yandexcloud.net", "rc1b-o7rjkubsbekh2itt.db.yandex.net", 6000)
}

func TestNewGpHPWithMDBReplacementProdOnPremises(t *testing.T) {
	checkGpHPWithMDBReplacementHostUnchanged(t, "gpseg0.mdb.onpremises.net", 6000)
}

func TestNewGpHPWithMDBReplacementProdOnStrangeName(t *testing.T) {
	checkGpHPWithMDBReplacementHostUnchanged(t, "rc1b-o7rjkubsbekh2itt.mdb.yandexcloud.net.nic.ru", 12345)
}

func TestNewGPHPWithMDBReplacementInternal(t *testing.T) {
	checkGpHPWithMDBReplacement(t, "sas-fjeeagflm78c89k4.db.yandex.net", "sas-fjeeagflm78c89k4.mdb.yandex.net", 6000)
}

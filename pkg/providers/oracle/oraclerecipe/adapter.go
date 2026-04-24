package oraclerecipe

import (
	"context"
	"os"
	"strconv"

	"github.com/transferia/transferia/pkg/abstract/model"
	oracle "github.com/transferia/transferia/pkg/providers/oracle"
)

// RecipeOracleSource returns an OracleSource populated from RECIPE_ORACLE_* env vars.
// Calls PrepareContainer so it's safe to call at package init time.
func RecipeOracleSource() *oracle.OracleSource {
	PrepareContainer(context.Background())

	port, _ := strconv.Atoi(os.Getenv("RECIPE_ORACLE_PORT"))

	src := new(oracle.OracleSource)
	src.ConnectionType = oracle.OracleServiceNameConnection
	src.Host = os.Getenv("RECIPE_ORACLE_HOST")
	src.Port = port
	src.ServiceName = os.Getenv("RECIPE_ORACLE_SERVICE")
	src.User = os.Getenv("RECIPE_ORACLE_USER")
	src.Password = model.SecretString(os.Getenv("RECIPE_ORACLE_PASSWORD"))
	src.IsNonConsistentSnapshot = true
	src.TrackerType = oracle.OracleNoLogTracker
	src.WithDefaults()
	return src
}

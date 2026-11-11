package mysql

import (
	"testing"

	"github.com/pingcap/parser"
	"github.com/stretchr/testify/assert"
)

func TestParserUnknownCharsetUtf8mb3(t *testing.T) {
	p := parser.New()
	t.Run("utf8mb3_general_ci", func(t *testing.T) {
		ddl := "CREATE TABLE categories (id int(10) unsigned NOT NULL AUTO_INCREMENT, title varchar(255) NOT NULL, created_at timestamp NULL DEFAULT NULL, updated_at timestamp NULL DEFAULT NULL, PRIMARY KEY (`id`)) ENGINE=InnoDB AUTO_INCREMENT=17 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;"
		_, _, err := p.Parse(ddl, "", "")
		assert.NoError(t, err)
	})
	t.Run("utf8mb3_unicode_ci", func(t *testing.T) {
		ddl := "CREATE TABLE categories (id int(10) unsigned NOT NULL AUTO_INCREMENT, title varchar(255) NOT NULL, created_at timestamp NULL DEFAULT NULL, updated_at timestamp NULL DEFAULT NULL, PRIMARY KEY (`id`)) ENGINE=InnoDB AUTO_INCREMENT=17 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_unicode_ci;"
		_, _, err := p.Parse(ddl, "", "")
		assert.NoError(t, err)
	})

}

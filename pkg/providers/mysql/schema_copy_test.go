//go:build !disable_mysql_provider

package mysql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseMySQLTableDDL(t *testing.T) {
	table := StatementID{Namespace: "db", Name: "dept_emp"}
	clearDDL, keys := parseTableDDL(table, "CREATE TABLE `dept_emp` (\n  `emp_no` int(11) NOT NULL,\n  `dept_no` char(4) NOT NULL,\n  `from_date` date NOT NULL,\n  `to_date` date NOT NULL,\n  PRIMARY KEY (`emp_no`,`dept_no`),\n  KEY `dept_no` (`dept_no`),\n  CONSTRAINT `dept_emp_ibfk_1` FOREIGN KEY (`emp_no`) REFERENCES `employees` (`emp_no`) ON DELETE CASCADE,\n  CONSTRAINT `dept_emp_ibfk_2` FOREIGN KEY (`dept_no`) REFERENCES `departments` (`dept_no`) ON DELETE CASCADE\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci")
	require.Equal(t, "CREATE TABLE IF NOT EXISTS `dept_emp` (\n  `emp_no` int(11) NOT NULL,\n  `dept_no` char(4) NOT NULL,\n  `from_date` date NOT NULL,\n  `to_date` date NOT NULL,\n  PRIMARY KEY (`emp_no`,`dept_no`),\n  KEY `dept_no` (`dept_no`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci", clearDDL)
	require.Equal(t, 2, len(keys))
}

func TestParseMySQLViewDDL(t *testing.T) {
	clearDDL, _ := parseViewDDL("CREATE ALGORITHM=UNDEFINED DEFINER=`mysql.sys`@`localhost` SQL SECURITY INVOKER VIEW `sys`.`version` (`sys_version`,`mysql_version`) AS select '2.1.1' AS `sys_version`,version() AS `mysql_version`")
	require.Equal(t, "CREATE OR REPLACE ALGORITHM=UNDEFINED VIEW `sys`.`version` (`sys_version`,`mysql_version`) AS select '2.1.1' AS `sys_version`,version() AS `mysql_version`", clearDDL)
}

func TestParseMySQLRoutineDDL(t *testing.T) {
	clearDDL, _ := parseRoutineDDL(&RoutineRow{Schema: "db", Name: "version_major", Typ: "FUNCTION"},
		`CREATE DEFINER=user FUNCTION version_major() RETURNS tinyint unsigned
    NO SQL
    SQL SECURITY INVOKER
    COMMENT '123'
BEGIN
    RETURN SUBSTRING_INDEX(SUBSTRING_INDEX(VERSION(), '-', 1), '.', 1);
END`)
	require.Equal(t, "drop FUNCTION if exists `version_major`;\n"+
		`CREATE FUNCTION version_major() RETURNS tinyint unsigned
    NO SQL
    COMMENT '123'
BEGIN
    RETURN SUBSTRING_INDEX(SUBSTRING_INDEX(VERSION(), '-', 1), '.', 1);
END`, clearDDL)
}

func TestParseMySQLTriggerDDL(t *testing.T) {
	id := TriggerRow{Namespace: "db", Name: "smpl_trg"}
	clearDDL, _ := parseTriggerDDL(id, "CREATE DEFINER=`root`@`localhost` TRIGGER `smpl_trg` BEFORE INSERT ON `customers` FOR EACH ROW SET NEW.`creditLimit` = NEW.`creditLimit` * 1000")
	require.Equal(t, "drop trigger if exists `smpl_trg`;\n"+
		"CREATE TRIGGER `smpl_trg` BEFORE INSERT ON `customers` FOR EACH ROW SET NEW.`creditLimit` = NEW.`creditLimit` * 1000", clearDDL)
}

func TestParseMySQLTriggerDDLInUpperLowerCase(t *testing.T) {
	id := TriggerRow{Namespace: "db", Name: "smpl_trg"}
	clearDDL, _ := parseTriggerDDL(id, "CREATE DEFINER=`adfox-root`@`%` trigger targeting_hyperlocal_pre_insert before insert on targeting_hyperlocal for each row BEGIN IF EXISTS (SELECT 1 FROM targeting_profile WHERE id = NEW.targeting_profile_id AND deps = 0) THEN SET NEW.deps = REPLACE(NEW.deps, 'dep_targeting_profile', ''); ELSE SET NEW.deps = CONCAT_WS(',', NEW.deps, 'dep_targeting_profile'); END IF; IF NEW.is_deleted = 1 THEN SET NEW.deps = CONCAT_WS(',', NEW.deps, 'dep_deleted'); END IF; END")
	require.Equal(t, "drop trigger if exists `smpl_trg`;\n"+
		"CREATE trigger targeting_hyperlocal_pre_insert before insert on targeting_hyperlocal for each row BEGIN IF EXISTS (SELECT 1 FROM targeting_profile WHERE id = NEW.targeting_profile_id AND deps = 0) THEN SET NEW.deps = REPLACE(NEW.deps, 'dep_targeting_profile', ''); ELSE SET NEW.deps = CONCAT_WS(',', NEW.deps, 'dep_targeting_profile'); END IF; IF NEW.is_deleted = 1 THEN SET NEW.deps = CONCAT_WS(',', NEW.deps, 'dep_deleted'); END IF; END", clearDDL)
}

func TestParseMySQLPartitions(t *testing.T) {
	clearDDL, _ := parseTableDDL(StatementID{Name: "testtable_no_comment"}, `CREATE TABLE test_partitions (
id bigint(20) unsigned NOT NULL AUTO_INCREMENT,
name char(255) DEFAULT NULL,
categoryID int(6) unsigned NOT NULL,
PRIMARY KEY (id,categoryID)
) ENGINE=InnoDB AUTO_INCREMENT=1313963376 DEFAULT CHARSET=utf8
PARTITION BY RANGE (categoryID)
(PARTITION p0 VALUES LESS THAN (300) ENGINE = InnoDB,
PARTITION p1 VALUES LESS THAN (600) ENGINE = InnoDB,
PARTITION p2 VALUES LESS THAN (900) ENGINE = InnoDB,
PARTITION p3 VALUES LESS THAN (1200) ENGINE = InnoDB,
PARTITION p4 VALUES LESS THAN (1500) ENGINE = InnoDB,
PARTITION p5 VALUES LESS THAN (1800) ENGINE = InnoDB,
PARTITION p6 VALUES LESS THAN (2100) ENGINE = InnoDB,
PARTITION p7 VALUES LESS THAN (2400) ENGINE = InnoDB,
PARTITION p8 VALUES LESS THAN (2700) ENGINE = InnoDB,
PARTITION p9 VALUES LESS THAN (3000) ENGINE = InnoDB)
`)
	require.Equal(t, `CREATE TABLE IF NOT EXISTS test_partitions (
id bigint(20) unsigned NOT NULL AUTO_INCREMENT,
name char(255) DEFAULT NULL,
categoryID int(6) unsigned NOT NULL,
PRIMARY KEY (id,categoryID)
) ENGINE=InnoDB AUTO_INCREMENT=1313963376 DEFAULT CHARSET=utf8
PARTITION BY RANGE (categoryID)
(PARTITION p0 VALUES LESS THAN (300) ENGINE = InnoDB,
PARTITION p1 VALUES LESS THAN (600) ENGINE = InnoDB,
PARTITION p2 VALUES LESS THAN (900) ENGINE = InnoDB,
PARTITION p3 VALUES LESS THAN (1200) ENGINE = InnoDB,
PARTITION p4 VALUES LESS THAN (1500) ENGINE = InnoDB,
PARTITION p5 VALUES LESS THAN (1800) ENGINE = InnoDB,
PARTITION p6 VALUES LESS THAN (2100) ENGINE = InnoDB,
PARTITION p7 VALUES LESS THAN (2400) ENGINE = InnoDB,
PARTITION p8 VALUES LESS THAN (2700) ENGINE = InnoDB,
PARTITION p9 VALUES LESS THAN (3000) ENGINE = InnoDB)`, clearDDL)
}

func TestParseMySQLPartitionsWithSpaces(t *testing.T) { // CLOUDSUPPORT-112909
	clearDDL, _ := parseTableDDL(StatementID{Name: "testtable_no_comment"}, `CREATE TABLE test_partitions (
  id bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  name char(255) DEFAULT NULL,
  categoryID int(6) unsigned NOT NULL,
  PRIMARY KEY (id,categoryID)
) ENGINE=InnoDB AUTO_INCREMENT=1313963376 DEFAULT CHARSET=utf8
 PARTITION BY RANGE (categoryID)
(PARTITION p0 VALUES LESS THAN (300) ENGINE = InnoDB,
 PARTITION p1 VALUES LESS THAN (600) ENGINE = InnoDB,
 PARTITION p2 VALUES LESS THAN (900) ENGINE = InnoDB,
 PARTITION p3 VALUES LESS THAN (1200) ENGINE = InnoDB,
 PARTITION p4 VALUES LESS THAN (1500) ENGINE = InnoDB,
 PARTITION p5 VALUES LESS THAN (1800) ENGINE = InnoDB,
 PARTITION p6 VALUES LESS THAN (2100) ENGINE = InnoDB,
 PARTITION p7 VALUES LESS THAN (2400) ENGINE = InnoDB,
 PARTITION p8 VALUES LESS THAN (2700) ENGINE = InnoDB,
 PARTITION p9 VALUES LESS THAN (3000) ENGINE = InnoDB)
`)
	require.Equal(t, `CREATE TABLE IF NOT EXISTS test_partitions (
  id bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  name char(255) DEFAULT NULL,
  categoryID int(6) unsigned NOT NULL,
  PRIMARY KEY (id,categoryID)
) ENGINE=InnoDB AUTO_INCREMENT=1313963376 DEFAULT CHARSET=utf8
 PARTITION BY RANGE (categoryID)
(PARTITION p0 VALUES LESS THAN (300) ENGINE = InnoDB,
 PARTITION p1 VALUES LESS THAN (600) ENGINE = InnoDB,
 PARTITION p2 VALUES LESS THAN (900) ENGINE = InnoDB,
 PARTITION p3 VALUES LESS THAN (1200) ENGINE = InnoDB,
 PARTITION p4 VALUES LESS THAN (1500) ENGINE = InnoDB,
 PARTITION p5 VALUES LESS THAN (1800) ENGINE = InnoDB,
 PARTITION p6 VALUES LESS THAN (2100) ENGINE = InnoDB,
 PARTITION p7 VALUES LESS THAN (2400) ENGINE = InnoDB,
 PARTITION p8 VALUES LESS THAN (2700) ENGINE = InnoDB,
 PARTITION p9 VALUES LESS THAN (3000) ENGINE = InnoDB)`, clearDDL)
}

func TestForeignKeyWithIndex(t *testing.T) {
	statementID := StatementID{
		Namespace: "schema",
		Name:      "table101",
	}
	table, foreignKeys := parseTableDDL(statementID, `CREATE TABLE "table101" (
  "id" bigint COMMENT 'ID?',
  "tent" int,
  "column" int,
  "again" int,
  "wtf" int NOT NULL,
  "foo" bigint,
  "bar" tinyint,
  PRIMARY KEY ("id"),
  UNIQUE KEY "id" ("id"),
  KEY "key1" ("wtf") USING BTREE,
  KEY "key2" ("tent") USING BTREE,
  KEY "fk" ("foo"),
  CONSTRAINT "fk" FOREIGN KEY ("foo") REFERENCES "what" ("id") ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=lol COLLATE=lol`)
	require.Equal(t, `CREATE TABLE IF NOT EXISTS "table101" (
  "id" bigint COMMENT 'ID?',
  "tent" int,
  "column" int,
  "again" int,
  "wtf" int NOT NULL,
  "foo" bigint,
  "bar" tinyint,
  PRIMARY KEY ("id"),
  UNIQUE KEY "id" ("id"),
  KEY "key1" ("wtf") USING BTREE,
  KEY "key2" ("tent") USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=lol COLLATE=lol`, table)
	require.Equal(t, 1, len(foreignKeys))
	require.Equal(t,
		"ALTER TABLE `schema`.`table101` ADD   CONSTRAINT \"fk\" FOREIGN KEY (\"foo\") REFERENCES \"what\" (\"id\") ON DELETE CASCADE",
		foreignKeys[0])
}

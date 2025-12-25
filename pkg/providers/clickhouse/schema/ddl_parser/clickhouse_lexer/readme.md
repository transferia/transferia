# How to generate clickhouse_lexer.go - oneliner

`wget https://raw.githubusercontent.com/ClickHouse/ClickHouse/refs/heads/master/utils/antlr/ClickHouseLexer.g4 && antlr -Dlanguage=Go ./ClickHouseLexer.g4 && rm ./*.g4 ./*.interp ./*.token`

# How to generate clickhouse_lexer.go - detailed

take https://github.com/ClickHouse/ClickHouse/blob/master/utils/antlr/ClickHouseLexer.g4

generate sources:

`antlr -Dlanguage=Go ./ClickHouseLexer.g4`

remove useless files:

`rm ./*.g4 ./*.interp ./*.tokens`


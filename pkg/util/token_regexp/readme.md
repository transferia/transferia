## package description

token_regexp package - it's like standard golang 'regexp' package, but works with tokens instead of chars.


## before

Before, when you want to extract something from DDL/DML, you needed to write regex - like:

```go
createDDLre := regexp.MustCompile(`(?mis)` + // multiline, ignore case, dot matches new line
	`(create\s+(table|materialized\s+view)(\s+if\s+not\s+exists)?\s+(.+?))` + // create table/mv and name
	"(\\s+uuid\\s'[^']+')?(\\s+on\\s+cluster(\\s+[^\\s]+|\\s+`[^`]+`|\\s+\"[^\"]+\"|))?" + // uuid, on cluster optional clauses
	`\s*\((?:[^)(]+|\((?:[^)(]+|\([^)(]*\))*\))*\)\s*` + // table guts
	`engine\s*=\s*(([^\s]+\s*\([^)]+\))|([^\s]+))`, // engine
)

createDdlSQL = strings.TrimRight(createDdlSQL, "\n\r\t ;")
res := createDDLre.FindAllStringSubmatch(createDdlSQL, -1)
```

it has several common problems:
* regexps can't match real-world grammars (at least bcs of recursion in grammars, which is absent in usual regexps).
* such regexps hard to write - bcs you always need to handle corner-cases by hands (brackets in string constants, brackets/spaces/... in identifiers in backticks/single-quotes/double-quotes). It's never possible to do correctly by hands. In this particular case
* such regexps very hard to read & maintain

in this particular regexp there are problems:
* can't match parentheses arbitrary depth - it supports only depth==2.
* to support string constants into DDL - regexp should be fully rewritten, bcs unbalanced parentheses in string will fail match
* it took one day to understood what is `\s*\((?:[^)(]+|\((?:[^)(]+|\([^)(]*\))*\))*\)\s*` part - it's very curious approach to parse columns description

there reasons makes such regexps error-prone


## now

With package token_regexp you declaratively describe regexp-like query, which is matching on tokens

```go
queryFull := []any{
	"create",
	op.Or("table", op.Seq("materialized", "view")),
	op.Opt(op.Seq("if", "not", "exists")),
	op.Seq(op.Opt(op.Seq(op.AnyToken(), ".")), op.AnyToken()), // tableIdentifier
	op.Opt(op.Seq("uuid", op.AnyToken())),
	op.CapturingGroup(
		op.Opt(op.Seq("on", "cluster", op.Opt(op.AnyToken()))),
	),
	op.MatchParentheses(),
	"engine",
	"=",
	op.CapturingGroup(
		op.AnyToken(),
		op.Opt(op.MatchParentheses()),
	),
}

tokens := clickhouse_lexer.StringToTokens(createDdlSQL)
currMatcher := token_regexp.NewTokenRegexp(queryFull)
results := currMatcher.FindAll(tokens)
```

it has next advantages:
* it can match grammars
* it's easy to read
* it's easy to write
* you don't need to think about corner-cases (brackets in string constants, brackets/spaces/... in identifiers in backticks/single-quotes/double-quotes)


## implementation details

Implementation of execution this declarative pattern - absolutely like classical regexps. Regexp - like program, which is interpreted by matching virtual machine. That's why every pattern item called 'op' - like 'operation' (instruction) for virtual machine. And every instruction executor implemented into `.Consume*` method of op.

For now, everything goes to lower case and works case-independent. It's easy to change this behaviour if needed.

Ops can be:
* primitive (op.Match, op.MatchNot, op.MatchParentheses, op.AnyToken) - they just match to some tokens
* complex (op.CapturingGroup, op.Opt, op.Or, op.Seq) - they are used to group another ops, and not matched tokens. To extract matched tokens - use CapturingGroup.


## supported operations

* op.Match (also string const - will be wrapped into op.Match) - mandatory matches token with same text
* op.MatchNot - mandatory matches token with not equal text
* op.AnyToken - matches any one token
* op.Opt - (opt - means 'optional') matches either one token with same text or 0 tokens (like in classical regex char with optional quantifier)
* op.Seq - (seq - means 'sequence' of ops) filled by sequence of ops, and matches all of them. For example: `op.Seq("a", op.AnyToken, op.Opt("b"))` will match strings: `a`, `a b`, `a z b`, `a z`. Query, passed into `NewTokenRegexp` - wraps into op.Seq.
* op.Or - matches one of several cases. For example: `op.Or("a", "b")` or `op.Or("a", op.AnyToken())`
* op.MatchParentheses - matches for opened parentheses balanced closed parentheses.
* op.CapturingGroup - just as 'capturing group' in classical regexp, allows to extract part of string, which is matched on wrapped in op.CapturingGroup template.

Also see tests for every operation - it's quite clear

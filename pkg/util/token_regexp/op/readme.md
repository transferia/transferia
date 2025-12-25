## operations

* op.Match (also string const - will be wrapped into op.Match) - mandatory matches token with same text
* op.MatchNot - mandatory matches token with not equal text
* op.AnyToken - matches any one token
* op.Opt - (opt - means 'optional') matches either one token with same text or 0 tokens (like in classical regex char with optional quantifier)
* op.Seq - (seq - means 'sequence' of ops) filled by sequence of ops, and matches all of them. For example: `op.Seq("a", op.AnyToken, op.Opt("b"))` will match strings: `a`, `a b`, `a z b`, `a z`. Query, passed into `NewTokenRegexp` - wraps into op.Seq.
* op.Or - matches one of several cases. For example: `op.Or("a", "b")` or `op.Or("a", op.AnyToken())`
* op.MatchParentheses - matches for opened parentheses balanced closed parentheses.
* op.CapturingGroup - just as 'capturing group' in classical regexp, allows to extract part of string, which is matched on wrapped in op.CapturingGroup template.

Also see tests for every operation - it's quite clear

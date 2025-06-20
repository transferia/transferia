//go:build !disable_ydb_provider

package decimal

import (
	"fmt"
	"math/big"
)

var (
	ten  = big.NewInt(10)
	zero = big.NewInt(0)
	one  = big.NewInt(1)
	inf  = big.NewInt(0).Mul(
		big.NewInt(100000000000000000),
		big.NewInt(1000000000000000000),
	)
	nan    = big.NewInt(0).Add(inf, one)
	err    = big.NewInt(0).Add(nan, one)
	neginf = big.NewInt(0).Neg(inf)
	negnan = big.NewInt(0).Neg(nan)
)

var ErrSyntax = fmt.Errorf("invalid syntax")

type ParseError struct {
	Err   error
	Input string
}

func (p *ParseError) Error() string {
	return fmt.Sprintf(
		"decimal: parse %q: %v", p.Input, p.Err,
	)
}

func (p *ParseError) Unwrap() error {
	return p.Err
}

func syntaxError(s string) *ParseError {
	return &ParseError{
		Err:   ErrSyntax,
		Input: s,
	}
}

func precisionError(s string, precision, scale uint32) *ParseError {
	return &ParseError{
		Err:   fmt.Errorf("invalid precision/scale: %d/%d", precision, scale),
		Input: s,
	}
}

// IsInf reports whether x is an infinity.
func IsInf(x *big.Int) bool { return x.CmpAbs(inf) == 0 }

// IsNaN reports whether x is a "not-a-number" value.
func IsNaN(x *big.Int) bool { return x.CmpAbs(nan) == 0 }

// IsErr reports whether x is an "error" value.
func IsErr(x *big.Int) bool { return x.Cmp(err) == 0 }

// Parse interprets a string s with the given precision and scale and returns
// the corresponding big integer.
//
//	had to copy this function, since it's internal in ydb-driver
//	see here: https://github.com/ydb-platform/ydb-go-sdk/issues/1435
func Parse(s string, precision, scale uint32) (*big.Int, error) {
	if scale > precision {
		//nolint:descriptiveerrors
		return nil, precisionError(s, precision, scale)
	}

	v := big.NewInt(0)
	if s == "" {
		return v, nil
	}

	neg := s[0] == '-'
	if neg || s[0] == '+' {
		s = s[1:]
	}
	if isInf(s) {
		if neg {
			return v.Set(neginf), nil
		}
		return v.Set(inf), nil
	}
	if isNaN(s) {
		if neg {
			return v.Set(negnan), nil
		}
		return v.Set(nan), nil
	}

	integral := precision - scale

	var dot bool
	for ; len(s) > 0; s = s[1:] {
		c := s[0]
		if c == '.' {
			if dot {
				//nolint:descriptiveerrors
				return nil, syntaxError(s)
			}
			dot = true
			continue
		}
		if dot {
			if scale > 0 {
				scale--
			} else {
				break
			}
		}

		if !isDigit(c) {
			//nolint:descriptiveerrors
			return nil, syntaxError(s)
		}

		v.Mul(v, ten)
		v.Add(v, big.NewInt(int64(c-'0')))

		if !dot && v.Cmp(zero) > 0 && integral == 0 {
			if neg {
				return neginf, nil
			}
			return inf, nil
		}
		integral--
	}
	if len(s) > 0 { // Characters remaining.
		c := s[0]
		if !isDigit(c) {
			//nolint:descriptiveerrors
			return nil, syntaxError(s)
		}
		plus := c > '5'
		if !plus && c == '5' {
			var x big.Int
			plus = x.And(v, one).Cmp(zero) != 0 // Last digit is not a zero.
			for !plus && len(s) > 1 {
				s = s[1:]
				c := s[0]
				if !isDigit(c) {
					//nolint:descriptiveerrors
					return nil, syntaxError(s)
				}
				plus = c != '0'
			}
		}
		if plus {
			v.Add(v, one)
			if v.Cmp(pow(ten, precision)) >= 0 {
				v.Set(inf)
			}
		}
	}
	v.Mul(v, pow(ten, scale))
	if neg {
		v.Neg(v)
	}
	return v, nil
}

func isInf(s string) bool {
	return len(s) >= 3 && (s[0] == 'i' || s[0] == 'I') && (s[1] == 'n' || s[1] == 'N') && (s[2] == 'f' || s[2] == 'F')
}

func isNaN(s string) bool {
	return len(s) >= 3 && (s[0] == 'n' || s[0] == 'N') && (s[1] == 'a' || s[1] == 'A') && (s[2] == 'n' || s[2] == 'N')
}

func isDigit(c byte) bool {
	return '0' <= c && c <= '9'
}

// pow returns new instance of big.Int equal to x^n.
func pow(x *big.Int, n uint32) *big.Int {
	var (
		v = big.NewInt(1)
		m = big.NewInt(0).Set(x)
	)
	for n > 0 {
		if n&1 != 0 {
			v.Mul(v, m)
		}
		n >>= 1
		m.Mul(m, m)
	}
	return v
}

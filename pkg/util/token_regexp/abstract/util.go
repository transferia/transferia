package abstract

import "math"

// tokensMinMaxPos - returns minPos & maxPos & is_found
//
// remember, that 'pos' here - it's number of rune in source text. To extract them - need to use something like golang.org/x/exp/utf8string or substringByRuneRange
func tokensMinMaxPos(tokens []*Token) (int, int, bool) {
	var currMin = math.MaxInt
	var currMax = math.MinInt

	for _, currToken := range tokens {
		if currToken.AntlrToken.GetStart() < currMin {
			currMin = currToken.AntlrToken.GetStart()
		}
		if currToken.AntlrToken.GetStop() > currMax {
			currMax = currToken.AntlrToken.GetStop()
		}
	}

	return currMin, currMax, currMin != math.MaxInt && currMax != math.MinInt
}

// tokensMinMaxPosArr - returns minPos & maxPos & is_found
//
// remember, that 'pos' here - it's number of rune in source text. To extract them - need to use something like golang.org/x/exp/utf8string or substringByRuneRange
func tokensMinMaxPosArr(in []*MatchedOp) (int, int, bool) {
	var currMin = math.MaxInt
	var currMax = math.MinInt

	for _, currToken := range in {
		newMin, newMax, isFound := tokensMinMaxPos(currToken.tokens)
		if !isFound {
			continue
		}
		if newMin < currMin {
			currMin = newMin
		}
		if newMax > currMax {
			currMax = newMax
		}
	}
	return currMin, currMax, currMin != math.MaxInt && currMax != math.MinInt
}

func ResolveMatchedOps(originalStr string, in []*MatchedOp) string {
	currMin, currMax, isFound := tokensMinMaxPosArr(in)
	if !isFound {
		return ""
	}
	return substringByRuneRange(originalStr, currMin, currMax)
}

func substringByRuneRange(in string, startRunePos, endRunePos int) string {
	return string([]rune(in)[startRunePos : endRunePos+1])
}

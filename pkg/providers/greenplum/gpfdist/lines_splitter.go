package gpfdist

import "github.com/transferia/transferia/library/go/core/xerrors"

type LinesSplitter struct {
	quotesCnt int
	buffer    []byte
}

// NewLinesSplitter creates splitter that reads input and extracts lines enquoted by " and \n combination.
func NewLinesSplitter() *LinesSplitter {
	// Usage example (pipeline):
	// 1. Do("AB") returns nil;
	// 2. Do("C\nDE") returns ["ABC"];
	// 3. Do("F\nGH\nK\n") returns ["DEF", "GH", "K"].
	return &LinesSplitter{quotesCnt: 0, buffer: nil}
}

// Do returns splitted lines which can reuse same memory as `bytes`,
// so it could be re-used only when returned value not needed.
func (s *LinesSplitter) Do(bytes []byte) [][]byte {
	res := make([][]byte, 0, 50000)
	left := 0
	for right := range bytes {
		if bytes[right] == '"' {
			s.quotesCnt++
			continue
		}
		if bytes[right] != '\n' || s.quotesCnt%2 != 0 {
			continue
		}
		// Found '\n' which is not escaped by '"', flush line.
		var splitted []byte
		if len(s.buffer) > 0 {
			splitted = append(s.buffer, bytes[left:right+1]...)
		} else {
			splitted = bytes[left : right+1]
		}
		if len(splitted) > 0 {
			res = append(res, splitted)
		}
		s.quotesCnt = 0
		s.buffer = nil
		left = right + 1
	}
	s.buffer = append(s.buffer, bytes[left:]...)
	return res
}

// Done validates that no unparsed data left.
func (s *LinesSplitter) Done() error {
	if len(s.buffer) > 0 {
		return xerrors.New("buffer is not empty")
	}
	return nil
}

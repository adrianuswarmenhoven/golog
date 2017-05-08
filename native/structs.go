package native

import (
	"bytes"
	"unicode"

	"github.com/adrianuswarmenhoven/golog"
	"github.com/adrianuswarmenhoven/golog/term"
)

type Marshaler interface {
	MarshalProlog(golog.Machine, term.Term) error
}

type Unmarshaler interface {
	UnmarshalProlog(map[uintptr]term.Term) term.Term
}

func gpName(gName string) string {
	buf := bytes.NewBuffer([]byte{})
	seenLower := false
	for i, c := range gName {
		if i > 0 && unicode.IsUpper(c) && seenLower {
			_, _ = buf.WriteRune('_')
		}
		if unicode.IsLower(c) {
			seenLower = true
		}
		if c == '_' {
			seenLower = false
		}
		_, _ = buf.WriteRune(unicode.ToLower(c))
	}
	return buf.String()
}

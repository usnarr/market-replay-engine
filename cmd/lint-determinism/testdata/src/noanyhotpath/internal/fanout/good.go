package fanout

// Reader is a named interface with methods -- not the empty interface, so
// it is not what this rule bans.
type Reader interface {
	Read() int
}

func consume(r Reader) int {
	return r.Read()
}

func concrete(n int) int {
	return n * 2
}

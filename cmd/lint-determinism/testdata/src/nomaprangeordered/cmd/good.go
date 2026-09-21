// Package cmd is outside the rule's scope: cmd/convert is in it, cmd
// itself is not. Ranging a map here is not flagged.
package cmd

func keys(m map[string]int) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	return out
}

// Package cmd is outside the rule's scope (internal/merge, internal/fanout
// only). An any parameter here is not flagged.
package cmd

func describe(v any) string {
	return fmtSprint(v)
}

func fmtSprint(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

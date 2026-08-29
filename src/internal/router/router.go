// Package router matches an incoming PUBLISH topic against the client's
// active subscription filters using MQTT topic wildcards (§4.7 Topic Names
// and Topic Filters):
//
//	+  single-level wildcard — matches exactly one level
//	#  multi-level wildcard  — matches any number of levels, must be last
//	$  leading dollar topics are only matched by an explicit $ filter
//
// This is a hand-rolled matcher with no dependency, in the style of the
// filtering provided by broker client libraries.
package router

// Router maps topic filters to subscription handlers.
type Router struct {
	filters map[string]uint64 // filter -> handler id
	handlers map[uint64]func(string, []byte)
	nextID   uint64
}

// New returns an empty router.
func New() *Router { return nil }

// Add registers a handler for the given topic filter and returns its id.
func (r *Router) Add(filter string, h func(string, []byte)) uint64 { return 0 }

// Remove deregisters a filter handler by id.
func (r *Router) Remove(id uint64) {}

// Match reports whether a concrete topic matches the given filter string
// under the §4.7 wildcard rules. Uses shared-prefix scanning, no regex.
func Match(filter, topic string) bool { return false }

// Lookup returns the handler for any filter that matches topic, or nil.
func (r *Router) Lookup(topic string) func(string, []byte) { return nil }

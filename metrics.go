package mqredis

// Metrics captures operational counters. Implement it with Prometheus or any
// other metrics backend.
type Metrics interface {
	IncPublished(topic string)
	IncConsumed(topic string)
	IncRetried(topic string)
	IncDLQ(topic string)
	ObservePending(topic string, count int64)
}

type nopMetrics struct{}

func (nopMetrics) IncPublished(string)          {}
func (nopMetrics) IncConsumed(string)           {}
func (nopMetrics) IncRetried(string)            {}
func (nopMetrics) IncDLQ(string)                {}
func (nopMetrics) ObservePending(string, int64) {}

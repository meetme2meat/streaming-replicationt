package constant

// The key that is mere reflection of id column that exist in postgres
const (
	QueryKey = "pgx_id"
)

// constant for NATS

const (
	NatURL        = "nats://0.0.0.0:4222"
	ClusterID     = "Cluster1"
	ConsumerID    = "consumer"
	CdrConsumerID = "cdr_consumer"
	Subject       = "default"
	DurableName   = "DurableQ"
)

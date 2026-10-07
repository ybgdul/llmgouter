package metrics

import (
	"sync"
	"time"
)

type NodeMetrics struct{ 
	NodeID string `json:"node_id"`
	Timestamp time.Time `json:"timestamp"`
	GPU GPUMetrics `json:"gpu"`
	Workload WorkloadStatus `json:"workload"`
	Models ModelStatus `json:"models"`
	mu sync.RWMutex
	cachedPrefixes map[string]time.Time
	maxCacheSize int
}

type GPUMetrics struct{ 
	VRAMTotalBytes int64   `json:"vram_total_bytes"`
	VRAMUsedBytes  int64   `json:"vram_used_bytes"`
	VRAMFreeBytes  int64   `json:"vram_free_bytes"`
	GPUUtilPct     float64 `json:"gpu_util_pct"`
}

type WorkloadStatus struct{ 
	ActiveStreams int64 `json:"active_sse_streams"`
	InFlighTokens int64 `json:"in_flight_tokens"`
	TokensCapacity int64 `json:"max_tokens_capacity"`
	TokensPerSecond float64 `json:"tokens_per_second"`
}

type ModelStatus struct{ 
	Loaded []string `json:"models"`
	Available []string `json:"available"`
}
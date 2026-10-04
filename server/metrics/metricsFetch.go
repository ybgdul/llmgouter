package metrics

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

func FetchMetrics(ctx context.Context, client *http.Client, baseUrl string) (*NodeMetrics, error) { 
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseUrl+"/metrics", nil)
	if err != nil { 
		return nil, fmt.Errorf("fetching metrics has failed: %v", err)
	}

	resp, err := client.Do(req)
	if err != nil { 
		return nil, fmt.Errorf("getting metrics response has failed: %v", err)
	}
	defer resp.Body.Close()

	var metrics NodeMetrics
	if err := json.NewDecoder(resp.Body).Decode(&metrics); err != nil { 
		return nil, fmt.Errorf("cannot parse metrics response: %v", err)
	}
	return &metrics, nil
}

// load balancer helpers:

func (m *NodeMetrics) HasModelLoaded(modelName string) bool {
	if m == nil {
		return false
	}
	for _, loaded := range m.Models.Loaded {
		if loaded == modelName {
			return true
		}
	}
	return false
}

// SupportsModel checks if the node can run the model (loaded OR on disk)
func (m *NodeMetrics) SupportsModel(modelName string) bool {
	if m == nil {
		return false
	}
	if m.HasModelLoaded(modelName) {
		return true
	}
	for _, avail := range m.Models.Available {
		if avail == modelName {
			return true
		}
	}
	return false
}

// VRAMUsageRatio returns a value between 0.0 and 1.0 representing GPU memory pressure
func (m *NodeMetrics) VRAMUsageRatio() float64 {
	if m == nil || m.GPU.VRAMTotalBytes == 0 {
		return 1.0 // Assume full memory pressure if unknown
	}
	return float64(m.GPU.VRAMUsedBytes) / float64(m.GPU.VRAMTotalBytes)
}

// CanAcceptTokenLoad checks if adding newTokens would exceed maximum token capacity
func (m *NodeMetrics) CanAcceptTokenLoad(newTokens int64) bool {
	if m == nil {
		return false
	}
	return (m.Workload.InFlighTokens + newTokens) <= m.Workload.TokensCapacity
}

// CalculateScore evaluates node suitability for a request (higher score = better pick)
func (m *NodeMetrics) CalculateScore(requestedModel string, estimatedTokens int64) float64 {
	if m == nil || !m.SupportsModel(requestedModel) || !m.CanAcceptTokenLoad(estimatedTokens) {
		return -1.0 // Ineligible
	}

	score := 100.0

	// 1. Massive bonus for hot/warm VRAM state (avoids model unloading delay)
	if m.HasModelLoaded(requestedModel) {
		score += 500.0
	}

	// 2. Penalize VRAM memory pressure
	vramPenalty := m.VRAMUsageRatio() * 50.0
	score -= vramPenalty

	// 3. Reward high Tokens-Per-Second throughput
	score += (m.Workload.TokensPerSecond * 2.0)

	// 4. Penalize active stream congestion
	score -= float64(m.Workload.ActiveStreams) * 10.0

	return score
}
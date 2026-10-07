package cluster

import (
	"context"
	"llmgouter/config"
	"llmgouter/server/metrics"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

type ManagedNode struct{ 
	ID string 
	BaseURL string 
	IsHealthy atomic.Bool
	mu sync.RWMutex
	LatestMetrics *metrics.NodeMetrics
	InFlightTokens int64
	ActiveStreams int64
}

type ClusterManager struct {
	nodes map[string]*ManagedNode
	httpClient *http.Client
	pollTimeout time.Duration
}

func NewClusterManager(config *config.Config) *ClusterManager { 
	client := &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns: 100,
		},
	}

	cm := &ClusterManager{
		nodes: make(map[string]*ManagedNode),
		httpClient: client,
		pollTimeout: 3 * time.Second,
	}

	for _, b := range config.Backends { 
		node := &ManagedNode{
			ID: b.ID,
			BaseURL: b.URL,
		}
		node.IsHealthy.Store(true)
		cm.nodes[b.ID] = node
	}

	return cm
}

func (cm *ClusterManager) StartBackgroundPoll(ctx context.Context) { 
	log.Println("Starting Cluster Manager Polling")

	go func() { 
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for { 
			select {
			case <-ctx.Done(): 
				return
			case <-ticker.C:
				cm.probeAllHealth(ctx)
			}
		}

	}()

	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for { 
			select { 
			case <-ctx.Done():
				return 
			case <-ticker.C:
				cm.fetchAllMetrics(ctx)
			}
		}
	}()
}

func (cm *ClusterManager) probeAllHealth(ctx context.Context) { 
	for _, n := range cm.nodes { 
		n.mu.Lock()
		defer n.mu.Unlock()
		var healthResp metrics.HealthResp
		healthResp = metrics.CheckHealth(ctx, cm.httpClient, n.BaseURL)
		if healthResp.Status != metrics.StatusHealthy {
			n.IsHealthy.Store(true)
		} else { 
			n.IsHealthy.Store(false)
		}
	}
}

func (cm *ClusterManager) fetchAllMetrics(ctx context.Context) { 
	for _, node := range cm.nodes { 
		if !node.IsHealthy.Load() { 
			continue 
		}

		go func(n *ManagedNode) { 
			reqCtx, cancel := context.WithTimeout(ctx, cm.pollTimeout)
			defer cancel()

			m, err := metrics.FetchMetrics(reqCtx, cm.httpClient, n.BaseURL)
			if err != nil { 
				return
			}

			n.mu.Lock()
			n.LatestMetrics = m
			n.mu.Unlock()
		}(node)
	}
}

func (cm *ClusterManager) MarkUnhealthy(nodeId string) { 
	if node, exists := cm.nodes[nodeId]; exists { 
		if node.IsHealthy.Swap(false) {
		}
	}
}

func (cm *ClusterManager) GetHealthyNodes() []*ManagedNode { 
	var healthy []*ManagedNode
	for _, n := range cm.nodes { 
		if n.IsHealthy.Load() { 
			healthy = append(healthy, n)
		}
	}
	return healthy 
}




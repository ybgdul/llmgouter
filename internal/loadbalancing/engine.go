package loadbalancing

import (
	"context"
	"fmt"
	"llmgouter/internal/cluster"
	"llmgouter/internal/sessions"
	"llmgouter/internal/tokenizers"
	"sync/atomic"
	"time"
)

const ttl = 30 * time.Second

type LoadBalancer struct{ 
	tokenizer *tokenizers.TokenizerEngine
	manager *cluster.ClusterManager
	session *sessions.SessionManager
}


func NewLoadBalancer( tokenizer *tokenizers.TokenizerEngine, manager *cluster.ClusterManager, session *sessions.SessionManager) *LoadBalancer { 
	return &LoadBalancer{
		tokenizer: tokenizer,
		manager: manager,
		session: session,
	}
}

func (lb *LoadBalancer) LoadBalance(ctx context.Context, model string, prompt string, sessionId string) (*cluster.ManagedNode, func(), error) { 
	tokens := int64(lb.tokenizer.CountTokens(prompt))
	prefixHash := lb.tokenizer.ExtractPrefixHash(prompt, 128)

	if targetNodeID, found := lb.session.GetNodeForSession(sessionId); found { 
		if node, exists := lb.manager.GetNodeByID(targetNodeID); exists && node.IsHealthy.Load() {
            node.RLockMetrics()
            metrics := node.LatestMetrics
        
            if metrics != nil && metrics.SupportsModel(model) {
                inFlight := atomic.LoadInt64(&node.InFlightTokens)
                if (inFlight + tokens) <= metrics.Workload.TokensCapacity {
                    node.RUnlockMetrics()

                    atomic.AddInt64(&node.ActiveStreams, 1)
                    atomic.AddInt64(&node.InFlightTokens, tokens)

                    releaseFunc := func() {
                        atomic.AddInt64(&node.ActiveStreams, -1)
                        atomic.AddInt64(&node.InFlightTokens, -tokens)
                    }
                    return node, releaseFunc, nil
                }
            }
            node.RUnlockMetrics()
        }
        lb.session.UnbindSession(sessionId)
	}


	healthyNodes := lb.manager.GetHealthyNodes()
	if len(healthyNodes) == 0 { 
		return nil, nil, fmt.Errorf("no healthy worker available")
	}

	var bestNode *cluster.ManagedNode = nil
	highestScore := -1e9


	for _, node := range healthyNodes { 

		node.RLockMetrics()	
		metrics := node.LatestMetrics

		if metrics == nil {
			node.RUnlockMetrics()
			go lb.manager.RefreshNodeMetrics(ctx, node)
			continue
		}

		if node.IsMetricsStale(ttl) { 
			go lb.manager.RefreshNodeMetrics(ctx, node)
		}

		if !node.IsHealthy.Load() || !node.LatestMetrics.SupportsModel(model) {
			node.RUnlockMetrics()
			continue
		}
		
		inFlight := atomic.LoadInt64(&node.InFlightTokens)
		if (inFlight + tokens) > node.LatestMetrics.Workload.TokensCapacity {
			node.RUnlockMetrics()
			continue
		}

		score := node.LatestMetrics.CalculateScore(model, tokens, prefixHash)
		node.RUnlockMetrics()

		score -= float64(atomic.LoadInt64(&node.ActiveStreams)) * 20.0 

		if score > highestScore { 
			highestScore = score 
			bestNode = node
		}
	}

	if bestNode == nil { 
		return nil, nil, fmt.Errorf("no eligible worker for given prompt has been found")
	}

	atomic.AddInt64(&bestNode.ActiveStreams, 1)
	atomic.AddInt64(&bestNode.InFlightTokens, tokens)

	releaseFunc := func() { 
		atomic.AddInt64(&bestNode.ActiveStreams, -1)
		atomic.AddInt64(&bestNode.InFlightTokens, -tokens)
	}

	return bestNode, releaseFunc, nil
}

package loadbalancing

import (
	"fmt"
	"llmgouter/internal/cluster"
	"llmgouter/internal/tokenizers"
	"sync/atomic"
)

type LoadBalancer struct{ 
	tokenizer *tokenizers.TokenizerEngine
	manager *cluster.ClusterManager
}


func NewLoadBalancer( tokenizer *tokenizers.TokenizerEngine, manager *cluster.ClusterManager) *LoadBalancer { 
	return &LoadBalancer{
		tokenizer: tokenizer,
		manager: manager,
	}
}

func (lb *LoadBalancer) LoadBalance(model string, prompt string) (*cluster.ManagedNode, func(), error) { 
	tokens := int64(lb.tokenizer.CountTokens(prompt))
	prefixHash := lb.tokenizer.ExtractPrefixHash(prompt, 128)

	var bestNode *cluster.ManagedNode = nil
	highestScore := -1e9


	for _, node := range lb.manager.GetHealthyNodes() { 
		if !node.IsHealthy.Load() || !node.LatestMetrics.SupportsModel(model) {
			continue
		}
		
		inFlight := atomic.LoadInt64(&node.InFlightTokens)
		if (inFlight + tokens) > node.LatestMetrics.Workload.TokensCapacity {
			continue
		}

		score := node.LatestMetrics.CalculateScore(model, tokens, prefixHash)

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
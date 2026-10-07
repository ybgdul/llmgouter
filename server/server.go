package server

import (
	"context"
	"fmt"
	"llmgouter/config"
	"llmgouter/internal/cluster"
	"llmgouter/internal/loadbalancing"
	"llmgouter/internal/tokenizers"
	"net/http"
	"net/url"
)

func Run() error { 
	config, err := config.NewConfig()
	if err != nil { 
		return fmt.Errorf("failed to load server configurations: %v", err)
	}

	manager := cluster.NewClusterManager(config)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	manager.StartBackgroundPoll(ctx)

	tk, err := tokenizers.NewEngine()
	if err != nil { 
		return fmt.Errorf("failed to load tokenizer: %v", err)
	}
	defer tk.Tokenizer.Close()

	lb := loadbalancing.NewLoadBalancer(tk, manager)

	mux := http.NewServeMux() 

	mux.HandleFunc("/ping", lb)


	return nil
}
package main

import (
	"context"
	"fmt"
	"llmgouter/config"
	"llmgouter/internal/cluster"
	"llmgouter/internal/loadbalancing"
	"llmgouter/internal/ratelimiting"
	"llmgouter/internal/sessions"
	"llmgouter/internal/tokenizers"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)
 
func main() { 
	config, err := config.NewConfig()
	if err != nil { 
		log.Fatalf("failed to load config: %v", err)
	}

	tk, err := tokenizers.NewEngine()
	if err != nil { 
		log.Fatalf("failed to load tokenizer: %v", err)
	} else { 
		defer tk.Tokenizer.Close()
	}

	manager := cluster.NewClusterManager(config)

	rateManager := ratelimiting.NewRateManager(60.0, 10.0, 10*time.Minute)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	go rateManager.CleanUpStart(ctx, 5*time.Minute)
	
	manager.StartBackgroundPoll(ctx)

	session := sessions.NewSessionManager(5*time.Minute)
	go session.StartSessionCleanup(ctx, 2 *time.Minute)

	lb := loadbalancing.NewLoadBalancer(tk, manager, session)

	mux := http.NewServeMux() 

	corePipeline := rateManager.Middleware(lb.TokenizeMiddleware(lb))
	mux.HandleFunc("/healthz", manager.HandleClusterHealthz)
	mux.Handle("/v1/chat/completions", corePipeline)

	serverAddr := fmt.Sprintf(":%d", config.Server.Port)
	server := &http.Server{
		Addr: serverAddr,
		Handler: mux,
		ReadTimeout: 120 * time.Second,
		WriteTimeout: 0,
		IdleTimeout: 120 * time.Second,
	}

	go func() {
		log.Printf("[PROXY RUNNING] LLM Gateway active on http://localhost%s", serverAddr)
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatalf("server forced to shut down: %v", err)
		}
	}()

	<-ctx.Done()
	log.Printf("[PROXY SHUTTING] LLM Gateway shutting down on http://localhost%s gracefully...", serverAddr)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Fatalf("server forced shutdown error: %v", err)
	}

	log.Println("[PROXY SHUTTING] Server stopped cleanly")
}
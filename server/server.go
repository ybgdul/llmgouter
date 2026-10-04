package server

import (
	"fmt"
	"llmgouter/config"
	"llmgouter/internal/tokenizers"
	"net/http"
	//"net/url"
)

func Run() error { 
	_, err := config.NewConfig()
	if err != nil { 
		return fmt.Errorf("failed to load server configurations: %v", err)
	}

	tk, err := tokenizers.NewEngine()
	if err != nil { 
		return fmt.Errorf("failed to load tokenizer: %v", err)
	}
	defer tk.Tokenizer.Close()

	mux := http.NewServeMux()

	mux.HandleFunc("/ping", ping)

	// for _, resource := range config.Resources {
	// 	url, _ := url.Parse(resource.DestinationUrl)
	// 	proxy := NewProxy(url)
	// 	mux.HandleFunc(resource.Endpoint, ProxyRequestHandler(proxy, url, resource.Endpoint))
	// }

	// if err := http.ListenAndServe(config.Server.Host+":"+config.Server.Port, mux); err != nil { 
	// 	return fmt.Errorf("Could not start the server: %v", err)
	// }

	return nil
}
package loadbalancing

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
)

type contextKey string 

const ReqInfoKey contextKey = "reqInfo"

type RequestInfo struct{ 
	Model string 
	Prompt string 
	BodyBytes []byte
}

type ChatPayload struct{ 
	Model string `json:"model"`
	Messages []Message `json:"messages"`
}

type Message struct{ 
	Role string `json:"role"`
	Content string `json:"content"`
}

func (lb *LoadBalancer) TokenizeMiddleware(next http.Handler) http.Handler { 
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { 
		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil { 
			http.Error(w,"failed to read request body", http.StatusBadRequest)
			return 
		}
		r.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))

		var payload ChatPayload
		if err := json.Unmarshal(bodyBytes, &payload); err != nil { 
			http.Error(w,"invalid JSON payload", http.StatusBadRequest)
			return
		}

		var fullPrompt string
		for _, msg := range payload.Messages { 
			fullPrompt += msg.Content + "\n"
		}

		ctx := context.WithValue(r.Context(), ReqInfoKey, &RequestInfo{
			Model: payload.Model,
			Prompt: fullPrompt,
			BodyBytes: bodyBytes,
		})

		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (lb *LoadBalancer) ServeHTTP(w http.ResponseWriter, r *http.Request) { 
	reqInfo, ok := r.Context().Value(ReqInfoKey).(*RequestInfo)
	if !ok { 
		http.Error(w, "internal context error", http.StatusInternalServerError)
		return
	}

	targetNode, releaseFunc, err := lb.LoadBalance(r.Context(), reqInfo.Model, reqInfo.Prompt)
	if err != nil { 
		log.Printf("[PROXY ERROR] %v", err)
		http.Error(w, fmt.Sprintf("service unavailable: %v", err), http.StatusServiceUnavailable)
		return
	}

	defer releaseFunc()

	parsedTargetURL, err := url.Parse(targetNode.BaseURL)
	if err != nil { 
		http.Error(w,"invalid target backend URL", http.StatusBadRequest)
		return 
	}

	r.Body = io.NopCloser(bytes.NewBuffer(reqInfo.BodyBytes))

	proxy := &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.Out.URL.Scheme = parsedTargetURL.Scheme	
			pr.Out.URL.Host = parsedTargetURL.Host
			pr.Out.URL.Path = r.URL.Path
			pr.Out.Host = parsedTargetURL.Host

			if reqInfoKeyVal := r.Header.Get("Authorization"); reqInfoKeyVal != "" {
				pr.Out.Header.Set("Authorization", reqInfoKeyVal)
			}
		},
		ModifyResponse: func(resp *http.Response) error {
			targetNode.UpdateFromTelemetry(resp.Header)
			return nil
		},
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			log.Printf("[PROXY FAILURE] Connection to node %s dropped: %v", targetNode.ID, err)
			lb.manager.MarkUnhealthy(targetNode.ID)
			http.Error(w, "Bad Gateway: Selected GPU worker node disconnected", http.StatusBadGateway)
		},
	}
	proxy.ServeHTTP(w, r)
}
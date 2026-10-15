package ratelimiting

import "net/http"

func (rm *RateManager) Middleware(next http.Handler) http.Handler { 
	return http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			clientIP := ExtractIp(r)
			bucket := rm.GetBucket(clientIP)

			if !bucket.Allow(){
				w.Header().Set("Retry-After", "1")
				http.Error(w, "Too Many Requests: Rate Limit Exceeded", http.StatusTooManyRequests)
				return
			}

			next.ServeHTTP(w, r)
		},
	)
}
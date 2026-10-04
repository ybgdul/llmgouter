package server

import (
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"time"
)

func NewProxy(target *url.URL) *httputil.ReverseProxy { 
	proxy := httputil.NewSingleHostReverseProxy(target)
	return proxy
}

func ProxyRequestHandler(proxy *httputil.ReverseProxy, url *url.URL, endpoint string) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {

		start := time.Now()

		fmt.Printf("[ PROXY SERVER ] Request received at %s at %s\n", r.URL, time.Now().UTC())
		r.URL.Host = url.Host
		r.URL.Scheme = url.Scheme
		r.Header.Set("X-Forwarded-Host", r.Header.Get("Host"))
		r.Host = url.Host

		path := r.URL.Path
		r.URL.Path = strings.TrimLeft(path, endpoint)

		rw := &responseWriter{
			ResponseWriter: w,
		}
		fmt.Printf("[ PROXY SERVER ]Proxying request to %s at %s\n", r.URL, time.Now().UTC())
		proxy.ServeHTTP(rw, r)
		duration := time.Since(start)

		fmt.Printf("[ PROXY SERVER ]%s %s -> %d (%d bytes, %s)\n", r.Method, r.URL.Path, rw.statusCode, rw.bytesWritten ,duration)
	}
}
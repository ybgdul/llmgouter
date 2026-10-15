package ratelimiting

import (
	"context"
	"net"
	"net/http"
	"sync"
	"time"
)

type Bucket struct { 
	mu sync.Mutex
	IP string 
	Tokens float64
	Capacity float64
	RefillRate float64
	LastRefill time.Time
}

type RateManager struct { 
	buckets sync.Map
	capacity float64
	refillRate float64
	staleTTL time.Duration
}

func (b *Bucket) Allow() bool { 
	b.mu.Lock()
	defer b.mu.Unlock()

	now := time.Now()
	relapsed := now.Sub(b.LastRefill)

	b.Tokens += relapsed.Seconds() * b.RefillRate
	if b.Tokens > b.Capacity { 
		b.Tokens = b.Capacity
	}

	if b.Tokens > 1.0 { 
		b.Tokens -= 1.0
		return true
	}
	return false
}

func (b *Bucket) IsStale(ttl time.Duration) bool { 
	b.mu.Lock()
	defer b.mu.Unlock()
	return time.Since(b.LastRefill) > ttl
}

func NewRateManager(capacity float64, refillRate float64, staleTTL time.Duration) *RateManager { 
	return &RateManager{
		capacity: capacity,
		refillRate: refillRate,
		staleTTL: staleTTL,
	}
}

func (r *RateManager) GetBucket(ip string) *Bucket{
	if bucket, ok := r.buckets.Load(ip); ok { 
		return bucket.(*Bucket)
	} 

	newBucket := &Bucket{
		IP: ip,
		Tokens: r.capacity,
		Capacity: r.capacity,
		RefillRate: r.refillRate,
		LastRefill: time.Now(),
	}

	actualBucket, _ := r.buckets.LoadOrStore(ip, newBucket)
	return actualBucket.(*Bucket)
}

func (r *RateManager) CleanUpStart(ctx context.Context, interval time.Duration) { 
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for { 
		select { 
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.buckets.Range(
				func(key any, value any) bool{
					ip := key.(string)
					bucket := value.(*Bucket)

					if bucket.IsStale(r.staleTTL) { 
						r.buckets.Delete(ip)
					}
					return true
				},
			)
		}
	}
}

func ExtractIp(r *http.Request) string { 
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" { 
		if ip, _, err := net.SplitHostPort(xff); err == nil { 
			return ip
		}
		return xff
	}
	ip, _, _ := net.SplitHostPort(r.RemoteAddr)
	return ip
}
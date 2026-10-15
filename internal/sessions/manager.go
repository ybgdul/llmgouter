package sessions

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"sync"
	"time"
)

type SessionManager struct{ 
	mu sync.RWMutex
	bindings map[string]*Binding
	ttl time.Duration
}

type Binding struct{ 
	NodeID string
	SessionID string 
	LastSeen time.Time
}

func NewSessionManager(ttl time.Duration ) *SessionManager { 
	return &SessionManager{
		bindings: make(map[string]*Binding),
		ttl: ttl,
	}
}

func (sm *SessionManager) GetNodeForSession(sessionId string) (string, bool) { 
	if sessionId== "" { 
		return "", false
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()

	binding, exists := sm.bindings[sessionId]
	if !exists { 
		return "", false
	}

	if time.Since(binding.LastSeen) > sm.ttl { 
		delete(sm.bindings, sessionId)
		return "", false
	}

	return binding.NodeID, false
}

func (sm *SessionManager) BindSession(sessionId string, nodeId string ) {
	if sessionId == "" || nodeId =="" { 
		return
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.bindings[sessionId] = &Binding{
		NodeID: nodeId,
		SessionID: sessionId,
		LastSeen: time.Now(),
	}
}

func (sm *SessionManager) UnbindSession(sessionId string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	delete(sm.bindings, sessionId)
}

func (sm *SessionManager) StartSessionCleanup(ctx context.Context, interval time.Duration) { 
	ticker := time.NewTicker(interval)
	for { 
		select { 
		case <-ctx.Done():
			return
		case <-ticker.C:
			sm.mu.Lock()
			now := time.Now()
			for id, b := range sm.bindings {
				if now.Sub(b.LastSeen) < sm.ttl {
					delete(sm.bindings, id)
				}
			}
			sm.mu.Unlock()
		}
	}
}

func ExtractSessionID(r *http.Request, firstMessage string ) string {
	if sessId := r.Header.Get("X-Session-ID"); sessId != "" { 
		return sessId
	}
	if sessId := r.Header.Get("Conversation-ID"); sessId != "" { 
		return sessId
	}

	if firstMessage != "" { 
		hash := sha256.Sum256([]byte(firstMessage))
		return hex.EncodeToString(hash[:16])
	}
	
	return ""
}
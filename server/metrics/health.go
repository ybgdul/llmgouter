package metrics
import (
	"context"
	"encoding/json"
	"net/http"
)

type HealthStatus string 

const( 
	StatusHealthy HealthStatus = "healthy"
	StatusDegraded HealthStatus = "degraded"
	StatusUnhealthy HealthStatus = "unhealthy"
)

type HealthResp struct{ 
	Status HealthStatus `json:"status"`
	UptimeSec int64 `json:"uptime_seconds"`
}

func CheckHealth(ctx context.Context, client *http.Client, baseUrl string) HealthResp { 
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseUrl+"/healthz", nil)
	if err != nil { 
		return HealthResp{
			Status: StatusUnhealthy,
		}
	}

	resp, err := client.Do(req)
	if err != nil { 
		return HealthResp{
			Status: StatusUnhealthy,
		}
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK { 
		return HealthResp{
			Status: StatusUnhealthy,
		}
	}

	var res HealthResp
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil { 
		return HealthResp{
			Status: StatusHealthy,
			UptimeSec: 0,
		}
	}

	return res
}
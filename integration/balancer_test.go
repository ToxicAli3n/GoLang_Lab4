package integration

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"
)

const baseAddress = "http://balancer:8090"

var client = &http.Client{
	Timeout: 3 * time.Second,
}

type RespBody struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func TestBalancer(t *testing.T) {
	if !isIntegrationTestEnabled() {
		t.Skip("Integration test is not enabled")
	}

	if !isBalancerAvailable() {
		t.Skip("Balancer is not available")
	}

	teamName := "megadreamteam"

	checkResponseBody(t, teamName)
}

func isIntegrationTestEnabled() bool {
	_, exists := os.LookupEnv("INTEGRATION_TEST")
	return exists
}

func isBalancerAvailable() bool {
	resp, err := client.Get(baseAddress)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

func checkResponseBody(t *testing.T, key string) {
	addr := fmt.Sprintf("%s/api/v1/some-data?key=%s", baseAddress, key)
	resp, err := client.Get(addr)
	if err != nil {
		t.Fatalf("Failed to get response from balancer: %v", err)
		return
	}
	defer resp.Body.Close()

	var body RespBody
	err = json.NewDecoder(resp.Body).Decode(&body)
	if err != nil {
		t.Fatalf("Failed to decode response body: %v", err)
		return
	}

	if body.Key != key {
		t.Errorf("Expected key %s, got %s", key, body.Key)
	}

	if body.Value == "" {
		t.Errorf("Expected non-empty value for key %s", key)
	}
}

func BenchmarkBalancer(b *testing.B) {
	for i := 0; i < b.N; i++ {
		resp, err := client.Get(fmt.Sprintf("%s/api/v1/some-data", baseAddress))
		if err != nil {
			b.Errorf("Request failed: %v", err)
		}
		if resp != nil {
			resp.Body.Close()
		}
	}
}

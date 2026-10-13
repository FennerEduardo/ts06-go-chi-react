package httpapi

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"strings"
	"time"
)

// TenantClaim is the JWT claim that carries the tenant. The tenant never comes from a header such as
// X-Tenant-Id, and there is no default tenant.
const TenantClaim = "tenant_id"

// TenantFrom returns the tenant claim of a valid HS256 bearer token (signature, exp, nbf checked), or "".
func TenantFrom(authorization, secret string) string {
	if secret == "" || len(authorization) < 7 || !strings.EqualFold(authorization[:7], "Bearer ") {
		return ""
	}
	parts := strings.Split(strings.TrimSpace(authorization[7:]), ".")
	if len(parts) != 3 {
		return ""
	}
	var header struct {
		Alg string `json:"alg"`
	}
	if raw, err := base64.RawURLEncoding.DecodeString(parts[0]); err != nil || json.Unmarshal(raw, &header) != nil || header.Alg != "HS256" {
		return ""
	}
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(parts[0] + "." + parts[1]))
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil || !hmac.Equal(sig, mac.Sum(nil)) {
		return ""
	}
	var claims map[string]any
	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil || json.Unmarshal(raw, &claims) != nil {
		return ""
	}
	now := float64(time.Now().Unix())
	if exp, ok := claims["exp"].(float64); ok && now >= exp {
		return ""
	}
	if nbf, ok := claims["nbf"].(float64); ok && now < nbf {
		return ""
	}
	tenant, _ := claims[TenantClaim].(string)
	return strings.TrimSpace(tenant)
}

// SignJWT signs an HS256 token (tests and local tooling; production tokens come from your identity provider).
func SignJWT(claims map[string]any, secret string) string {
	head := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))
	body, _ := json.Marshal(claims)
	payload := head + "." + base64.RawURLEncoding.EncodeToString(body)
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(payload))
	return payload + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

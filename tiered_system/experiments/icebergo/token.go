package main

// SciToken minting for the /sage Pelican namespace, in Go.
//
// This is a port of experiments/pelican_token.py, which in turn mirrors
// waggle-auth-app/downloads/views.py:120-168. Same claims, same ES256 key,
// same `kid`, same backdating hack. Done natively rather than shelling out
// to Python so the experiment measures one language's client end to end --
// a Go exporter would mint its own tokens too.
//
// The key signs for the ENTIRE /sage namespace. Every token minted here is
// therefore scoped to a single path prefix and given a short lifetime, and
// no token or key material is ever written to disk by this program.

import (
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

const (
	issuer   = "https://sagecontinuum.org"
	keyID    = "3cfa"
	rootURL  = "https://nrdstor.nationalresearchplatform.org:8443/sage"
	tokenLag = 60 // backdate nbf/iat; views.py:158-168
)

func defaultKeyPath() string {
	// icebergo -> experiments -> tiered_system -> sage-data-rework -> workspace
	wd, _ := os.Getwd()
	return filepath.Join(wd, "..", "..", "..", "..",
		"honeyhouse-config/applications/waggle-access/pelican/issuer-key.pem")
}

func loadKey(path string) (*ecdsa.PrivateKey, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	blk, _ := pem.Decode(raw)
	if blk == nil {
		return nil, fmt.Errorf("no PEM block in %s", path)
	}
	// The SAGE issuer key is SEC1 ("EC PRIVATE KEY"); accept PKCS#8 too in
	// case it is ever rotated to that form.
	if k, err := x509.ParseECPrivateKey(blk.Bytes); err == nil {
		return k, nil
	}
	any, err := x509.ParsePKCS8PrivateKey(blk.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	k, ok := any.(*ecdsa.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("%s is not an EC key", path)
	}
	return k, nil
}

func b64(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }

// mint returns a serialized SciToken for `scope`, e.g. "read:/p write:/p".
func mint(key *ecdsa.PrivateKey, scope string, lifetime time.Duration) (string, error) {
	now := time.Now().Unix()
	hdr, err := json.Marshal(map[string]string{
		"alg": "ES256", "typ": "JWT", "kid": keyID,
	})
	if err != nil {
		return "", err
	}
	claims, err := json.Marshal(map[string]any{
		"sub":   "cold-storage-test",
		"aud":   "ANY",
		"ver":   "scitoken:2.0",
		"scope": scope,
		"iss":   issuer,
		"iat":   now - tokenLag,
		"nbf":   now - tokenLag,
		"exp":   now + int64(lifetime.Seconds()),
		"jti":   fmt.Sprintf("cst-go-%d", now),
	})
	if err != nil {
		return "", err
	}

	signing := b64(hdr) + "." + b64(claims)
	sum := sha256.Sum256([]byte(signing))
	r, s, err := ecdsa.Sign(rand.Reader, key, sum[:])
	if err != nil {
		return "", err
	}

	// JWS wants fixed-width R||S, not the ASN.1 sequence crypto/ecdsa
	// returns from its other entry points. P-256 => 32 bytes each.
	// Left-pad: a short big.Int would otherwise shift the halves and
	// produce a signature that verifies nowhere.
	sig := make([]byte, 64)
	r.FillBytes(sig[:32])
	s.FillBytes(sig[32:])

	return signing + "." + b64(sig), nil
}

"""
Mint SciTokens for the SAGE /sage Pelican namespace.

This mirrors `waggle-auth-app/downloads/views.py:120-168` exactly -- same
claims, same algorithm, same key id, same "not before" lag hack -- but
without the Django settings and the `scitokens` package (which is not
installed here). A SciToken is a JWT with a prescribed claim set, so pyjwt
is sufficient and makes the claims explicit enough to audit.

The one deliberate difference from the production path: production sets
`scope = read:<path>` only. Write tests need `write:<path>` too, so `scope`
is a parameter here.

Key material comes from `issuer-key.pem` in honeyhouse-config. That key
signs for the ENTIRE /sage namespace, so every token minted here is scoped
to a single path and given a short lifetime.
"""
import os
import time
from pathlib import Path

import jwt

# Matches honeyhouse-config/applications/waggle-access/kustomization.yaml:47-53
ISSUER = "https://sagecontinuum.org"
KEY_ID = "3cfa"
ALGORITHM = "ES256"
ROOT_URL = "https://nrdstor.nationalresearchplatform.org:8443/sage"

# parents[4] is the checkout root: tools -> demo ->
# tiered_system -> sage-data-rework -> workspace. A container has no
# checkout above it; PELICAN_KEY_PATH overrides the path there.
DEFAULT_KEY_PATH = Path(os.environ.get("PELICAN_KEY_PATH") or (
    Path(__file__).resolve().parents[4]
    / "honeyhouse-config/applications/waggle-access/pelican/issuer-key.pem"
))


def mint(scope, lifetime=600, lag=60, key_path=None, subject="cold-storage-test"):
    """Return a serialized SciToken string for `scope`.

    `scope` is a SciTokens scope string, e.g. "read:/path" or
    "read:/path write:/path".

    `lag` backdates both `nbf` and `iat`, reproducing the production hack at
    views.py:158-168: Pelican may see a token before our clock agrees it is
    valid. Production uses lag=60 and extends the lifetime by the same
    amount so the usable window is unchanged.
    """
    key = Path(key_path or DEFAULT_KEY_PATH).read_bytes()
    now = int(time.time())
    claims = {
        "sub": subject,
        "aud": "ANY",
        "ver": "scitoken:2.0",
        "scope": scope,
        "iss": ISSUER,
        "iat": now - lag,
        "nbf": now - lag,
        "exp": now + lifetime,
        "jti": f"cst-{now}",
    }
    return jwt.encode(claims, key, algorithm=ALGORITHM,
                      headers={"kid": KEY_ID, "typ": "JWT"})


def url_for(path, scope=None, **kw):
    """Return a `?authz=`-bearing URL, the form used at views.py:144."""
    from urllib.parse import quote
    tok = mint(scope or f"read:{path}", **kw)
    return f"{ROOT_URL}{path}?authz={quote('Bearer ' + tok)}"


if __name__ == "__main__":
    import json
    import sys

    scope = sys.argv[1] if len(sys.argv) > 1 else "read:/cold-storage-test"
    tok = mint(scope)
    hdr = jwt.get_unverified_header(tok)
    body = jwt.decode(tok, options={"verify_signature": False})
    print("header:", json.dumps(hdr))
    print("claims:", json.dumps(body, indent=2))
    print(f"\nlength: {len(tok)} bytes")

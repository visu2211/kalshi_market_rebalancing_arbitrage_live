"""One-time setup: generate the RSA key pair for the DEMO account
execution/executor.py places orders against. Kalshi doesn't issue you a
private key — you generate an RSA key pair yourself, upload the PUBLIC key
through Kalshi's demo web UI, and Kalshi hands back an API Key ID that's
paired with it. Every signed request is then RSA-PSS-SHA256 signed with
your PRIVATE key, which never leaves this machine.

Separate from secrets/kalshi_private_key.pem (the prod, detection-only key
already configured here) — the demo account is a different Kalshi account
entirely and needs its own key pair. Adapted from the sibling arb-bot
project's scripts/generate_kalshi_keypair.py, renamed so the two key pairs
in this project's secrets/ don't collide.

Usage:
    python scripts/generate_demo_keypair.py

Writes:
    secrets/kalshi_demo_private_key.pem   (0600 perms; already in .gitignore — never commit this)
    secrets/kalshi_demo_public_key.pem    (paste this into Kalshi's demo UI)

Then:
    1. Log in to https://demo.kalshi.co -> Settings -> API Keys -> "Create new API key".
    2. Paste the contents of secrets/kalshi_demo_public_key.pem when prompted.
    3. Kalshi gives you back an API Key ID (a UUID) — put it in .env as
       KALSHI_DEMO_API_KEY_ID. Set
       KALSHI_DEMO_PRIVATE_KEY_PATH=./secrets/kalshi_demo_private_key.pem.
    4. Fund the demo account's paper balance via the same demo UI — orders
       reject with insufficient_balance until you do (see
       execution/executor.py's docstring).
"""

import os
import stat
from pathlib import Path

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

SECRETS_DIR = Path(__file__).resolve().parent.parent / "secrets"
PRIVATE_KEY_PATH = SECRETS_DIR / "kalshi_demo_private_key.pem"
PUBLIC_KEY_PATH = SECRETS_DIR / "kalshi_demo_public_key.pem"


def main() -> None:
    SECRETS_DIR.mkdir(exist_ok=True)

    if PRIVATE_KEY_PATH.exists():
        raise SystemExit(
            f"{PRIVATE_KEY_PATH} already exists — refusing to overwrite an existing key. "
            "Delete it yourself first if you really want to regenerate (you'll need to "
            "upload a new public key to Kalshi and get a new API Key ID too)."
        )

    # Kalshi requires >= 2048-bit RSA; use 3072 for extra headroom.
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=3072)

    private_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    public_pem = private_key.public_key().public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )

    PRIVATE_KEY_PATH.write_bytes(private_pem)
    os.chmod(PRIVATE_KEY_PATH, stat.S_IRUSR | stat.S_IWUSR)  # 0600 — owner read/write only
    PUBLIC_KEY_PATH.write_bytes(public_pem)

    print(f"Wrote private key: {PRIVATE_KEY_PATH} (0600, keep this secret)")
    print(f"Wrote public key:  {PUBLIC_KEY_PATH}")
    print()
    print("Next: upload the public key at https://demo.kalshi.co -> Settings -> API Keys,")
    print("then set KALSHI_DEMO_API_KEY_ID and KALSHI_DEMO_PRIVATE_KEY_PATH in your .env.")
    print()
    print("--- public key (also saved above) ---")
    print(public_pem.decode())


if __name__ == "__main__":
    main()

"""Kalshi request signing. Ed25519 and RSA-PSS, as documented for API keys.

The private key stays in process memory. Nothing here writes it to a log,
a model, or a response.
"""

from __future__ import annotations

import base64
from pathlib import Path
from typing import Protocol

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey


class KalshiCredentialsError(PermissionError):
    """Key id or private-key file is missing or unreadable. Not retried."""


class Signer(Protocol):
    def sign_text(self, text: str) -> str: ...


class PrivateKeySigner:
    def __init__(self, key: Ed25519PrivateKey | RSAPrivateKey) -> None:
        self._key = key
        self.algorithm = "ed25519" if isinstance(key, Ed25519PrivateKey) else "rsa-pss"

    def sign_text(self, text: str) -> str:
        message = text.encode("utf-8")
        if isinstance(self._key, Ed25519PrivateKey):
            signature = self._key.sign(message)
        else:
            signature = self._key.sign(
                message,
                padding.PSS(
                    mgf=padding.MGF1(hashes.SHA256()),
                    salt_length=padding.PSS.DIGEST_LENGTH,
                ),
                hashes.SHA256(),
            )
        return base64.b64encode(signature).decode("ascii")


def load_signer(path: str) -> PrivateKeySigner:
    file = Path(path)
    if not path or not file.is_file():
        raise KalshiCredentialsError("Kalshi private-key file is not configured.")
    try:
        raw = file.read_bytes()
        key = serialization.load_pem_private_key(raw, password=None)
    except (OSError, ValueError, TypeError) as exc:
        raise KalshiCredentialsError("Kalshi private key could not be read.") from exc
    if not isinstance(key, Ed25519PrivateKey | RSAPrivateKey):
        raise KalshiCredentialsError("Kalshi private key must be Ed25519 or RSA.")
    return PrivateKeySigner(key)


def signed_headers(
    *,
    key_id: str,
    signer: Signer,
    timestamp_ms: int,
    method: str,
    sign_path: str,
) -> dict[str, str]:
    """Headers for one request. ``sign_path`` must already exclude the query string."""
    if not key_id:
        raise KalshiCredentialsError("KALSHI_API_KEY_ID is not configured.")
    stamp = str(timestamp_ms)
    payload = stamp + method.upper() + sign_path.split("?", 1)[0]
    return {
        "KALSHI-ACCESS-KEY": key_id,
        "KALSHI-ACCESS-TIMESTAMP": stamp,
        "KALSHI-ACCESS-SIGNATURE": signer.sign_text(payload),
    }

import base64
import binascii
import hashlib
import hmac
import math
import secrets
import threading
import time
from typing import Callable, Dict, Optional, Tuple

from argon2 import PasswordHasher, Type
from argon2.exceptions import VerificationError

LEGACY_PASSWORD_HASH_ALGORITHM = "pbkdf2_sha256"
LEGACY_PASSWORD_HASH_ITERATIONS = 600_000
PASSWORD_SALT_BYTES = 16
MIN_PASSWORD_LENGTH = 12
MAX_PASSWORD_LENGTH = 1024
SESSION_COOKIE_NAME = "lacolmena_session"
SESSION_TTL_SECONDS = 3600
ARGON2ID_PREFIX = "$argon2id$"
PASSWORD_HASHER = PasswordHasher(type=Type.ID)


def hash_password(password: str) -> str:
    if (
        not isinstance(password, str)
        or not password
        or len(password) > MAX_PASSWORD_LENGTH
    ):
        raise ValueError("La contraseña no es válida.")

    try:
        password_bytes = password.encode("utf-8")
    except UnicodeEncodeError as error:
        raise ValueError("La contraseña no es válida.") from error
    return PASSWORD_HASHER.hash(password_bytes)


def verify_password(password: str, encoded_hash: Optional[str]) -> bool:
    if (
        not isinstance(password, str)
        or not isinstance(encoded_hash, str)
        or not encoded_hash
        or len(password) > MAX_PASSWORD_LENGTH
    ):
        return False

    if encoded_hash.startswith(ARGON2ID_PREFIX):
        try:
            return PASSWORD_HASHER.verify(encoded_hash, password)
        except (VerificationError, UnicodeEncodeError):
            return False

    return _verify_legacy_pbkdf2(password, encoded_hash)


def _verify_legacy_pbkdf2(password: str, encoded_hash: str) -> bool:
    try:
        algorithm, iteration_text, salt_text, expected_text = encoded_hash.split("$")
        iterations = int(iteration_text)
        salt = base64.b64decode(salt_text, altchars=b"-_", validate=True)
        expected = base64.b64decode(expected_text, altchars=b"-_", validate=True)
    except (ValueError, binascii.Error):
        return False

    if (
        algorithm != LEGACY_PASSWORD_HASH_ALGORITHM
        or iterations < 100_000
        or iterations > LEGACY_PASSWORD_HASH_ITERATIONS
        or len(salt) < PASSWORD_SALT_BYTES
        or len(expected) != hashlib.sha256().digest_size
    ):
        return False

    try:
        password_bytes = password.encode("utf-8")
    except UnicodeEncodeError:
        return False
    actual = hashlib.pbkdf2_hmac("sha256", password_bytes, salt, iterations)
    return hmac.compare_digest(actual, expected)


def password_hash_needs_rehash(encoded_hash: str) -> bool:
    if not encoded_hash.startswith(ARGON2ID_PREFIX):
        return True
    try:
        return PASSWORD_HASHER.check_needs_rehash(encoded_hash)
    except VerificationError:
        return True


DUMMY_PASSWORD_HASH = hash_password("dummy-password-for-unknown-users")


class TokenBucketRateLimiter:
    def __init__(
        self,
        capacity: int = 5,
        refill_seconds: int = 60,
        clock: Callable[[], float] = time.monotonic,
    ):
        self.capacity = capacity
        self.refill_seconds = refill_seconds
        self._clock = clock
        self._buckets: Dict[str, Tuple[float, float]] = {}
        self._lock = threading.Lock()

    def consume(self, key: str) -> Tuple[bool, int]:
        now = self._clock()
        with self._lock:
            tokens, last_updated = self._buckets.get(
                key, (float(self.capacity), now)
            )
            tokens = min(
                float(self.capacity),
                tokens + (now - last_updated) / self.refill_seconds,
            )

            if tokens >= 1:
                self._buckets[key] = (tokens - 1, now)
                return True, 0

            self._buckets[key] = (tokens, now)
            retry_after = math.ceil((1 - tokens) * self.refill_seconds)
            return False, max(1, retry_after)


class SessionStore:
    def __init__(self, ttl_seconds: int = SESSION_TTL_SECONDS):
        self.ttl_seconds = ttl_seconds
        self._sessions: Dict[str, Tuple[dict, float]] = {}
        self._lock = threading.Lock()

    def create(self, user: dict) -> str:
        token = secrets.token_urlsafe(32)
        now = time.monotonic()
        with self._lock:
            self._remove_expired(now)
            self._sessions[token] = (
                {
                    "id": user["id"],
                    "nombre": user["nombre"],
                    "email": user["email"],
                    "rol": user["rol"],
                },
                now + self.ttl_seconds,
            )
        return token

    def get(self, token: str) -> Optional[dict]:
        now = time.monotonic()
        with self._lock:
            session = self._sessions.get(token)
            if session is None:
                return None
            user, expires_at = session
            if expires_at <= now:
                self._sessions.pop(token, None)
                return None
            return dict(user)

    def revoke(self, token: str) -> None:
        with self._lock:
            self._sessions.pop(token, None)

    def revoke_user(self, user_id: int) -> None:
        with self._lock:
            tokens = [
                token
                for token, (user, _) in self._sessions.items()
                if user["id"] == user_id
            ]
            for token in tokens:
                self._sessions.pop(token, None)

    def _remove_expired(self, now: float) -> None:
        expired = [
            token for token, (_, expires_at) in self._sessions.items()
            if expires_at <= now
        ]
        for token in expired:
            self._sessions.pop(token, None)


def session_cookie(token: str) -> str:
    return (
        f"{SESSION_COOKIE_NAME}={token}; Path=/; Max-Age={SESSION_TTL_SECONDS}; "
        "HttpOnly; Secure; SameSite=Strict"
    )


def clear_session_cookie() -> str:
    return (
        f"{SESSION_COOKIE_NAME}=; Path=/; Max-Age=0; "
        "HttpOnly; Secure; SameSite=Strict"
    )


LOGIN_RATE_LIMITER = TokenBucketRateLimiter()
SESSION_STORE = SessionStore()

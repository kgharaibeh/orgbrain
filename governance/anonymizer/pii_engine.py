"""
PII Anonymization Engine
Implements all anonymization methods defined in governance rules YAML files.
Uses Vault Transit for FPE key material + Presidio for NLP scrubbing.
"""

import hashlib
import hmac
import re
import os
import logging
from typing import Any, Optional
from dataclasses import dataclass

import hvac
import yaml
from ff3 import FF3Cipher
from presidio_analyzer import AnalyzerEngine
from presidio_anonymizer import AnonymizerEngine
from presidio_anonymizer.entities import OperatorConfig

log = logging.getLogger(__name__)


@dataclass
class AnonymizationRule:
    field: str
    method: str
    vault_key: Optional[str] = None
    joinable: bool = False
    reversible: bool = False
    nullable: bool = False
    mapping: Optional[dict] = None
    presidio_entities: Optional[list] = None
    note: str = ""


class VaultKeyMaterial:
    """Fetches and caches FPE key bytes from Vault Transit."""

    def __init__(self, vault_addr: str, vault_token: str):
        self._vault_token = vault_token
        self._client = hvac.Client(url=vault_addr, token=vault_token)
        self._key_cache: dict[str, bytes] = {}

    def _local_key(self, key_name: str) -> bytes:
        """Derive a deterministic 32-byte key via local HMAC-SHA256 (fallback)."""
        return hmac.new(
            self._vault_token.encode(), key_name.encode(), "sha256"
        ).digest()

    def get_key_bytes(self, key_name: str) -> bytes:
        if key_name in self._key_cache:
            return self._key_cache[key_name]
        try:
            import base64
            # hvac >= 1.0 uses generate_hmac; input must be base64-encoded
            response = self._client.secrets.transit.generate_hmac(
                name=key_name,
                input_data=base64.b64encode(key_name.encode()).decode(),
                algorithm="sha2-256",
            )
            raw = response["data"]["hmac"]  # "vault:v1:<base64>"
            b64_part = raw.replace("vault:v1:", "")
            # pad to valid base64 length before decoding
            key_bytes = base64.b64decode(b64_part + "==")[:32].ljust(32, b"\x00")
        except Exception:
            key_bytes = self._local_key(key_name)
        self._key_cache[key_name] = key_bytes
        return key_bytes

    def encrypt(self, key_name: str, plaintext_b64: str) -> str:
        response = self._client.secrets.transit.encrypt_data(
            name=key_name, plaintext=plaintext_b64
        )
        return response["data"]["ciphertext"]

    def decrypt(self, key_name: str, ciphertext: str) -> str:
        response = self._client.secrets.transit.decrypt_data(
            name=key_name, ciphertext=ciphertext
        )
        return response["data"]["plaintext"]

    def hmac_sign(self, key_name: str, data_b64: str) -> str:
        import base64
        try:
            response = self._client.secrets.transit.generate_hmac(
                name=key_name, input_data=data_b64, algorithm="sha2-256"
            )
            return response["data"]["hmac"]
        except Exception:
            data = base64.b64decode(data_b64 + "==")
            h = hmac.new(self._local_key(key_name), data, "sha256").hexdigest()
            return f"vault:v1:{h}"


class PIIEngine:
    """
    Applies anonymization rules to a record dict.
    Thread-safe — one instance per Flink task (initialized in open()).
    """

    NUMERIC_ALPHABET = "0123456789"
    ALPHA_ALPHABET = "abcdefghijklmnopqrstuvwxyz"
    ALPHANUM_ALPHABET = "abcdefghijklmnopqrstuvwxyz0123456789"

    def __init__(
        self,
        vault_addr: str,
        vault_token: str,
        rules: list[AnonymizationRule],
    ):
        self._vault = VaultKeyMaterial(vault_addr, vault_token)
        self._rules = {r.field: r for r in rules}
        self._fpe_ciphers: dict[str, FF3Cipher] = {}

        # Presidio for NLP scrubbing
        self._presidio_analyzer = AnalyzerEngine()
        self._presidio_anonymizer = AnonymizerEngine()

    def _get_fpe_cipher(self, vault_key: str, alphabet: str) -> FF3Cipher:
        cache_key = f"{vault_key}:{alphabet}"
        if cache_key not in self._fpe_ciphers:
            key_bytes = self._vault.get_key_bytes(vault_key)
            tweak = key_bytes[:7]
            # installed ff3 uses integer radix, not alphabet string
            self._fpe_ciphers[cache_key] = FF3Cipher(
                key_bytes.hex(), tweak.hex(), len(alphabet)
            )
        return self._fpe_ciphers[cache_key]

    # ─── Anonymization methods ─────────────────────────────────────────────

    def fpe_numeric(self, value: str, vault_key: str) -> str:
        """Format-preserving encryption for numeric strings (e.g. account numbers)."""
        if not value:
            return value
        # Preserve non-digit characters (dashes, spaces in IBANs)
        digits_only = re.sub(r"\D", "", value)
        if len(digits_only) < 2:
            return value  # FF3 needs at least 2 chars
        cipher = self._get_fpe_cipher(vault_key, self.NUMERIC_ALPHABET)
        encrypted_digits = cipher.encrypt(digits_only)
        # Reinsert non-digit characters at original positions
        result, digit_iter = [], iter(encrypted_digits)
        for ch in value:
            result.append(next(digit_iter) if ch.isdigit() else ch)
        return "".join(result)

    def fpe_date(self, value: str, vault_key: str) -> str:
        """FPE on dates: preserves YYYY-MM-DD format, shuffles month and day."""
        if not value:
            return value
        try:
            parts = str(value).split("-")
            if len(parts) != 3:
                return value
            year, month, day = parts
            # Encrypt month and day as 2-digit numeric strings
            cipher = self._get_fpe_cipher(vault_key, self.NUMERIC_ALPHABET)
            enc_month = cipher.encrypt(month.zfill(2))
            enc_day = cipher.encrypt(day.zfill(2))
            # Clamp to valid ranges
            enc_month = str(max(1, min(12, int(enc_month)))).zfill(2)
            enc_day = str(max(1, min(28, int(enc_day)))).zfill(2)
            return f"{year}-{enc_month}-{enc_day}"
        except Exception:
            return value

    def fpe_email(self, value: str, vault_key: str) -> str:
        """FPE on email — encrypts local part, preserves domain."""
        if not value or "@" not in value:
            return value
        local, domain = value.rsplit("@", 1)
        if len(local) < 2:
            return value
        cipher = self._get_fpe_cipher(vault_key, self.ALPHANUM_ALPHABET)
        encrypted_local = cipher.encrypt(local.lower())
        return f"{encrypted_local}@{domain}"

    def hmac_sha256(self, value: str, vault_key: str) -> str:
        """HMAC-SHA256 one-way hash — deterministic, consistent token."""
        if not value:
            return value
        import base64
        b64_value = base64.b64encode(str(value).encode()).decode()
        signed = self._vault.hmac_sign(vault_key, b64_value)
        # Shorten to 32 hex chars for readability while keeping collision resistance
        return signed.replace("vault:v1:", "")[:32]

    def generalize(self, value: str, mapping: dict) -> str:
        """Replace value with a generalized category."""
        return mapping.get(str(value), mapping.get("", "UNKNOWN"))

    def nlp_scrub(self, value: str, entities: list[str]) -> str:
        """Use Presidio to detect and redact PII entities from free text."""
        if not value:
            return value
        results = self._presidio_analyzer.analyze(
            text=str(value), entities=entities, language="en"
        )
        if not results:
            return value
        anonymized = self._presidio_anonymizer.anonymize(
            text=str(value),
            analyzer_results=results,
            operators={
                entity: OperatorConfig("replace", {"new_value": f"[{entity}]"})
                for entity in entities
            },
        )
        return anonymized.text

    # ─── Main dispatch ─────────────────────────────────────────────────────

    def anonymize_record(self, record: dict) -> dict:
        """Apply all matching rules to a record. Returns anonymized copy."""
        result = dict(record)
        for field, rule in self._rules.items():
            if field not in result:
                continue
            raw_value = result[field]
            if raw_value is None and rule.nullable:
                continue

            try:
                if rule.method == "fpe_numeric":
                    result[field] = self.fpe_numeric(str(raw_value), rule.vault_key)

                elif rule.method == "fpe_date":
                    result[field] = self.fpe_date(str(raw_value), rule.vault_key)

                elif rule.method == "fpe_email":
                    result[field] = self.fpe_email(str(raw_value), rule.vault_key)

                elif rule.method == "hmac_sha256":
                    result[field] = self.hmac_sha256(str(raw_value), rule.vault_key)

                elif rule.method == "generalize":
                    result[field] = self.generalize(str(raw_value), rule.mapping or {})

                elif rule.method == "suppress":
                    del result[field]

                elif rule.method == "nlp_scrub":
                    result[field] = self.nlp_scrub(
                        str(raw_value), rule.presidio_entities or []
                    )

                elif rule.method == "keep":
                    pass  # explicit no-op

            except Exception as e:
                log.warning("Failed to anonymize field %s: %s", field, e)
                # On error: suppress the field rather than leak raw PII
                result[field] = "[ANONYMIZATION_ERROR]"

        return result


def load_rules_for_topic(rules_yaml_path: str, raw_topic: str) -> list[AnonymizationRule]:
    """Parse governance YAML and return rules for the given raw topic."""
    with open(rules_yaml_path) as f:
        config = yaml.safe_load(f)

    for topic_config in config.get("topics", []):
        if topic_config["raw_topic"] == raw_topic:
            return [
                AnonymizationRule(
                    field=r["field"],
                    method=r["method"],
                    vault_key=r.get("vault_key"),
                    joinable=r.get("joinable", False),
                    reversible=r.get("reversible", False),
                    nullable=r.get("nullable", False),
                    mapping=r.get("mapping"),
                    presidio_entities=r.get("presidio_entities"),
                    note=r.get("note", ""),
                )
                for r in topic_config.get("rules", [])
            ]
    return []

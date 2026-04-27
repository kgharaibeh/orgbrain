#!/bin/sh
# =============================================================================
# Vault Initialization — OrgBrain
# Sets up Transit secrets engine for FPE keys + access policies
# =============================================================================

set -e

echo ">>> Waiting for Vault to be ready..."
until vault status 2>/dev/null; do sleep 2; done
echo ">>> Vault is ready."

# ─── Enable Transit Secrets Engine (for FPE / encryption) ────────────────────
vault secrets enable transit || echo "Transit already enabled"

# ─── Create FPE encryption keys ──────────────────────────────────────────────
# One key per sensitive field type — allows key rotation per type independently

vault write -f transit/keys/fpe-numeric-key \
  type=aes256-gcm96

vault write -f transit/keys/fpe-date-key \
  type=aes256-gcm96

vault write -f transit/keys/fpe-email-key \
  type=aes256-gcm96

vault write -f transit/keys/hmac-name-key \
  type=aes256-gcm96

vault write -f transit/keys/hmac-device-key \
  type=aes256-gcm96

echo ">>> FPE keys created."

# ─── Enable KV store for config ──────────────────────────────────────────────
vault secrets enable -path=orgbrain kv-v2 || echo "KV already enabled"

# Store the active key aliases for the anonymizer to reference
vault kv put orgbrain/anonymizer/config \
  fpe_numeric_key="fpe-numeric-key" \
  fpe_date_key="fpe-date-key" \
  fpe_email_key="fpe-email-key" \
  hmac_name_key="hmac-name-key" \
  hmac_device_key="hmac-device-key"

# ─── Policies ────────────────────────────────────────────────────────────────

# anonymizer-policy: encrypt only, no decrypt
vault policy write anonymizer-policy - <<'POLICY'
# Anonymizer service — encrypt PII, cannot decrypt
path "transit/encrypt/*" {
  capabilities = ["update"]
}
path "transit/hmac/*" {
  capabilities = ["update"]
}
path "orgbrain/data/anonymizer/config" {
  capabilities = ["read"]
}
POLICY

# deanonymizer-policy: decrypt + audit
vault policy write deanonymizer-policy - <<'POLICY'
# Deanonymizer — authorized data stewards only
path "transit/decrypt/*" {
  capabilities = ["update"]
}
path "transit/encrypt/*" {
  capabilities = ["update"]
}
path "orgbrain/data/*" {
  capabilities = ["read"]
}
POLICY

# brain-read-policy: no decrypt, read config only
vault policy write brain-read-policy - <<'POLICY'
path "orgbrain/data/*" {
  capabilities = ["read", "list"]
}
POLICY

# admin-policy: full access
vault policy write orgbrain-admin - <<'POLICY'
path "*" {
  capabilities = ["create", "read", "update", "delete", "list", "sudo"]
}
POLICY

echo ">>> Vault policies created."

# ─── AppRole Auth (for services to authenticate) ─────────────────────────────
vault auth enable approle || echo "AppRole already enabled"

# Anonymizer service role
vault write auth/approle/role/anonymizer-role \
  token_policies="anonymizer-policy" \
  token_ttl=1h \
  token_max_ttl=24h

# Deanonymizer role (for authorized API calls)
vault write auth/approle/role/deanonymizer-role \
  token_policies="deanonymizer-policy" \
  token_ttl=30m \
  token_max_ttl=4h

# Brain service role
vault write auth/approle/role/brain-role \
  token_policies="brain-read-policy" \
  token_ttl=24h \
  token_max_ttl=72h

# Retrieve and store role IDs / secret IDs for service config
ANONYMIZER_ROLE_ID=$(vault read -field=role_id auth/approle/role/anonymizer-role/role-id)
ANONYMIZER_SECRET_ID=$(vault write -field=secret_id -f auth/approle/role/anonymizer-role/secret-id)

BRAIN_ROLE_ID=$(vault read -field=role_id auth/approle/role/brain-role/role-id)
BRAIN_SECRET_ID=$(vault write -field=secret_id -f auth/approle/role/brain-role/secret-id)

# Store credentials in KV for container startup (dev only — use external secret mgmt in prod)
vault kv put orgbrain/services/anonymizer \
  role_id="${ANONYMIZER_ROLE_ID}" \
  secret_id="${ANONYMIZER_SECRET_ID}"

vault kv put orgbrain/services/brain \
  role_id="${BRAIN_ROLE_ID}" \
  secret_id="${BRAIN_SECRET_ID}"

echo ">>> AppRole credentials stored."
echo ">>> Vault initialization complete!"
echo ""
echo "  Vault UI:  http://localhost:8200"
echo "  Root token: orgbrain-vault-root  (DEV MODE ONLY)"

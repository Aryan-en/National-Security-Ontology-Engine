#!/usr/bin/env bash
# Vault secrets engine initialisation — 2.6.5
# Run once after vault operator init + unseal.
# Sets up PKI, KV, and Transit secrets engines for NSOE.

set -euo pipefail
VAULT_ADDR="${VAULT_ADDR:-https://vault-node-1.internal:8200}"
export VAULT_ADDR

echo "=== Enabling PKI secrets engine ==="
vault secrets enable -path=nsoe-pki pki
vault secrets tune -max-lease-ttl=87600h nsoe-pki
vault write nsoe-pki/root/generate/internal \
    common_name="NSOE Internal CA" \
    ttl=87600h \
    key_type=ec key_bits=384
vault write nsoe-pki/roles/nsoe-service \
    allowed_domains="*.nsoe.internal,nsoe.internal" \
    allow_subdomains=true \
    max_ttl=8760h \
    key_type=ec key_bits=256 \
    require_cn=false

echo "=== Enabling KV v2 secrets engine ==="
vault secrets enable -path=nsoe-kv -version=2 kv
vault kv put nsoe-kv/socmint/twitter_bearer_token value="REPLACE_ME"
vault kv put nsoe-kv/socmint/telegram_bot_token   value="REPLACE_ME"
vault kv put nsoe-kv/taxii/cert_password           value="REPLACE_ME"
vault kv put nsoe-kv/neo4j/password               value="REPLACE_ME"
vault kv put nsoe-kv/keycloak/admin_password      value="REPLACE_ME"

echo "=== Enabling Transit secrets engine (field-level encryption) ==="
vault secrets enable -path=nsoe-transit transit
vault write nsoe-transit/keys/pii-fields type=aes256-gcm96
vault write nsoe-transit/keys/biometrics  type=aes256-gcm96

echo "=== Configuring AppRole authentication ==="
vault auth enable approle
vault write auth/approle/role/nsoe-api \
    secret_id_ttl=24h \
    token_num_uses=1000 \
    token_ttl=1h \
    token_max_ttl=4h \
    token_policies="nsoe-api-policy"
vault write auth/approle/role/nsoe-edge \
    secret_id_ttl=720h \
    token_ttl=24h \
    token_max_ttl=168h \
    token_policies="nsoe-edge-policy"

echo "=== Writing policies ==="
vault policy write nsoe-api-policy - <<EOF
path "nsoe-kv/data/*"      { capabilities = ["read"] }
path "nsoe-pki/issue/*"    { capabilities = ["create","update"] }
path "nsoe-transit/encrypt/pii-fields" { capabilities = ["update"] }
path "nsoe-transit/decrypt/pii-fields" { capabilities = ["update"] }
EOF

vault policy write nsoe-edge-policy - <<EOF
path "nsoe-kv/data/kafka/*" { capabilities = ["read"] }
path "nsoe-pki/issue/nsoe-service" { capabilities = ["create","update"] }
EOF

echo "=== Vault initialisation complete ==="

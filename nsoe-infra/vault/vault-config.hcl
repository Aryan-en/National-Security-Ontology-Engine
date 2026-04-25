# HashiCorp Vault Production Cluster Configuration — 2.6.5
# 3-node HA cluster using Raft integrated storage.
# Deploy one copy of this config on each Vault node with
# the node-specific api_addr and cluster_addr.

storage "raft" {
  path    = "/vault/data"
  node_id = "vault-node-1"    # change per node: vault-node-1/2/3

  retry_join {
    leader_api_addr = "https://vault-node-1.internal:8200"
  }
  retry_join {
    leader_api_addr = "https://vault-node-2.internal:8200"
  }
  retry_join {
    leader_api_addr = "https://vault-node-3.internal:8200"
  }
}

listener "tcp" {
  address       = "0.0.0.0:8200"
  tls_cert_file = "/vault/tls/server.crt"
  tls_key_file  = "/vault/tls/server.key"
  tls_client_ca_file = "/vault/tls/ca.crt"
  tls_require_and_verify_client_cert = true
}

api_addr     = "https://vault-node-1.internal:8200"   # change per node
cluster_addr = "https://vault-node-1.internal:8201"   # change per node

seal "awskms" {                           # or "gcpckms" / "azurekeyvault"
  region     = "ap-south-1"
  kms_key_id = "alias/nsoe-vault-unseal"
}

telemetry {
  prometheus_retention_time = "30s"
  disable_hostname           = true
}

ui = false    # UI disabled in production; use CLI or API only

log_level  = "warn"
log_format = "json"

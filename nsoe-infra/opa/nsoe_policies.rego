# NSOE OPA Policies — 2.6.3
# Enforced via OPA sidecar on all nsoe-api instances.
# Policy categories:
#   1. Data classification access control (RESTRICTED vs SECRET)
#   2. Geographic zone restrictions for analysts
#   3. API rate limiting (enforced upstream by Envoy)

package nsoe.authz

import future.keywords.in

# ── Helpers ──────────────────────────────────────────────────

default allow := false

# Analyst roles
analyst_roles := {"nsoe_analyst", "nsoe_senior_analyst", "nsoe_admin"}

# Classification levels ordered by sensitivity
classification_rank := {
    "UNCLASSIFIED": 0,
    "RESTRICTED":   1,
    "SECRET":       2,
    "TOP_SECRET":   3,
}

# Clearance level → maximum classification rank the user may access
clearance_rank := {
    "RESTRICTED":  1,
    "SECRET":      2,
    "TOP_SECRET":  3,
}

# ── Rule 1: Basic role check ──────────────────────────────────

user_has_analyst_role if {
    some role in input.user.roles
    role in analyst_roles
}

# ── Rule 2: Classification access control ────────────────────
# A user may only access a resource if their clearance_rank >= resource classification_rank

user_clearance_rank := rank if {
    rank := clearance_rank[input.user.clearance_level]
}

user_clearance_rank := 0 if {
    not clearance_rank[input.user.clearance_level]
}

resource_classification_rank := rank if {
    rank := classification_rank[input.resource.classification_level]
}

resource_classification_rank := 0 if {
    not classification_rank[input.resource.classification_level]
}

can_access_classification if {
    user_clearance_rank >= resource_classification_rank
}

# ── Rule 3: Geographic zone restriction ───────────────────────
# Analysts are assigned to one or more permitted geo zones.
# "nsoe_admin" users bypass zone restrictions.

can_access_zone if {
    "nsoe_admin" in input.user.roles
}

can_access_zone if {
    input.resource.geo_zone_id == null
}

can_access_zone if {
    some zone in input.user.permitted_zones
    zone == input.resource.geo_zone_id
}

can_access_zone if {
    "ALL_ZONES" in input.user.permitted_zones
}

# ── Rule 4: HTTP method restrictions ─────────────────────────
# Junior analysts (nsoe_analyst) may not DELETE or write to case management.

can_perform_method if {
    input.method in {"GET", "HEAD", "OPTIONS"}
}

can_perform_method if {
    input.method in {"POST", "PUT", "PATCH"}
    user_has_analyst_role
}

can_perform_method if {
    input.method == "DELETE"
    "nsoe_admin" in input.user.roles
}

# ── Rule 5: Feedback endpoint — any analyst may submit ────────
feedback_endpoint_allow if {
    input.path == "/api/v1/feedback"
    input.method == "POST"
    user_has_analyst_role
}

# ── Master allow rule ─────────────────────────────────────────

allow if {
    user_has_analyst_role
    can_access_classification
    can_access_zone
    can_perform_method
}

allow if {
    feedback_endpoint_allow
}

# ── Audit data (always returned, used by audit log service) ──

audit := {
    "user_sub":         input.user.sub,
    "clearance_level":  input.user.clearance_level,
    "resource_path":    input.path,
    "method":           input.method,
    "allowed":          allow,
    "timestamp":        input.request_time,
}

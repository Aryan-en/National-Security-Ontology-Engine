/**
 * API client — wraps fetch with auth token injection and error handling.
 */

import { getToken } from "./auth";

const BASE_URL = import.meta.env.VITE_API_BASE_URL ?? "http://localhost:8080/api/v1";

async function request<T>(
  method: string,
  path: string,
  body?: unknown,
): Promise<T> {
  const token = getToken();
  const resp = await fetch(`${BASE_URL}${path}`, {
    method,
    headers: {
      "Content-Type": "application/json",
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
    },
    body: body !== undefined ? JSON.stringify(body) : undefined,
  });

  if (!resp.ok) {
    const detail = await resp.text().catch(() => resp.statusText);
    throw new Error(`API ${method} ${path} → ${resp.status}: ${detail}`);
  }

  if (resp.status === 204) return undefined as T;
  return resp.json() as Promise<T>;
}

export const api = {
  get:  <T>(path: string)                     => request<T>("GET",  path),
  post: <T>(path: string, body?: unknown)      => request<T>("POST", path, body),
  put:  <T>(path: string, body?: unknown)      => request<T>("PUT",  path, body),
};

// Typed helpers

export interface AlertResponse {
  alert_id: string;
  alert_type: string;
  alert_tier: string;
  fused_confidence: number;
  camera_id?: string;
  geo_zone_id?: string;
  latitude?: number;
  longitude?: number;
  involved_track_ids: string[];
  involved_entity_ids: string[];
  contributing_events: string[];
  source_domains: string[];
  triggered_at: string;
  status: string;
}

export interface AlertPage {
  alerts: AlertResponse[];
  total: number;
  page: number;
  page_size: number;
}

export interface EntityResult {
  entity_id: string;
  entity_type: string;
  name?: string;
  risk_score?: number;
  confidence_score?: number;
  watchlist_flag: boolean;
  classification_level: string;
  source_domain?: string;
  last_seen?: string;
  highlight: Record<string, string[]>;
}

export interface SearchResponse {
  results: EntityResult[];
  total: number;
  query: string;
}

export interface GraphResponse {
  nodes: { id: string; labels: string[]; properties: Record<string, unknown> }[];
  edges: { source: string; target: string; type: string; properties: Record<string, unknown> }[];
}

export const alertsApi = {
  list: (page = 1, pageSize = 50, status = ["ACTIVE"]) =>
    api.get<AlertPage>(`/alerts?page=${page}&page_size=${pageSize}&${status.map(s => `status=${s}`).join("&")}`),
  get: (id: string) => api.get<AlertResponse>(`/alerts/${id}`),
  acknowledge: (id: string) => api.post<void>(`/alerts/${id}/acknowledge`),
  close: (id: string) => api.post<void>(`/alerts/${id}/close`),
  escalate: (id: string) => api.post<void>(`/alerts/${id}/escalate`),
};

export const entitiesApi = {
  search: (q: string, entityType?: string, minRisk = 0) =>
    api.get<SearchResponse>(
      `/entities/search?q=${encodeURIComponent(q)}` +
      (entityType ? `&entity_type=${entityType}` : "") +
      (minRisk > 0 ? `&min_risk=${minRisk}` : ""),
    ),
  get: (id: string) => api.get<EntityResult>(`/entities/${id}`),
};

export const graphApi = {
  neighborhood: (entityId: string, hops = 1) =>
    api.post<GraphResponse>("/graph/neighborhood", { entity_id: entityId, hops }),
  path: (fromId: string, toId: string, maxHops = 5) =>
    api.post<GraphResponse>("/graph/path", { from_entity_id: fromId, to_entity_id: toId, max_hops: maxHops }),
};

export const feedbackApi = {
  submit: (alertId: string, verdict: "TRUE_POSITIVE" | "FALSE_POSITIVE", notes = "") =>
    api.post<void>("/feedback", { alert_id: alertId, verdict, notes }),
};

// ── Case Management ──────────────────────────────────────────

export interface CaseEvidence {
  evidence_id: string;
  evidence_type: "GRAPH_ENTITY" | "VIDEO_CLIP" | "DOCUMENT" | "INDICATOR";
  uri: string;
  added_at: string;
}

export interface CaseComment {
  comment_id: string;
  body: string;
  author_sub: string;
  created_at: string;
}

export interface CaseSummary {
  case_id: string;
  title: string;
  status: "OPEN" | "IN_PROGRESS" | "CLOSED" | "ESCALATED";
  priority: string;
  assigned_to: string;
  created_at: string;
  sla_deadline: string;
  classification_level: string;
}

export interface CaseTimeline {
  case: CaseSummary;
  alerts: { alert_id: string; alert_type: string; alert_tier: string; triggered_at: string }[];
  evidence: CaseEvidence[];
  comments: CaseComment[];
}

export interface CaseListResponse {
  cases: CaseSummary[];
  total: number;
}

export const casesApi = {
  list: (statuses = ["OPEN", "IN_PROGRESS"], assignedToMe = false) =>
    api.get<CaseListResponse>(
      `/cases?${statuses.map(s => `status=${s}`).join("&")}&assigned_to_me=${assignedToMe}`,
    ),
  get: (id: string) => api.get<CaseTimeline>(`/cases/${id}`),
  create: (alertId: string, alertTier: string, title?: string) =>
    api.post<{ case_id: string }>("/cases", { alert_id: alertId, alert_tier: alertTier, title }),
  addEvidence: (caseId: string, evidenceType: CaseEvidence["evidence_type"], uri: string, description = "") =>
    api.post<{ evidence_id: string }>(`/cases/${caseId}/evidence`, { evidence_type: evidenceType, uri, description }),
  addComment: (caseId: string, body: string) =>
    api.post<{ comment_id: string }>(`/cases/${caseId}/comments`, { body }),
  updateStatus: (caseId: string, status: CaseSummary["status"]) =>
    api.put<void>(`/cases/${caseId}/status`, { status }),
};

// ── SOCMINT Feed ─────────────────────────────────────────────

export interface SocmintPost {
  event_id: string;
  platform: "TWITTER" | "TELEGRAM" | "RSS";
  author_handle: string;
  content: string;
  language: string;
  threat_intent_score: number;
  source_credibility: number;
  is_bot: boolean;
  entities: { text: string; label: string; confidence: number }[];
  hold_until_ms?: number;
  collected_at: string;
}

export interface SocmintFeedResponse {
  posts: SocmintPost[];
  total: number;
}

export const socmintApi = {
  feed: (minThreat = 0.4, limit = 50) =>
    api.get<SocmintFeedResponse>(`/socmint/feed?min_threat=${minThreat}&limit=${limit}`),
};

// ── Risk Score History ────────────────────────────────────────

export interface RiskScorePoint {
  scored_at: string;
  risk_score: number;
}

export interface RiskScoreHistory {
  entity_id: string;
  entity_name?: string;
  history: RiskScorePoint[];
}

export const mlApi = {
  riskHistory: (entityId: string, days = 30) =>
    api.get<RiskScoreHistory>(`/ml/risk-history/${entityId}?days=${days}`),
};

// ── Audit Log ────────────────────────────────────────────────

export interface AuditEntry {
  entry_id: string;
  actor_sub: string;
  action: string;
  resource_type: string;
  resource_id: string;
  payload_hash: string;
  timestamp: string;
}

export interface AuditLogResponse {
  entries: AuditEntry[];
  total: number;
}

export const auditApi = {
  list: (resourceType?: string, actorSub?: string, page = 1, pageSize = 100) =>
    api.get<AuditLogResponse>(
      `/audit?page=${page}&page_size=${pageSize}` +
      (resourceType ? `&resource_type=${resourceType}` : "") +
      (actorSub ? `&actor_sub=${encodeURIComponent(actorSub)}` : ""),
    ),
};

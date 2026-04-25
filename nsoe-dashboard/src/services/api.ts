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

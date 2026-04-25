/**
 * Entity search panel — queries Elasticsearch via /api/v1/entities/search
 */

import React, { useState, useCallback } from "react";
import type { EntityResult } from "../services/api";
import { entitiesApi } from "../services/api";

interface Props {
  onEntitySelect?: (entity: EntityResult) => void;
}

export function EntitySearch({ onEntitySelect }: Props) {
  const [query, setQuery] = useState("");
  const [results, setResults] = useState<EntityResult[]>([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const search = useCallback(async () => {
    if (query.trim().length < 2) return;
    setLoading(true);
    setError(null);
    try {
      const resp = await entitiesApi.search(query);
      setResults(resp.results);
      setTotal(resp.total);
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Search failed");
    } finally {
      setLoading(false);
    }
  }, [query]);

  function handleKeyDown(e: React.KeyboardEvent<HTMLInputElement>) {
    if (e.key === "Enter") search();
  }

  return (
    <div style={{ padding: 12 }}>
      <div style={{ display: "flex", gap: 8, marginBottom: 12 }}>
        <input
          type="text"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          onKeyDown={handleKeyDown}
          placeholder="Search entities…"
          style={{
            flex: 1, padding: "6px 10px", borderRadius: 4,
            border: "1px solid #334155", background: "#1e293b",
            color: "#e2e8f0", fontSize: 13,
          }}
        />
        <button
          onClick={search}
          disabled={loading || query.trim().length < 2}
          style={{
            padding: "6px 14px", borderRadius: 4, border: "none",
            background: "#2563eb", color: "#fff", cursor: "pointer", fontSize: 13,
          }}
        >
          {loading ? "…" : "Search"}
        </button>
      </div>

      {error && <div style={{ color: "#ef4444", fontSize: 12, marginBottom: 8 }}>{error}</div>}

      {results.length > 0 && (
        <div style={{ color: "#64748b", fontSize: 12, marginBottom: 6 }}>
          {total} result{total !== 1 ? "s" : ""} found
        </div>
      )}

      {results.map((entity) => (
        <div
          key={entity.entity_id}
          onClick={() => onEntitySelect?.(entity)}
          style={{
            padding: "8px 10px", marginBottom: 4, borderRadius: 4,
            background: "#1e293b", cursor: "pointer",
            border: "1px solid #334155",
          }}
        >
          <div style={{ display: "flex", justifyContent: "space-between" }}>
            <span style={{ color: "#e2e8f0", fontSize: 13, fontWeight: 500 }}>
              {entity.name ?? entity.entity_id}
            </span>
            {entity.watchlist_flag && (
              <span style={{
                padding: "1px 6px", borderRadius: 3, background: "#991b1b",
                color: "#fff", fontSize: 11,
              }}>
                WATCHLIST
              </span>
            )}
          </div>
          <div style={{ color: "#64748b", fontSize: 11, marginTop: 2 }}>
            {entity.entity_type}
            {entity.risk_score != null && ` · Risk: ${(entity.risk_score * 100).toFixed(0)}%`}
          </div>
        </div>
      ))}
    </div>
  );
}

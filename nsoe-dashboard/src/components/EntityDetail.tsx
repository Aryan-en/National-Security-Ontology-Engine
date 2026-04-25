/**
 * EntityDetail — 2.8.2
 * Shows full entity profile: metadata, risk score badge, graph neighborhood
 * and a link to open the cross-domain timeline.
 */

import React, { useEffect, useState } from "react";
import { entitiesApi, graphApi } from "../services/api";
import type { EntityResult, GraphResponse } from "../services/api";
import { GraphExplorer } from "./GraphExplorer";

interface Props {
  entityId: string;
  onClose: () => void;
  onOpenTimeline: (entityId: string) => void;
}

const RISK_COLOR = (score: number) =>
  score >= 0.8 ? "#ef4444" : score >= 0.6 ? "#f97316" : score >= 0.4 ? "#eab308" : "#22c55e";

const CLASSIFICATION_COLORS: Record<string, string> = {
  TOP_SECRET: "#7c3aed",
  SECRET:     "#dc2626",
  CONFIDENTIAL: "#ea580c",
  RESTRICTED: "#ca8a04",
  UNCLASSIFIED: "#16a34a",
};

const FIELD_LABEL: Record<string, string> = {
  entity_id:           "Entity ID",
  entity_type:         "Type",
  source_domain:       "Source Domain",
  confidence_score:    "Confidence",
  classification_level:"Classification",
  last_seen:           "Last Seen",
  watchlist_flag:      "Watchlisted",
};

export const EntityDetail: React.FC<Props> = ({ entityId, onClose, onOpenTimeline }) => {
  const [entity, setEntity] = useState<EntityResult | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    setLoading(true);
    setError(null);
    entitiesApi.get(entityId)
      .then(setEntity)
      .catch(e => setError(String(e)))
      .finally(() => setLoading(false));
  }, [entityId]);

  if (loading) return (
    <div style={overlay}>
      <div style={panel}>
        <div style={{ color: "#64748b", padding: 32, textAlign: "center" }}>Loading entity…</div>
      </div>
    </div>
  );

  if (error || !entity) return (
    <div style={overlay}>
      <div style={panel}>
        <div style={{ color: "#ef4444", padding: 24 }}>Error: {error ?? "Entity not found"}</div>
        <button onClick={onClose} style={closeBtnStyle}>Close</button>
      </div>
    </div>
  );

  const riskScore = entity.risk_score ?? 0;
  const classColor = CLASSIFICATION_COLORS[entity.classification_level] ?? "#64748b";

  return (
    <div style={overlay} onClick={(e) => { if (e.target === e.currentTarget) onClose(); }}>
      <div style={panel}>
        {/* Header */}
        <div style={{
          display: "flex", justifyContent: "space-between", alignItems: "flex-start",
          marginBottom: 16,
        }}>
          <div>
            <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
              <span style={{ fontSize: 20, fontWeight: 700, color: "#e2e8f0" }}>
                {entity.name ?? entity.entity_id.slice(0, 16)}
              </span>
              <span style={{
                padding: "2px 8px", borderRadius: 4, fontSize: 11, fontWeight: 600,
                background: classColor + "22", color: classColor, border: `1px solid ${classColor}55`,
              }}>
                {entity.classification_level}
              </span>
              {entity.watchlist_flag && (
                <span style={{
                  padding: "2px 8px", borderRadius: 4, fontSize: 11, fontWeight: 600,
                  background: "#ef444422", color: "#ef4444", border: "1px solid #ef444455",
                }}>
                  WATCHLIST
                </span>
              )}
            </div>
            <div style={{ color: "#64748b", fontSize: 12, marginTop: 4 }}>
              {entity.entity_type} · {entity.source_domain ?? "—"}
            </div>
          </div>

          {/* Risk gauge */}
          <div style={{
            display: "flex", flexDirection: "column", alignItems: "center", gap: 2, marginRight: 40,
          }}>
            <div style={{
              width: 64, height: 64, borderRadius: "50%",
              background: `conic-gradient(${RISK_COLOR(riskScore)} ${riskScore * 360}deg, #1e293b 0deg)`,
              display: "flex", alignItems: "center", justifyContent: "center",
            }}>
              <div style={{
                width: 48, height: 48, borderRadius: "50%", background: "#0f172a",
                display: "flex", alignItems: "center", justifyContent: "center",
                color: RISK_COLOR(riskScore), fontWeight: 700, fontSize: 15,
              }}>
                {Math.round(riskScore * 100)}
              </div>
            </div>
            <span style={{ fontSize: 11, color: "#64748b" }}>Risk Score</span>
          </div>

          <button onClick={onClose} style={closeBtnStyle}>✕</button>
        </div>

        {/* Metadata grid */}
        <div style={{
          display: "grid", gridTemplateColumns: "1fr 1fr", gap: "8px 16px",
          padding: "12px 0", borderTop: "1px solid #1e293b", borderBottom: "1px solid #1e293b",
          marginBottom: 16,
        }}>
          {Object.entries(FIELD_LABEL).map(([key, label]) => {
            const val = (entity as Record<string, unknown>)[key];
            if (val === undefined || val === null) return null;
            return (
              <div key={key}>
                <div style={{ fontSize: 10, color: "#64748b", textTransform: "uppercase", letterSpacing: 0.5 }}>
                  {label}
                </div>
                <div style={{ fontSize: 13, color: "#e2e8f0" }}>
                  {typeof val === "boolean" ? (val ? "Yes" : "No") :
                   typeof val === "number" ? val.toFixed(3) :
                   String(val)}
                </div>
              </div>
            );
          })}
        </div>

        {/* Actions */}
        <div style={{ display: "flex", gap: 8, marginBottom: 16 }}>
          <button
            onClick={() => onOpenTimeline(entity.entity_id)}
            style={{
              padding: "6px 14px", borderRadius: 4, border: "1px solid #334155",
              background: "#1e3a5f", color: "#93c5fd", cursor: "pointer", fontSize: 13,
            }}
          >
            Cross-Domain Timeline
          </button>
          <button
            onClick={() => navigator.clipboard?.writeText(entity.entity_id)}
            style={{
              padding: "6px 14px", borderRadius: 4, border: "1px solid #334155",
              background: "transparent", color: "#64748b", cursor: "pointer", fontSize: 13,
            }}
          >
            Copy ID
          </button>
        </div>

        {/* Graph neighborhood */}
        <div style={{ fontSize: 12, color: "#64748b", marginBottom: 8 }}>
          Graph Neighbourhood (2 hops)
        </div>
        <div style={{ flex: 1, minHeight: 320, background: "#0a0f1e", borderRadius: 6, overflow: "hidden" }}>
          <GraphExplorer entity={entity} defaultHops={2} />
        </div>
      </div>
    </div>
  );
};

// ── Styles ───────────────────────────────────────────────────

const overlay: React.CSSProperties = {
  position: "fixed", inset: 0, background: "rgba(0,0,0,0.6)", zIndex: 1000,
  display: "flex", alignItems: "center", justifyContent: "center",
};

const panel: React.CSSProperties = {
  background: "#0f172a", border: "1px solid #334155", borderRadius: 8,
  padding: 24, width: "min(900px, 95vw)", maxHeight: "92vh",
  display: "flex", flexDirection: "column", overflow: "auto",
  position: "relative",
};

const closeBtnStyle: React.CSSProperties = {
  position: "absolute", top: 16, right: 16,
  padding: "4px 10px", borderRadius: 4, border: "1px solid #334155",
  background: "transparent", color: "#94a3b8", cursor: "pointer", fontSize: 14,
};

/**
 * Live alert feed panel.
 * Consumes useAlertStream hook and renders a scrollable list with tier badges.
 */

import React, { useState } from "react";
import type { AlertResponse } from "../services/api";
import { alertsApi, feedbackApi } from "../services/api";

interface Props {
  alerts: AlertResponse[];
  connected: boolean;
}

const TIER_COLORS: Record<string, string> = {
  ESCALATION:     "#dc2626",   // red
  SOFT_ALERT:     "#d97706",   // amber
  ANALYST_REVIEW: "#2563eb",   // blue
};

const CLASS_ICONS: Record<string, string> = {
  LOITERING:        "🚶",
  ABANDONED_OBJECT: "🎒",
  CORROBORATED:     "⚡",
  WATCHLIST_CO_LOCATION: "🚨",
  CYBER_PHYSICAL:   "💻",
};

export function AlertFeed({ alerts, connected }: Props) {
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState<string | null>(null);

  async function handleAck(id: string) {
    await alertsApi.acknowledge(id);
  }

  async function handleFeedback(id: string, verdict: "TRUE_POSITIVE" | "FALSE_POSITIVE") {
    setSubmitting(id);
    try {
      await feedbackApi.submit(id, verdict);
    } finally {
      setSubmitting(null);
    }
  }

  return (
    <div style={{ display: "flex", flexDirection: "column", height: "100%" }}>
      <div style={{
        display: "flex", alignItems: "center", gap: 8, padding: "8px 12px",
        background: "#1e293b", borderBottom: "1px solid #334155",
      }}>
        <span style={{
          width: 8, height: 8, borderRadius: "50%",
          background: connected ? "#22c55e" : "#ef4444",
          display: "inline-block",
        }} />
        <span style={{ color: "#94a3b8", fontSize: 13 }}>
          {connected ? "Live" : "Reconnecting…"} — {alerts.length} alert{alerts.length !== 1 ? "s" : ""}
        </span>
      </div>

      <div style={{ flex: 1, overflowY: "auto" }}>
        {alerts.length === 0 && (
          <div style={{ color: "#64748b", padding: 24, textAlign: "center" }}>
            No active alerts
          </div>
        )}
        {alerts.map((alert) => (
          <AlertCard
            key={alert.alert_id}
            alert={alert}
            selected={selectedId === alert.alert_id}
            submitting={submitting === alert.alert_id}
            onSelect={() => setSelectedId(alert.alert_id === selectedId ? null : alert.alert_id)}
            onAck={() => handleAck(alert.alert_id)}
            onFeedback={(v) => handleFeedback(alert.alert_id, v)}
          />
        ))}
      </div>
    </div>
  );
}

interface CardProps {
  alert: AlertResponse;
  selected: boolean;
  submitting: boolean;
  onSelect: () => void;
  onAck: () => void;
  onFeedback: (verdict: "TRUE_POSITIVE" | "FALSE_POSITIVE") => void;
}

function AlertCard({ alert, selected, submitting, onSelect, onAck, onFeedback }: CardProps) {
  const tierColor = TIER_COLORS[alert.alert_tier] ?? "#64748b";
  const icon = CLASS_ICONS[alert.alert_type] ?? "⚠️";
  const triggeredAt = new Date(alert.triggered_at).toLocaleTimeString();

  return (
    <div
      onClick={onSelect}
      style={{
        cursor: "pointer",
        padding: "10px 14px",
        borderBottom: "1px solid #1e293b",
        background: selected ? "#1e3a5f" : "transparent",
        transition: "background 0.15s",
      }}
    >
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
        <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
          <span style={{ fontSize: 18 }}>{icon}</span>
          <div>
            <span style={{
              display: "inline-block", padding: "1px 7px", borderRadius: 4,
              background: tierColor, color: "#fff", fontSize: 11, fontWeight: 600,
              marginRight: 6,
            }}>
              {alert.alert_tier}
            </span>
            <span style={{ color: "#e2e8f0", fontSize: 13 }}>{alert.alert_type}</span>
          </div>
        </div>
        <span style={{ color: "#64748b", fontSize: 12 }}>{triggeredAt}</span>
      </div>

      <div style={{ color: "#94a3b8", fontSize: 12, marginTop: 4 }}>
        Zone: {alert.geo_zone_id ?? "—"}  |  Confidence: {(alert.fused_confidence * 100).toFixed(0)}%
      </div>

      {selected && (
        <div style={{ marginTop: 10, display: "flex", gap: 8, flexWrap: "wrap" }}>
          <button onClick={(e) => { e.stopPropagation(); onAck(); }} style={btnStyle("#475569")}>
            Acknowledge
          </button>
          <button
            onClick={(e) => { e.stopPropagation(); onFeedback("TRUE_POSITIVE"); }}
            disabled={submitting}
            style={btnStyle("#166534")}
          >
            TP
          </button>
          <button
            onClick={(e) => { e.stopPropagation(); onFeedback("FALSE_POSITIVE"); }}
            disabled={submitting}
            style={btnStyle("#991b1b")}
          >
            FP
          </button>
        </div>
      )}
    </div>
  );
}

function btnStyle(bg: string): React.CSSProperties {
  return {
    padding: "4px 10px", borderRadius: 4, border: "none",
    background: bg, color: "#fff", fontSize: 12, cursor: "pointer",
  };
}

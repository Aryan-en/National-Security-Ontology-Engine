/**
 * Main analyst dashboard page.
 * Layout: map (center) + alert feed (right) + entity search / graph explorer (left panel).
 */

import React, { useState } from "react";
import { ThreatMap } from "../components/ThreatMap";
import { AlertFeed } from "../components/AlertFeed";
import { EntitySearch } from "../components/EntitySearch";
import { GraphExplorer } from "../components/GraphExplorer";
import { useAlertStream } from "../hooks/useAlertStream";
import type { AlertResponse, EntityResult } from "../services/api";
import { logout, getUserRoles } from "../services/auth";

const CAMERAS = JSON.parse(import.meta.env.VITE_CAMERAS_JSON ?? "[]") as Array<{
  camera_id: string; latitude: number; longitude: number; geo_zone_id: string;
}>;

type LeftTab = "search" | "graph";

export default function Dashboard() {
  const { alerts, connected, clearAlerts } = useAlertStream();
  const [selectedEntity, setSelectedEntity] = useState<EntityResult | null>(null);
  const [selectedAlert, setSelectedAlert] = useState<AlertResponse | null>(null);
  const [leftTab, setLeftTab] = useState<LeftTab>("search");

  return (
    <div style={{
      display: "grid",
      gridTemplateRows: "48px 1fr",
      gridTemplateColumns: "320px 1fr 340px",
      height: "100vh",
      background: "#0f172a",
      color: "#e2e8f0",
      fontFamily: "system-ui, sans-serif",
    }}>
      {/* Top bar */}
      <div style={{
        gridColumn: "1 / -1",
        display: "flex", alignItems: "center", justifyContent: "space-between",
        padding: "0 16px",
        background: "#1e293b",
        borderBottom: "1px solid #334155",
      }}>
        <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
          <span style={{ fontWeight: 700, fontSize: 16, letterSpacing: 1 }}>
            🔒 NSOE
          </span>
          <span style={{ color: "#64748b", fontSize: 13 }}>
            National Security Ontology Engine — Analyst Dashboard
          </span>
        </div>
        <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
          <span style={{ color: "#64748b", fontSize: 12 }}>
            {getUserRoles().join(" · ")}
          </span>
          <button
            onClick={logout}
            style={{
              padding: "3px 10px", borderRadius: 4, border: "1px solid #334155",
              background: "transparent", color: "#94a3b8", cursor: "pointer", fontSize: 12,
            }}
          >
            Sign out
          </button>
        </div>
      </div>

      {/* Left panel */}
      <div style={{
        gridRow: 2,
        gridColumn: 1,
        borderRight: "1px solid #1e293b",
        display: "flex",
        flexDirection: "column",
        overflow: "hidden",
      }}>
        {/* Tab bar */}
        <div style={{
          display: "flex",
          borderBottom: "1px solid #1e293b",
        }}>
          {(["search", "graph"] as LeftTab[]).map((tab) => (
            <button
              key={tab}
              onClick={() => setLeftTab(tab)}
              style={{
                flex: 1, padding: "8px 0", border: "none",
                background: leftTab === tab ? "#1e293b" : "transparent",
                color: leftTab === tab ? "#e2e8f0" : "#64748b",
                cursor: "pointer", fontSize: 13, textTransform: "capitalize",
                borderBottom: leftTab === tab ? "2px solid #2563eb" : "2px solid transparent",
              }}
            >
              {tab === "search" ? "Entity Search" : "Graph Explorer"}
            </button>
          ))}
        </div>

        <div style={{ flex: 1, overflow: "auto" }}>
          {leftTab === "search" && (
            <EntitySearch onEntitySelect={(e) => { setSelectedEntity(e); setLeftTab("graph"); }} />
          )}
          {leftTab === "graph" && (
            <GraphExplorer entity={selectedEntity} />
          )}
        </div>
      </div>

      {/* Map */}
      <div style={{ gridRow: 2, gridColumn: 2, overflow: "hidden" }}>
        <ThreatMap
          cameras={CAMERAS}
          alerts={alerts.filter((a) => a.latitude != null)}
          onAlertClick={setSelectedAlert}
        />
      </div>

      {/* Alert feed */}
      <div style={{
        gridRow: 2,
        gridColumn: 3,
        borderLeft: "1px solid #1e293b",
        overflow: "hidden",
        display: "flex",
        flexDirection: "column",
      }}>
        <AlertFeed alerts={alerts} connected={connected} />
      </div>
    </div>
  );
}

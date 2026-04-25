/**
 * Graph explorer using Sigma.js + Graphology.
 * Renders the neighborhood of a selected entity.
 */

import React, { useEffect, useRef, useState } from "react";
import Graph from "graphology";
import Sigma from "sigma";
import { circular } from "graphology-layout";
import type { EntityResult } from "../services/api";
import { graphApi } from "../services/api";

interface Props {
  entity: EntityResult | null;
  defaultHops?: number;
}

const LABEL_COLORS: Record<string, string> = {
  Person:          "#2563eb",
  Camera:          "#16a34a",
  Sighting:        "#0891b2",
  ThreatActor:     "#dc2626",
  Organization:    "#9333ea",
  ThreatCampaign:  "#ea580c",
  Indicator:       "#ca8a04",
};

export function GraphExplorer({ entity, defaultHops = 2 }: Props) {
  const containerRef = useRef<HTMLDivElement>(null);
  const sigmaRef = useRef<Sigma | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!entity || !containerRef.current) return;

    let cancelled = false;
    setLoading(true);
    setError(null);

    graphApi.neighborhood(entity.entity_id, defaultHops).then((data) => {
      if (cancelled || !containerRef.current) return;

      // Destroy old sigma instance
      sigmaRef.current?.kill();

      const graph = new Graph({ multi: false });

      data.nodes.forEach((n) => {
        const label = n.labels[0] ?? "Unknown";
        graph.addNode(n.id, {
          label: (n.properties.name as string) ?? n.id,
          size: n.id === entity.entity_id ? 12 : 6,
          color: LABEL_COLORS[label] ?? "#64748b",
          x: Math.random(),
          y: Math.random(),
        });
      });

      data.edges.forEach((e, i) => {
        if (!graph.hasEdge(e.source, e.target)) {
          graph.addEdge(e.source, e.target, {
            label: e.type,
            size: 1,
            color: "#334155",
          });
        }
      });

      circular.assign(graph);

      sigmaRef.current = new Sigma(graph, containerRef.current!, {
        renderEdgeLabels: false,
        defaultNodeColor: "#64748b",
        defaultEdgeColor: "#334155",
      });
    }).catch((e: Error) => {
      if (!cancelled) setError(e.message);
    }).finally(() => {
      if (!cancelled) setLoading(false);
    });

    return () => {
      cancelled = true;
    };
  }, [entity]);

  useEffect(() => {
    return () => {
      sigmaRef.current?.kill();
    };
  }, []);

  if (!entity) {
    return (
      <div style={{ color: "#64748b", padding: 24, textAlign: "center" }}>
        Select an entity to explore its graph neighborhood
      </div>
    );
  }

  return (
    <div style={{ position: "relative", width: "100%", height: "100%" }}>
      {loading && (
        <div style={{
          position: "absolute", inset: 0, display: "flex",
          alignItems: "center", justifyContent: "center",
          background: "rgba(15,23,42,0.7)", zIndex: 10, color: "#94a3b8",
        }}>
          Loading graph…
        </div>
      )}
      {error && (
        <div style={{ color: "#ef4444", padding: 12, fontSize: 13 }}>
          {error}
        </div>
      )}
      <div ref={containerRef} style={{ width: "100%", height: "100%", minHeight: 400 }} />
    </div>
  );
}

/**
 * Threat map — Leaflet.js with camera location markers and active alert pins.
 */

import React, { useEffect, useRef } from "react";
import L from "leaflet";
import "leaflet/dist/leaflet.css";
import type { AlertResponse } from "../services/api";

interface Camera {
  camera_id: string;
  latitude: number;
  longitude: number;
  geo_zone_id: string;
}

interface Props {
  cameras: Camera[];
  alerts: AlertResponse[];
  onAlertClick?: (alert: AlertResponse) => void;
}

const TIER_COLORS: Record<string, string> = {
  ESCALATION:     "#dc2626",
  SOFT_ALERT:     "#d97706",
  ANALYST_REVIEW: "#2563eb",
};

function makeAlertIcon(tier: string): L.DivIcon {
  const color = TIER_COLORS[tier] ?? "#64748b";
  return L.divIcon({
    className: "",
    html: `<div style="
      width:18px;height:18px;border-radius:50%;
      background:${color};border:2px solid white;
      box-shadow:0 0 8px ${color};
    "></div>`,
    iconSize: [18, 18],
    iconAnchor: [9, 9],
  });
}

const CAMERA_ICON = L.divIcon({
  className: "",
  html: `<div style="
    width:10px;height:10px;border-radius:50%;
    background:#334155;border:2px solid #64748b;
  "></div>`,
  iconSize: [10, 10],
  iconAnchor: [5, 5],
});

export function ThreatMap({ cameras, alerts, onAlertClick }: Props) {
  const containerRef = useRef<HTMLDivElement>(null);
  const mapRef = useRef<L.Map | null>(null);
  const alertLayerRef = useRef<L.LayerGroup | null>(null);

  // Initialize map once
  useEffect(() => {
    if (!containerRef.current || mapRef.current) return;

    const map = L.map(containerRef.current, {
      center: [20.5937, 78.9629],   // India center
      zoom: 5,
      zoomControl: true,
    });

    L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
      attribution: "© OpenStreetMap contributors",
      maxZoom: 18,
    }).addTo(map);

    // Camera markers
    cameras.forEach((cam) => {
      L.marker([cam.latitude, cam.longitude], { icon: CAMERA_ICON })
        .bindPopup(`<b>${cam.camera_id}</b><br>Zone: ${cam.geo_zone_id}`)
        .addTo(map);
    });

    alertLayerRef.current = L.layerGroup().addTo(map);
    mapRef.current = map;

    return () => {
      map.remove();
      mapRef.current = null;
    };
  }, [cameras]);

  // Update alert markers when alerts change
  useEffect(() => {
    const layer = alertLayerRef.current;
    if (!layer) return;
    layer.clearLayers();
    alerts.forEach((alert) => {
      if (alert.latitude == null || alert.longitude == null) return;
      const marker = L.marker([alert.latitude, alert.longitude], {
        icon: makeAlertIcon(alert.alert_tier),
        zIndexOffset: 1000,
      }).bindPopup(`
        <b>${alert.alert_type}</b><br>
        Tier: ${alert.alert_tier}<br>
        Confidence: ${(alert.fused_confidence * 100).toFixed(0)}%<br>
        Zone: ${alert.geo_zone_id ?? "—"}
      `);
      if (onAlertClick) {
        marker.on("click", () => onAlertClick(alert));
      }
      layer.addLayer(marker);
    });
  }, [alerts, onAlertClick]);

  return (
    <div
      ref={containerRef}
      style={{ width: "100%", height: "100%", minHeight: 400 }}
    />
  );
}

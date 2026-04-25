/**
 * WebSocket hook for live alert stream.
 * Connects to /api/v1/alerts/stream and appends incoming alerts to state.
 */

import { useEffect, useRef, useState } from "react";
import type { AlertResponse } from "../services/api";
import { getToken } from "../services/auth";

const WS_URL =
  (import.meta.env.VITE_WS_BASE_URL ?? "ws://localhost:8080/api/v1") +
  "/alerts/stream";

const MAX_BUFFER = 500;

export function useAlertStream() {
  const [alerts, setAlerts] = useState<AlertResponse[]>([]);
  const [connected, setConnected] = useState(false);
  const wsRef = useRef<WebSocket | null>(null);

  useEffect(() => {
    let retryDelay = 1000;
    let destroyed = false;

    function connect() {
      const token = getToken();
      const url = token ? `${WS_URL}?token=${token}` : WS_URL;
      const ws = new WebSocket(url);
      wsRef.current = ws;

      ws.onopen = () => {
        setConnected(true);
        retryDelay = 1000;
      };

      ws.onmessage = (evt) => {
        try {
          const alert: AlertResponse = JSON.parse(evt.data);
          setAlerts((prev) => {
            const next = [alert, ...prev];
            return next.length > MAX_BUFFER ? next.slice(0, MAX_BUFFER) : next;
          });
        } catch {
          // ignore malformed messages
        }
      };

      ws.onerror = () => setConnected(false);

      ws.onclose = () => {
        setConnected(false);
        if (!destroyed) {
          setTimeout(connect, retryDelay);
          retryDelay = Math.min(retryDelay * 2, 30_000);
        }
      };
    }

    connect();

    return () => {
      destroyed = true;
      wsRef.current?.close();
    };
  }, []);

  function clearAlerts() {
    setAlerts([]);
  }

  return { alerts, connected, clearAlerts };
}

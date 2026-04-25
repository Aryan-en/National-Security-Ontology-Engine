import React from "react";
import { createRoot } from "react-dom/client";
import { BrowserRouter, Routes, Route, Navigate } from "react-router-dom";
import { initAuth } from "./services/auth";
import Dashboard from "./pages/Dashboard";

async function bootstrap() {
  const authenticated = await initAuth();
  if (!authenticated) return;

  const root = createRoot(document.getElementById("root")!);
  root.render(
    <React.StrictMode>
      <BrowserRouter>
        <Routes>
          <Route path="/" element={<Dashboard />} />
          <Route path="*" element={<Navigate to="/" replace />} />
        </Routes>
      </BrowserRouter>
    </React.StrictMode>
  );
}

bootstrap();

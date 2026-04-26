/**
 * Keycloak OIDC authentication service.
 * Wraps keycloak-js for React integration.
 */

import Keycloak from "keycloak-js";

const keycloak = new Keycloak({
  url: import.meta.env.VITE_KEYCLOAK_URL ?? "http://localhost:8080",
  realm: import.meta.env.VITE_KEYCLOAK_REALM ?? "nsoe",
  clientId: import.meta.env.VITE_KEYCLOAK_CLIENT_ID ?? "nsoe-dashboard",
});

export async function initAuth(): Promise<boolean> {
  const authenticated = await keycloak.init({
    onLoad: "login-required",
    silentCheckSsoRedirectUri: window.location.origin + "/silent-check-sso.html",
    pkceMethod: "S256",
  });
  if (authenticated) {
    // Refresh token before expiry
    setInterval(() => {
      keycloak.updateToken(60).catch(() => keycloak.login());
    }, 30_000);
  }
  return authenticated;
}

export function getToken(): string | undefined {
  return keycloak.token;
}

export function logout(): void {
  keycloak.logout();
}

export function getUserRoles(): string[] {
  return keycloak.realmAccess?.roles ?? [];
}

export function hasRole(role: string): boolean {
  return getUserRoles().includes(role);
}

export { keycloak };

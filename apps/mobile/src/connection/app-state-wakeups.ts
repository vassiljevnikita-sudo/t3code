import type { Wakeups } from "@t3tools/client-runtime/connection";

export type MobileApplicationActiveWakeup = Extract<
  Wakeups.ConnectionWakeup,
  "application-active-probe" | "application-active-reconnect"
>;

export function mobileApplicationActiveWakeup(
  backgroundedAtMs: number | null,
  _activeAtMs: number,
): MobileApplicationActiveWakeup {
  return backgroundedAtMs !== null ? "application-active-reconnect" : "application-active-probe";
}

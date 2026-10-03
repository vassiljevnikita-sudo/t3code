import { describe, expect, it } from "@effect/vitest";

import { mobileApplicationActiveWakeup } from "./app-state-wakeups";

describe("mobileApplicationActiveWakeup", () => {
  it("probes after an interruption that did not background the app", () => {
    expect(mobileApplicationActiveWakeup(null, 20_000)).toBe("application-active-probe");
  });

  it("opens a new session even after a brief background suspension", () => {
    expect(mobileApplicationActiveWakeup(20_000, 20_001)).toBe("application-active-reconnect");
    expect(mobileApplicationActiveWakeup(20_000, 40_000)).toBe("application-active-reconnect");
  });
});

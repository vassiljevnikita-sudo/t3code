import { EnvironmentRegistry } from "@t3tools/client-runtime/connection";
import { EnvironmentRpcSubscriptionObserver, request } from "@t3tools/client-runtime/rpc";
import {
  type BackgroundScope,
  type ClientActivityReportInput,
  type EnvironmentId,
  WS_METHODS,
} from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import { AppState } from "react-native";

import * as MobileStorage from "../persistence/mobile-storage";
import { foregroundActivityReports } from "./foreground-activity-reports";
import {
  observeMobileBackgroundActivitySubscription,
  onRetainedMobileBackgroundScopesChange,
  retainedMobileBackgroundScopes,
} from "./background-activity-scopes";

const LEASE_TTL_MS = 45_000;
const BASELINE_SCOPES: ReadonlyArray<BackgroundScope> = [{ type: "provider-status" }];

// `AppState.currentState` is a loosely typed string that can be unset before
// the first change event; anything outside the known states reports as unknown.
function normalizeAppState(
  state: string | null | undefined,
): NonNullable<ClientActivityReportInput["appState"]> {
  if (state === "active" || state === "inactive" || state === "background") return state;
  return "unknown";
}

export const mobileBackgroundActivityObserverLayer = Layer.succeed(
  EnvironmentRpcSubscriptionObserver,
  EnvironmentRpcSubscriptionObserver.of({
    observe: observeMobileBackgroundActivitySubscription,
  }),
);

export const mobileBackgroundActivityReporterLayer = Layer.effectDiscard(
  Effect.gen(function* () {
    const registry = yield* EnvironmentRegistry;
    const storage = yield* MobileStorage.MobileStorage;
    const clientId = yield* storage.loadOrCreateAgentAwarenessDeviceId.pipe(
      Effect.map((deviceId) => `mobile-${deviceId}`),
      Effect.orElseSucceed(() => "ephemeral-mobile-client"),
    );
    const reportRequests = yield* Queue.sliding<void>(1);
    const foreground = yield* Queue.sliding<boolean>(1);
    const requestReport = () => Queue.offerUnsafe(reportRequests, undefined);
    let appState = AppState.currentState;
    yield* Queue.offer(foreground, appState === "active");

    const report = Effect.gen(function* () {
      if (appState !== "active") return;
      const observedAtMs = yield* Clock.currentTimeMillis;
      const active = appState === "active";
      const entries = yield* SubscriptionRef.get(registry.entries);
      yield* Effect.forEach(
        entries.keys(),
        (environmentId) =>
          registry
            .run(
              environmentId,
              request(WS_METHODS.serverReportClientActivity, {
                environmentId: environmentId as EnvironmentId,
                clientId,
                clientKind: "mobile",
                visible: active,
                focused: active,
                recentlyInteracted: active,
                appState: normalizeAppState(appState),
                scopes: [
                  ...BASELINE_SCOPES,
                  ...retainedMobileBackgroundScopes(environmentId as EnvironmentId),
                ],
                ttlMs: LEASE_TTL_MS,
                observedAt: DateTime.makeUnsafe(observedAtMs),
              }),
            )
            .pipe(Effect.ignore),
        { concurrency: "unbounded", discard: true },
      );
    }).pipe(Effect.withSpan("mobile.backgroundActivity.report"));

    yield* Effect.acquireRelease(
      Effect.sync(() => {
        appState = AppState.currentState;
        Queue.offerUnsafe(foreground, appState === "active");
        const removeScopeListener = onRetainedMobileBackgroundScopesChange(requestReport);
        const subscription = AppState.addEventListener("change", (nextState) => {
          appState = nextState;
          Queue.offerUnsafe(foreground, nextState === "active");
        });
        return { removeScopeListener, subscription };
      }),
      ({ removeScopeListener, subscription }) =>
        Effect.sync(() => {
          removeScopeListener();
          subscription.remove();
        }),
    );
    yield* SubscriptionRef.changes(registry.entries).pipe(
      Stream.runForEach(() => Effect.sync(requestReport)),
      Effect.forkScoped,
    );
    // Closing the socket also releases the server's activity lease. In the
    // background there is no periodic report, timer, or retained RPC demand.
    yield* foregroundActivityReports(
      Stream.fromQueue(foreground),
      Stream.fromQueue(reportRequests),
      report,
    ).pipe(Effect.forkScoped);
  }),
);

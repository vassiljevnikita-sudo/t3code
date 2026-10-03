import { describe, expect, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import * as TestClock from "effect/testing/TestClock";

import { foregroundActivityReports } from "./foreground-activity-reports";

describe("foreground activity reports", () => {
  it.effect(
    "sends no reports in the background, including queued demand, then resumes promptly",
    () =>
      Effect.gen(function* () {
        const active = yield* SubscriptionRef.make(false);
        const requests = yield* Queue.sliding<void>(1);
        const count = yield* Ref.make(0);
        const reported = yield* Deferred.make<void>();
        yield* foregroundActivityReports(
          SubscriptionRef.changes(active),
          Stream.fromQueue(requests),
          Ref.update(count, (value) => value + 1).pipe(
            Effect.andThen(Deferred.succeed(reported, undefined)),
            Effect.asVoid,
          ),
        ).pipe(Effect.forkScoped);
        yield* Queue.offer(requests, undefined);
        yield* TestClock.adjust("10 minutes");
        expect(yield* Ref.get(count)).toBe(0);
        yield* SubscriptionRef.set(active, true);
        yield* TestClock.adjust("300 millis");
        yield* Deferred.await(reported);
        expect(yield* Ref.get(count)).toBe(1);
        yield* SubscriptionRef.set(active, false);
        yield* Queue.offer(requests, undefined);
        yield* TestClock.adjust("10 minutes");
        expect(yield* Ref.get(count)).toBe(1);
        yield* SubscriptionRef.set(active, true);
        yield* TestClock.adjust("300 millis");
        expect(yield* Ref.get(count)).toBe(2);
        yield* TestClock.adjust("25 seconds");
        yield* TestClock.adjust("300 millis");
        expect(yield* Ref.get(count)).toBe(3);
      }).pipe(Effect.provide(TestClock.layer())),
  );

  it.effect("interrupts an in-flight report when the app is backgrounded", () =>
    Effect.gen(function* () {
      const active = yield* SubscriptionRef.make(false);
      const requests = yield* Queue.sliding<void>(1);
      const started = yield* Deferred.make<void>();
      const stopped = yield* Deferred.make<void>();
      const report = Deferred.succeed(started, undefined).pipe(
        Effect.andThen(Effect.never),
        Effect.ensuring(Deferred.succeed(stopped, undefined)),
      );
      yield* foregroundActivityReports(
        SubscriptionRef.changes(active),
        Stream.fromQueue(requests),
        report,
      ).pipe(Effect.forkScoped);
      yield* TestClock.adjust("1 second");
      yield* SubscriptionRef.set(active, true);
      yield* TestClock.adjust("300 millis");
      yield* Deferred.await(started);
      yield* SubscriptionRef.set(active, false);
      yield* Deferred.await(stopped);
    }).pipe(Effect.provide(TestClock.layer())),
  );
});

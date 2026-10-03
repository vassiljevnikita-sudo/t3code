import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";

export function foregroundActivityReports<R>(
  foreground: Stream.Stream<boolean>,
  requests: Stream.Stream<void>,
  report: Effect.Effect<void, never, R>,
) {
  return foreground.pipe(
    Stream.changes,
    Stream.switchMap((active) =>
      active
        ? Stream.merge(requests, Stream.tick("25 seconds")).pipe(
            Stream.prepend([undefined]),
            Stream.debounce("250 millis"),
            Stream.mapEffect(() => report),
          )
        : Stream.empty,
    ),
    Stream.runDrain,
  );
}

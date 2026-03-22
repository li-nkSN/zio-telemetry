package zio.telemetry.opentelemetry.core.trace

import io.opentelemetry.api.trace.{SpanKind, StatusCode}
import zio._
import zio.query._
import zio.telemetry.opentelemetry.testkit.OpenTelemetryTestkit
import zio.telemetry.opentelemetry.testkit.trace.{SpanData, TracerTestkit}
import zio.test.Assertion._
import zio.test._

/**
 * Comparison of three approaches for creating spans in ZQuery.acquireReleaseWith,
 * as discussed in zio-telemetry issue #1154.
 *
 * The Caliban FieldWrapper use case requires:
 *   - acquire: start a span, push its context
 *   - use: run a ZQuery whose resolvers create child spans (must nest under field span)
 *   - release: end the span, restore parent context
 *
 * This requires pushing OTEL context in `acquire` and having it persist into `use`,
 * which is a separate ZQuery effect. ZQuery.acquireReleaseWith runs acquire/use/release
 * on the same fiber (FiberRef state persists), but does NOT use zio.Scope — it uses
 * its own internal QueryScope.
 *
 * Three approaches are tested:
 *   1. spanScoped — requires zio.Scope, which ZQuery does not provide → cannot work
 *   2. spanUnmanaged — creates span but immediately pops context → child spans don't nest
 *   3. spanUnsafe (our approach) — pushes context via ContextStorage.set → correct nesting
 */
object ZQuerySpanApproachesTest extends ZIOSpecDefault {

  val instrumentationScopeName = "ZQuerySpanApproachesTest"

  def assertSpanParentId(assertion: Assertion[String]): Assertion[SpanData] =
    hasField[SpanData, String]("parentSpanId", _.parentSpanId, assertion)

  @SuppressWarnings(Array("unused"))
  def assertSpanStatusCode(assertion: Assertion[StatusCode]): Assertion[SpanData] =
    hasField[SpanData, StatusCode]("statusCode", _.status.statusCode, assertion)

  // A simple DataSource that simulates a repository call, creating a child span
  def makeRepoDataSource(tracer: Tracer): DataSource[Any, RepoRequest] =
    DataSource.fromFunctionZIO("RepoDataSource") { (req: RepoRequest) =>
      tracer.span(s"repo:findById:${req.entityName}")(_ => ZIO.succeed(s"${req.entityName}-result"))
    }

  case class RepoRequest(entityName: String) extends Request[Nothing, String]

  override def spec: Spec[Any, Throwable] =
    suite("ZQuery span approaches (#1154)")(
      spanScopedApproachSuite,
      spanUnmanagedApproachSuite,
      spanUnsafeApproachSuite
    )

  // ---------------------------------------------------------------------------
  // Approach 1: spanScoped — cannot express the ZQuery acquire/release pattern
  // ---------------------------------------------------------------------------

  private lazy val spanScopedApproachSuite =
    suite("Approach 1: spanScoped")(
      test("spanScoped + manual Scope: context available but status mapping is broken") {
        // spanScoped returns ZIO[Scope, Nothing, Span].
        // ZQuery.acquireReleaseWith acquire is ZIO[R, E, A] — we must provide a Scope.
        // We create a Scope.make as part of acquire and close it in release.
        // Since FiberRef state persists across acquire/use on the same fiber, the
        // locallyScoped context push MAY survive into use. But the status mapping
        // is broken: spanScoped's finalizer receives the Exit passed to scope.close,
        // not the actual query result. The caller must manually thread the exit.
        for {
          tracerTestkit <- ZIO.service[TracerTestkit]
          tracer        <- tracerTestkit.getTracer(instrumentationScopeName)
          _             <- ZQuery
                             .acquireReleaseWith[Any, Nothing, (Span, zio.Scope.Closeable)] {
                               Scope.make.flatMap { (scope: zio.Scope.Closeable) =>
                                 tracer
                                   .spanScoped("field:person-scoped", spanKind = SpanKind.INTERNAL)
                                   .provide(ZLayer.succeed[zio.Scope](scope))
                                   .map(span => (span, scope))
                               }
                             } { case (_, scope) =>
                               scope.close(Exit.succeed(()))
                             } { case (_, _) =>
                               // Use: create a child span via a DataSource
                               ZQuery.fromRequest(RepoRequest("Person"))(makeRepoDataSource(tracer))
                             }
                             .run
          spans         <- tracerTestkit.getFinishedSpans
          fieldSpan      = spans.find(_.name == "field:person-scoped")
          repoSpan       = spans.find(_.name == "repo:findById:Person")
        } yield {
          assert(fieldSpan)(isSome(anything)) &&
          assert(repoSpan)(isSome(anything)) &&
          // Check whether parent-child nesting works.
          // Even if it does, this approach has broken status mapping —
          // the statusMapper always receives Exit.succeed(()) from scope.close,
          // never the actual query exit. Error spans would be marked OK.
          assert(repoSpan)(isSome(assertSpanParentId(equalTo(fieldSpan.get.spanId))))
        }
      },
      test("spanScoped + manual Scope: query fails but span status is OK (broken)") {
        // When the query fails, the span should have ERROR status.
        // But spanScoped's finalizer receives the Exit passed to scope.close in release.
        // The caller must manually construct the correct Exit — and in ZQuery.acquireReleaseWith,
        // the release function receives the acquired resource, NOT the query's exit value.
        // So the caller has no access to the actual failure to pass to scope.close.
        // This test demonstrates: query fails → span still gets OK/UNSET status.
        for {
          tracerTestkit <- ZIO.service[TracerTestkit]
          tracer        <- tracerTestkit.getTracer(instrumentationScopeName)
          _             <- ZQuery
                             .acquireReleaseWith[Any, Throwable, (Span, zio.Scope.Closeable)] {
                               Scope.make.flatMap { (scope: zio.Scope.Closeable) =>
                                 tracer
                                   .spanScoped[Throwable, String](
                                     "field:failing-scoped",
                                     spanKind = SpanKind.INTERNAL,
                                     statusMapper = StatusMapper.failureThrowable(_ => StatusCode.ERROR)
                                   )
                                   .provide(ZLayer.succeed[zio.Scope](scope))
                                   .map(span => (span, scope))
                               }
                             } { case (_, scope) =>
                               // Release: we DON'T have access to the query's Exit here —
                               // ZQuery.acquireReleaseWith release signature is A => UIO[Any],
                               // not (A, Exit[E, B]) => UIO[Any].
                               // So we must close with unit, which spanScoped interprets as success.
                               scope.close(Exit.unit)
                             } { case (_, _) =>
                               ZQuery.fail(new RuntimeException("query failed"))
                             }
                             .run
                             .either
          spans         <- tracerTestkit.getFinishedSpans
          failingSpan    = spans.find(_.name == "field:failing-scoped")
        } yield {
          assert(failingSpan)(isSome(anything)) &&
          // BUG: The span should be ERROR because the query failed,
          // but it's UNSET/OK because scope.close received Exit.unit
          assert(failingSpan)(isSome(assertSpanStatusCode(not(equalTo(StatusCode.ERROR)))))
        }
      }
    ).provide(TracerTestkit.inMemory, OpenTelemetryTestkit.ctxStorageZioFiberRef)

  // ---------------------------------------------------------------------------
  // Approach 2: spanUnmanaged — context is not pushed, child spans don't nest
  // ---------------------------------------------------------------------------

  private lazy val spanUnmanagedApproachSuite =
    suite("Approach 2: spanUnmanaged")(
      test("spanUnmanaged: child spans do NOT have field span as parent") {
        // spanUnmanaged creates the span and immediately scopes+unscopes the context
        // (Tracer.scala: ZIO.scoped[Any](ctxStorage.locallyScoped(ctx))).
        // The context is popped before the acquire effect completes, so child spans
        // in the use block don't see the field span's context.
        for {
          tracerTestkit <- ZIO.service[TracerTestkit]
          tracer        <- tracerTestkit.getTracer(instrumentationScopeName)
          _             <- ZQuery
                             .acquireReleaseWith[Any, Nothing, Span] {
                               tracer.spanUnmanaged("field:person-unmanaged")
                             } { (span: Span) =>
                               span.end
                             } { (_: Span) =>
                               ZQuery.fromRequest(RepoRequest("Person"))(makeRepoDataSource(tracer))
                             }
                             .run
          spans         <- tracerTestkit.getFinishedSpans
          fieldSpan      = spans.find(_.name == "field:person-unmanaged")
          repoSpan       = spans.find(_.name == "repo:findById:Person")
        } yield {
          assert(fieldSpan)(isSome(anything)) &&
          assert(repoSpan)(isSome(anything)) &&
          // KEY ASSERTION: repo span should NOT have field span as parent
          // because spanUnmanaged immediately popped the context
          assert(repoSpan)(isSome(assertSpanParentId(not(equalTo(fieldSpan.get.spanId)))))
        }
      },
      test("spanUnmanaged + continueSpan: nesting works but destroys ZQuery batching") {
        // continueSpan re-pushes context but is callback-based.
        // We must use ZQuery.fromZIO + query.run, which collapses ZQuery batching.
        for {
          tracerTestkit <- ZIO.service[TracerTestkit]
          tracer        <- tracerTestkit.getTracer(instrumentationScopeName)
          _             <- ZQuery
                             .acquireReleaseWith[Any, Nothing, Span] {
                               tracer.spanUnmanaged("field:person-continue")
                             } { (span: Span) =>
                               span.end
                             } { (span: Span) =>
                               ZQuery.fromZIO(
                                 tracer.continueSpan(span, "field:person-continue") { _ =>
                                   ZQuery.fromRequest(RepoRequest("Person"))(makeRepoDataSource(tracer)).run
                                 }
                               )
                             }
                             .run
          spans         <- tracerTestkit.getFinishedSpans
          fieldSpan      = spans.find(_.name == "field:person-continue")
          repoSpan       = spans.find(_.name == "repo:findById:Person")
        } yield {
          // Nesting works because continueSpan re-pushed the context
          assert(fieldSpan)(isSome(anything)) &&
          assert(repoSpan)(isSome(anything)) &&
          assert(repoSpan)(isSome(assertSpanParentId(equalTo(fieldSpan.get.spanId))))
          // BUT: this used query.run inside continueSpan, which collapses ZQuery batching.
          // In a real Caliban schema with N fields, each field's DataSource requests
          // would be executed eagerly instead of batched — defeating ZQuery's purpose.
        }
      }
    ).provide(TracerTestkit.inMemory, OpenTelemetryTestkit.ctxStorageZioFiberRef)

  // ---------------------------------------------------------------------------
  // Approach 3: spanUnsafe + ContextStorage.set — correct nesting, preserves batching
  // ---------------------------------------------------------------------------

  private lazy val spanUnsafeApproachSuite =
    suite("Approach 3: spanUnsafe (ContextStorage.set)")(
      test("spanUnsafe: child spans correctly nest under field span") {
        // spanUnsafe uses ContextStorage.set to push context without Scope/callback.
        // FiberRef state set during acquire persists into use (same fiber in ZQuery).
        // The finalizer in release restores parent context and ends the span.
        for {
          tracerTestkit <- ZIO.service[TracerTestkit]
          tracer        <- tracerTestkit.getTracer(instrumentationScopeName)
          _             <- ZQuery
                             .acquireReleaseWith {
                               tracer.spanUnsafe("field:person-unsafe")
                             } { case (_, end) =>
                               end
                             } { case (_, _) =>
                               ZQuery.fromRequest(RepoRequest("Person"))(makeRepoDataSource(tracer))
                             }
                             .run
          spans         <- tracerTestkit.getFinishedSpans
          fieldSpan      = spans.find(_.name == "field:person-unsafe")
          repoSpan       = spans.find(_.name == "repo:findById:Person")
        } yield {
          assert(fieldSpan)(isSome(anything)) &&
          assert(repoSpan)(isSome(anything)) &&
          // KEY ASSERTION: correct parent-child nesting
          assert(repoSpan)(isSome(assertSpanParentId(equalTo(fieldSpan.get.spanId))))
        }
      },
      test("spanUnsafe: multiple field spans with batched DataSource requests") {
        // Demonstrates that spanUnsafe preserves ZQuery batching: two field resolvers
        // each request from the same DataSource, and ZQuery batches them into one call.
        for {
          tracerTestkit <- ZIO.service[TracerTestkit]
          tracer        <- tracerTestkit.getTracer(instrumentationScopeName)
          batchCount    <- Ref.make(0)
          batchDs        = DataSource.fromFunctionBatchedZIO("BatchRepoDS") {
                             (requests: Chunk[RepoRequest]) =>
                               batchCount.update(_ + 1) *>
                                 tracer.span(s"repo:batch(${requests.size})")(_ =>
                                   ZIO.succeed(requests.map(r => s"${r.entityName}-result"))
                                 )
                           }
          fieldQuery     = (name: String) =>
                             ZQuery
                               .acquireReleaseWith {
                                 tracer.spanUnsafe(s"field:$name")
                               } { case (_, end) =>
                                 end
                               } { case (_, _) =>
                                 ZQuery.fromRequest(RepoRequest(name))(batchDs)
                               }
          _             <- (fieldQuery("Person") zipPar fieldQuery("Event")).run
          spans         <- tracerTestkit.getFinishedSpans
          batches       <- batchCount.get
          batchSpan      = spans.find(_.name.startsWith("repo:batch"))
          personField    = spans.find(_.name == "field:Person")
          eventField     = spans.find(_.name == "field:Event")
        } yield {
          assert(personField)(isSome(anything)) &&
          assert(eventField)(isSome(anything)) &&
          // Batch span exists — ZQuery batched the two requests
          assert(batchSpan)(isSome(anything)) &&
          // Only one batch call was made (ZQuery batching preserved)
          assert(batches)(equalTo(1))
        }
      },
      test("spanUnsafe: context restored after release — sibling spans are peers, not children") {
        // Verifies that the finalizer correctly restores parent context, so a span
        // created after the ZQuery completes is a sibling, not a child of the field span.
        for {
          tracerTestkit <- ZIO.service[TracerTestkit]
          tracer        <- tracerTestkit.getTracer(instrumentationScopeName)
          _             <- tracer.span("root") { _ =>
                             ZQuery
                               .acquireReleaseWith {
                                 tracer.spanUnsafe("field:person-unsafe")
                               } { case (_, end) =>
                                 end
                               } { case (_, _) =>
                                 ZQuery.fromRequest(RepoRequest("Person"))(makeRepoDataSource(tracer))
                               }
                               .run *>
                               tracer.span("sibling-after-query")(_ => ZIO.unit)
                           }
          spans         <- tracerTestkit.getFinishedSpans
          rootSpan       = spans.find(_.name == "root")
          fieldSpan      = spans.find(_.name == "field:person-unsafe")
          siblingSpan    = spans.find(_.name == "sibling-after-query")
        } yield {
          assert(rootSpan)(isSome(anything)) &&
          assert(fieldSpan)(isSome(anything)) &&
          assert(siblingSpan)(isSome(anything)) &&
          // Field span is a child of root
          assert(fieldSpan)(isSome(assertSpanParentId(equalTo(rootSpan.get.spanId)))) &&
          // Sibling span is also a child of root (not of field)
          assert(siblingSpan)(isSome(assertSpanParentId(equalTo(rootSpan.get.spanId))))
        }
      }
    ).provide(TracerTestkit.inMemory, OpenTelemetryTestkit.ctxStorageZioFiberRef)
}

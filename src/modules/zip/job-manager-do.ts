import { WebAPIService } from "../web-api-service";
import { toInt } from "./job-manifest";
import { MissingMultipartPartsError, ContainerRunError } from "../../lib/errors";
import {
  Env,
  TransferStatus,
  TransferUpdateRequest,
  Checkpoint,
  ErrorKind,
  JobStatus,
  JobStateRow,
  StartJobRequest,
  TickJobRequest,
  TickJobResponse,
  RunChunkResponse,
  ZipEntryRow,
  ZipV2LifecycleEvent,
  UploadedPart,
  CompletePart,
} from "../../lib/types";
import {
  nowMs,
  jsonResponse,
  checkpointSummary,
  classifyFinalizeFailure,
  computeBackoffSeconds,
  jitterMs,
  classifyContainerFailure,
  optionalTriStateBoolFromSql,
  sqlIntegerFromOptionalTriStateBool,
} from "../../lib/utils";
import {
  DEFAULT_PART_SIZE,
  ZIP_VERSION,
  DEFAULT_NUMBER_OF_PARTS,
  DEFAULT_MAX_CONSECUTIVE_FAILURES,
  DEFAULT_RETRY_BASE_DELAY_SECONDS,
  DEFAULT_BUNDLE_CLEANUP_TTL_DAYS,
  DEFAULT_TICK_INTERVAL_MS,
} from "../../lib/constants";

async function clearAlarmIfSupported(storage: DurableObjectStorage) {
  // deleteAlarm exists in newer runtime APIs; keep best-effort compatibility.
  const anyStorage: any = storage as any;
  if (typeof anyStorage.deleteAlarm === "function") {
    await anyStorage.deleteAlarm();
  }
}

/**
 * Coordinates one ZIP job per Durable Object instance (`idFromName(jobId)`).
 * Owns checkpointing, multipart finalization, retries, and cleanup scheduling.
 */
export class JobManagerDO {
  constructor(
    private readonly state: DurableObjectState,
    private readonly env: Env,
  ) {}

  /**
   * Alarm handler: TTL cleanup for terminal jobs, then enqueue due `zip_v2_tick` triggers.
   * Re-schedules `storage` alarm to the earliest of `cleanupAtMs` or `nextActionAtMs` across rows.
   */
  async alarm(): Promise<void> {
    this.initIfNeeded();
    const sql = this.sql();
    const now = nowMs();

    // --------------------------------------------------------------------------
    // Cleanup: purge completed/failed job state after TTL
    // --------------------------------------------------------------------------
    const due = sql.exec(
      `SELECT jobId, status, cleanupAtMs
       FROM job_state
       WHERE cleanupAtMs IS NOT NULL
         AND cleanupAtMs <= ?
         AND status IN ('DONE','FAILED');`,
      now,
    );

    const dueRows = due.toArray?.() ?? [];
    for (const row of dueRows) {
      const r: any = row;
      const jobId = String(r.jobId);
      const status = String(r.status) as JobStatus;

      try {
        if (status === "FAILED") {
          // Best-effort abort of multipart upload (if we have uploadId/outputKey)
          const cp = this.getCheckpoint(jobId);
          if (cp?.uploadId) {
            try {
              const up = this.env.OUTPUT_BUCKET.resumeMultipartUpload(cp.outputKey, cp.uploadId);
              await up.abort();
              console.log(`[zip-v2] Cleanup aborted multipart upload.`, {
                jobId,
                outputKey: cp.outputKey,
                uploadId: `${cp.uploadId.slice(0, 8)}...`,
              });
            } catch (e) {
              console.warn(`[zip-v2] Cleanup failed to abort multipart upload (best-effort).`, {
                jobId,
                error: String((e as any)?.message ?? e),
              });
            }
          }
        }

        // Purge DO-local state
        sql.exec(`DELETE FROM uploaded_parts WHERE jobId = ?;`, jobId);
        sql.exec(`DELETE FROM zip_entries WHERE jobId = ?;`, jobId);
        sql.exec(`DELETE FROM checkpoint WHERE jobId = ?;`, jobId);
        sql.exec(`DELETE FROM job_state WHERE jobId = ?;`, jobId);

        console.log(`[zip-v2] Cleanup purged job state.`, {
          jobId,
          status,
          event: "cleanup.due" satisfies ZipV2LifecycleEvent,
        });
      } catch (e) {
        console.error(`[zip-v2] Cleanup failed.`, {
          jobId,
          status,
          error: String((e as any)?.message ?? e),
          event: "cleanup.failure" satisfies ZipV2LifecycleEvent,
        });
      }
    }

    // --------------------------------------------------------------------------
    // Tick scheduling: enqueue due ZIP_V2_TICK messages (nextActionAtMs is the wake time)
    // --------------------------------------------------------------------------
    const dueTicks = sql.exec(
      `SELECT jobId
       FROM job_state
       WHERE nextActionAtMs IS NOT NULL
         AND nextActionAtMs <= ?
         AND status IN ('PENDING','RUNNING','FINALIZING');`,
      now,
    );
    const dueTickRows = dueTicks.toArray?.() ?? [];
    for (const row of dueTickRows) {
      const r: any = row;
      const jobId = String(r.jobId);
      try {
        await this.env.QUEUE_WORKER_MAIN.send({
          type: "zip_v2_tick",
          data: { jobId },
        } as any);
        sql.exec(`UPDATE job_state SET nextActionAtMs = NULL WHERE jobId = ?;`, jobId);
        console.log(`[zip-v2] Alarm enqueued tick.`, {
          jobId,
          event: "tick.dispatched" satisfies ZipV2LifecycleEvent,
        });
      } catch (e) {
        // Keep nextActionAtMs so we try again on next alarm run.
        console.error(`[zip-v2] Alarm failed to enqueue tick.`, {
          jobId,
          error: String((e as any)?.message ?? e),
          event: "tick.dispatch.failed" satisfies ZipV2LifecycleEvent,
        });
      }
    }

    await this.rescheduleStorageAlarmFromDb();
  }

  /**
   * HTTP entrypoints for `/start`, `/tick`, and container-forwarded `/entries`.
   *
   * @param req - The HTTP request
   * @returns The HTTP response
   */
  async fetch(req: Request): Promise<Response> {
    this.initIfNeeded();
    const url = new URL(req.url);

    // Start the zip v2 job
    if (req.method === "POST" && url.pathname === "/start") {
      const body = await req.json();
      await this.handleStart(body as StartJobRequest);

      return jsonResponse({ ok: true });
    }

    // One unit of work per request; DO persists `nextActionAtMs`
    // for the next wake (alarm → queue tick).
    if (req.method === "POST" && url.pathname === "/tick") {
      const body = (await req.json()) satisfies TickJobRequest;
      const result = await this.handleTick(body.jobId);

      return jsonResponse(result);
    }

    // Handle status requests to get the job status
    // if (req.method === "GET" && url.pathname === "/status") {
    //   const jobId = url.searchParams.get("jobId");
    //   if (!jobId) {
    //     return jsonResponse({ error: "missing jobId" }, 400);
    //   }

    //   const status = await this.getJobStatus(jobId);

    //   return jsonResponse(status);
    // }

    // Handle list entries requests
    if (url.pathname === "/entries" && req.method === "GET") {
      const jobId = url.searchParams.get("jobId");
      if (!jobId) {
        return jsonResponse({ error: "missing jobId" }, 400);
      }

      const entries = this.listZipEntries(jobId);

      return jsonResponse(entries);
    }

    // Handle upsert entry requests
    if (url.pathname === "/entries" && req.method === "POST") {
      const jobId = url.searchParams.get("jobId");
      const fileIndex = url.searchParams.get("fileIndex");
      if (!jobId || fileIndex === null) {
        return jsonResponse({ error: "missing jobId/fileIndex" }, 400);
      }

      const entry = (await req.json()) as Omit<ZipEntryRow, "jobId" | "fileIndex">;
      this.upsertZipEntry(jobId, Number(fileIndex), entry);

      return jsonResponse({ ok: true });
    }

    return new Response("not found", { status: 404 });
  }

  // --------------------------------------------------------------------------------
  // Handlers
  // --------------------------------------------------------------------------------

  /**
   * PUT compression status to the Web API and persist `isTransferStatusUpdated` on `job_state`.
   * Idempotent retries are handled inside `WebAPIService.updateTransferStatus`.
   */
  private async persistTransferStatusNotify(job: JobStateRow, request: TransferUpdateRequest) {
    const jobId = job.jobId;
    const webAPIService = new WebAPIService(this.env.SECRET_KEY, this.env.WEB_API_BASE_URL);

    console.log(`[zip-v2] Persisting transfer status notify.`, {
      jobId,
      transferId: job.transferId,
      request: JSON.stringify(request),
    });

    try {
      await webAPIService.updateTransferStatus(job.transferId, request);
      const fresh = this.getJobRow(jobId);

      if (!fresh) {
        throw new Error(`Job not found after update transfer status: ${jobId}`);
      }

      this.upsertJob({
        ...fresh,
        isTransferStatusUpdated: true,
        updatedAtMs: nowMs(),
      });
    } catch (e) {
      const fresh = this.getJobRow(jobId);
      if (fresh) {
        this.upsertJob({
          ...fresh,
          isTransferStatusUpdated: false,
          updatedAtMs: nowMs(),
        });
      }

      console.warn(`[zip-v2] Failed to update transfer status`, {
        error: e,
        jobId,
        transferId: job.transferId,
        request: JSON.stringify(request),
      });
    }
  }

  /**
   * Idempotent: creates `PENDING` row + checkpoint and schedules first tick via `nextActionAtMs`.
   *
   * @param body - The start job request body
   */
  private async handleStart(body: StartJobRequest) {
    const { jobId, transferId, manifestKey, outputKey } = body;

    const existing = this.getActiveJobRow(jobId);
    if (existing) {
      console.log(`[zip-v2] Active job already exists.`, {
        jobId,
        transferId,
        manifestKey,
        outputKey,
        existing: existing?.status,
      });

      if (existing.status === "DONE" && existing.isTransferStatusUpdated !== true) {
        const cp = this.getCheckpoint(jobId);
        await this.persistTransferStatusNotify(existing, {
          status: TransferStatus.READY,
          bundleObjectKey: cp?.outputKey ?? outputKey,
        });
      }

      return;
    }

    const partSize = toInt(this.env.ZIP_PART_SIZE_BYTES, DEFAULT_PART_SIZE);

    this.upsertJob({
      jobId,
      transferId,
      status: "PENDING",
      createdAtMs: nowMs(),
      updatedAtMs: nowMs(),
      consecutiveFailures: 0,
      cleanupAtMs: undefined,
      // Wake immediately so `alarm()` can enqueue the first `zip_v2_tick` (queue is trigger-only).
      nextActionAtMs: nowMs(),
    } satisfies JobStateRow);

    this.upsertCheckpoint(jobId, {
      manifestKey,
      outputKey,
      uploadId: undefined,
      partSize,
      nextPartNumber: 1,
      fileIndex: 0,
      zipOffset: "0",
      bytesWrittenTotal: "0",
      filesDone: 0,
      done: false,
    });

    console.log(`[zip-v2] Job started.`, {
      jobId,
      transferId,
      manifestKey,
      outputKey,
      event: "job.start.requested" satisfies ZipV2LifecycleEvent,
    });

    await this.rescheduleStorageAlarmFromDb();
  }

  /**
   * If `nextActionAtMs` is still in the future, skip this tick and return a response (backoff in effect).
   * Otherwise returns `null` so the caller continues.
   */
  private maybeReturnBackoffTickResponse(job: JobStateRow): TickJobResponse | null {
    if (!job.nextActionAtMs || nowMs() >= job.nextActionAtMs) {
      return null;
    }
    const retryAfterSeconds = Math.max(1, Math.ceil((job.nextActionAtMs - nowMs()) / 1000));

    console.log(`[zip-v2] [${job.status}] Tick skipped (backoff).`, {
      jobId: job.jobId,
      transferId: job.transferId,
      retryAfterSeconds,
      consecutiveFailures: job.consecutiveFailures,
      lastErrorKind: job.lastErrorKind,
      nextActionAtMs: job.nextActionAtMs,
      event: "tick.deferred" satisfies ZipV2LifecycleEvent,
    });

    return { status: job.status, done: false, retryAfterSeconds };
  }

  /**
   * Runs one unit of ZIP v2 work: finalize path, or one container chunk under the global semaphore.
   * Retries and next wake times are persisted on `job_state.nextActionAtMs` — not in the queue consumer.
   */
  private async handleTick(jobId: string): Promise<TickJobResponse> {
    const tickStartMs = nowMs();
    let job = this.getJobRow(jobId);
    const checkpoint = this.getCheckpoint(jobId);

    if (!job || !checkpoint) {
      throw new Error(`No Job or Checkpoint found for job: ${jobId}`);
    }

    const backoffResponse = this.maybeReturnBackoffTickResponse(job);
    if (backoffResponse) {
      return backoffResponse;
    }

    // If we were in FINALIZING, skip runChunk and attempt multipart completion.
    if (job.status === "FINALIZING") {
      try {
        const finalized = await this.finalizeMultipartIfNeeded(job, checkpoint);
        if (!finalized) {
          // `finalizeMultipartIfNeeded` already ran `applyRetryableBackoff` + `rescheduleStorageAlarmFromDb`.
          console.log(`[zip-v2] Finalization deferred (alarm/backoff already scheduled).`, {
            jobId,
            transferId: job.transferId,
            event: "finalize.deferred" satisfies ZipV2LifecycleEvent,
          });

          return { status: "FINALIZING", done: false };
        }

        const { outputBytes, verifyDurationMs } = await this.verifyFinalOutput(checkpoint);
        await this.scheduleCleanup({ ...job, status: "DONE" });

        // Notify the Web API that the job is done
        const doneAfterFinalize = this.getJobRow(jobId);
        if (doneAfterFinalize) {
          await this.persistTransferStatusNotify(doneAfterFinalize, {
            status: TransferStatus.READY,
            bundleObjectKey: checkpoint.outputKey,
          });
        }

        console.log(`[zip-v2] Tick done (job complete).`, {
          jobId,
          transferId: job.transferId,
          outputKey: checkpoint.outputKey,
          outputBytes,
          verifyDurationMs,
          tickDurationMs: nowMs() - tickStartMs,
          event: "output.verified" satisfies ZipV2LifecycleEvent,
        });

        return { status: "DONE", done: true };
      } catch (e) {
        await this.failFinalizeJob(job, checkpoint, e);
        return { status: "FAILED", done: false };
      }
    }

    console.log(`[zip-v2] Tick start.`, {
      jobId,
      jobStatus: job.status,
      transferId: job.transferId,
      tickStartMs,
      checkpoint: checkpointSummary(checkpoint),
      event: "tick.processing" satisfies ZipV2LifecycleEvent,
    });

    if (job.status === "DONE" || job.status === "FAILED" || job.status === "CANCELLED") {
      console.log(`[zip-v2] Job already completed.`, {
        jobId,
        jobStatus: job.status,
      });

      // Notify the Web API that the job is done
      if (job.status === "DONE" && job.isTransferStatusUpdated !== true) {
        const cp = this.getCheckpoint(jobId);
        if (cp?.outputKey) {
          await this.persistTransferStatusNotify(job, {
            status: TransferStatus.READY,
            bundleObjectKey: cp.outputKey,
          });
        }
      }

      // Notify the Web API that the job failed
      if (job.status === "FAILED" && job.isTransferStatusUpdated !== true) {
        await this.persistTransferStatusNotify(job, {
          status: TransferStatus.READY_BUT_COMPRESSION_FAILED,
        });
      }

      return {
        status: job.status,
        done: job.status === "DONE",
      };
    }

    const existingOut = await this.env.OUTPUT_BUCKET.head(checkpoint.outputKey);
    if (existingOut) {
      this.upsertJob({
        ...job,
        status: "DONE",
        updatedAtMs: nowMs(),
        nextActionAtMs: undefined,
      });
      await this.rescheduleStorageAlarmFromDb();

      // Notify the Web API that the job is done
      const doneJob = this.getJobRow(jobId) ?? { ...job, status: "DONE" as const };
      await this.persistTransferStatusNotify(doneJob, {
        status: TransferStatus.READY,
        bundleObjectKey: checkpoint.outputKey,
      });

      return { status: "DONE", done: true };
    }

    // Global concurrency only — release must run only if we actually acquired (see `finally`).
    const semaphoreAcquired = await this.acquireSemaphore(job, checkpoint);
    if (!semaphoreAcquired) {
      return {
        status: job.status,
        done: false,
      };
    }

    try {
      this.upsertJob({
        ...job,
        status: "RUNNING",
        updatedAtMs: nowMs(),
      });

      // Create a new multipart upload if it doesn't exist and update the checkpoint
      const maxParts = toInt(this.env.ZIP_MAX_PARTS_PER_TICK, DEFAULT_NUMBER_OF_PARTS);
      if (!checkpoint.uploadId) {
        const newCheckpoint = await this.createMultipartUpload(job, checkpoint);
        checkpoint.uploadId = newCheckpoint.uploadId;
      }

      // Run the container to upload the next parts
      const containerId = this.env.ZipContainer.idFromName(jobId);
      const containerStub = this.env.ZipContainer.get(containerId);

      // Forward the request to the container
      const runChunkStartMs = nowMs();
      const runResp = await containerStub.fetch("https://zip/runChunk", {
        method: "POST",
        body: JSON.stringify({
          jobId,
          manifestKey: checkpoint.manifestKey,
          outputKey: checkpoint.outputKey,
          uploadId: checkpoint.uploadId,
          partSize: checkpoint.partSize,
          nextPartNumber: checkpoint.nextPartNumber,
          fileIndex: checkpoint.fileIndex,
          zipOffset: checkpoint.zipOffset,
          maxParts,
        }),
      });
      const runChunkDurationMs = nowMs() - runChunkStartMs;

      if (!runResp.ok) {
        const errText = await runResp.text();
        console.error(`[zip-v2] Container run failed.`, {
          status: runResp.status,
          error: errText,
          durationMs: runChunkDurationMs,
          jobId,
          transferId: job.transferId,
          bundleObjectKey: checkpoint.outputKey,
          manifestKey: checkpoint.manifestKey,
          checkpoint: checkpointSummary(checkpoint),
        });
        throw new ContainerRunError(runResp.status, errText);
      }

      const run = (await runResp.json()) satisfies RunChunkResponse;

      console.log(`[zip-v2] runChunk ok.`, {
        jobId,
        transferId: job.transferId,
        durationMs: runChunkDurationMs,
        maxParts,
        uploadedPartsCount: run.uploadedParts?.length ?? 0,
        nextPartNumber: run.nextPartNumber,
        fileIndex: run.fileIndex,
        zipOffset: run.zipOffset,
        bytesWrittenTotal: run.bytesWrittenTotal,
        filesDone: run.filesDone,
        done: run.done,
        event: (run.done
          ? "chunk.uploads_completed"
          : "chunk.succeeded") satisfies ZipV2LifecycleEvent,
      });

      if (job.consecutiveFailures > 0 || job.nextActionAtMs || job.lastErrorKind) {
        this.upsertJob({
          ...job,
          consecutiveFailures: 0,
          nextActionAtMs: undefined,
          lastFailureAtMs: undefined,
          lastErrorKind: undefined,
          updatedAtMs: nowMs(),
        });
        job = this.getJobRow(jobId) ?? job;
      }

      if (run.uploadedParts?.length) {
        this.insertUploadedParts(jobId, run.uploadedParts);
      }

      this.upsertCheckpoint(jobId, {
        ...checkpoint,
        nextPartNumber: run.nextPartNumber,
        fileIndex: run.fileIndex,
        zipOffset: run.zipOffset,
        bytesWrittenTotal: run.bytesWrittenTotal,
        filesDone: run.filesDone,
        done: run.done,
      } satisfies Checkpoint);

      const refreshedAfterCp = this.getJobRow(jobId);
      if (!refreshedAfterCp) {
        throw new Error(`Job row missing after checkpoint update jobId=${jobId}`);
      }
      job = refreshedAfterCp;

      if (run.done) {
        // Finalization stage - complete multipart using the full persisted parts list.
        this.upsertJob({ ...job, status: "FINALIZING", updatedAtMs: nowMs() });
        try {
          const finalized = await this.finalizeMultipartIfNeeded(
            {
              ...job,
              status: "FINALIZING",
            },
            checkpoint,
          );

          if (!finalized) {
            console.log(`[zip-v2] Finalization deferred (alarm/backoff already scheduled).`, {
              jobId,
              transferId: job.transferId,
              event: "finalize.deferred" satisfies ZipV2LifecycleEvent,
            });
            return { status: "FINALIZING", done: false };
          }
        } catch (error) {
          await this.failFinalizeJob(
            {
              ...job,
              status: "FINALIZING",
            },
            checkpoint,
            error,
          );
          return { status: "FAILED", done: false };
        }

        // Verify output exists and update status.
        const { outputBytes, verifyDurationMs } = await this.verifyFinalOutput(checkpoint);

        // Schedule cleanup after successful verification
        await this.scheduleCleanup({ ...job, status: "DONE" });

        const doneAfterCleanup = this.getJobRow(jobId) ?? job;
        await this.persistTransferStatusNotify(doneAfterCleanup, {
          status: TransferStatus.READY,
          bundleObjectKey: checkpoint.outputKey,
        });

        console.log(`[zip-v2] Tick done (job complete).`, {
          jobId,
          transferId: job.transferId,
          outputKey: checkpoint.outputKey,
          outputBytes,
          verifyDurationMs,
          tickDurationMs: nowMs() - tickStartMs,
          event: "output.verified" satisfies ZipV2LifecycleEvent,
        });

        return { status: "DONE", done: true };
      }

      // Not done. Caller should enqueue another tick.
      console.log(`[zip-v2] Tick done (job still running).`, {
        jobId,
        transferId: job.transferId,
        tickDurationMs: nowMs() - tickStartMs,
        event: "chunk.succeeded" satisfies ZipV2LifecycleEvent,
      });

      await this.scheduleNextProgressAction(jobId);

      return {
        status: "RUNNING",
        done: false,
      };
    } catch (err: any) {
      return await this.handleTickFailure(err as Error, jobId, job, checkpoint, tickStartMs);
    } finally {
      if (semaphoreAcquired) {
        const releaseId = this.env.ZipSemaphore.idFromName("global");
        await this.env.ZipSemaphore.get(releaseId).fetch("https://semaphore/release", {
          method: "POST",
          body: JSON.stringify({ jobId }),
        });
      }
    }
  }

  private getBundleCleanupTtlMs(jobId: string) {
    const bundleCleanupTtlDays = toInt(
      this.env.BUNDLE_CLEANUP_TTL_DAYS,
      DEFAULT_BUNDLE_CLEANUP_TTL_DAYS,
    );
    const bundleCleanupTtlMs = bundleCleanupTtlDays * 24 * 60 * 60 * 1000;
    const cleanupAtMs = nowMs() + bundleCleanupTtlMs;

    console.log(`[zip-v2] Bundle cleanup TTL: ${bundleCleanupTtlDays} days`, {
      jobId,
      bundleCleanupTtlDays,
      bundleCleanupTtlMs,
      cleanupAtMs,
    });

    return cleanupAtMs;
  }
  /**
   * Marks job `DONE` and schedules row purge after `CLEANUP_TTL_DAYS`.
   * Clears any pending chunk wake.
   */
  private async scheduleCleanup(job: JobStateRow) {
    const cleanupAtMs = this.getBundleCleanupTtlMs(job.jobId);

    this.upsertJob({
      ...job,
      status: "DONE",
      updatedAtMs: nowMs(),
      cleanupAtMs,
      nextActionAtMs: undefined,
    });

    await this.rescheduleStorageAlarmFromDb();

    console.log(`[zip-v2] Cleanup scheduled (done).`, {
      jobId: job.jobId,
      transferId: job.transferId,
      cleanupAtMs,
      event: "output.verified" satisfies ZipV2LifecycleEvent,
    });
  }

  /**
   * Verifies the final output exists and returns the output size
   * and verification duration.
   */
  private async verifyFinalOutput(checkpoint: Checkpoint) {
    const verifyStartMs = nowMs();
    const out = await this.env.OUTPUT_BUCKET.head(checkpoint.outputKey);
    const verifyDurationMs = nowMs() - verifyStartMs;
    if (!out) {
      throw new Error("Container reported done but output does not exist");
    }

    return {
      outputBytes: out.size,
      verifyDurationMs,
    };
  }

  /**
   * Terminal failure during FINALIZING: non-retryable error, missing parts, or retry
   * budget exhausted. Clears `nextActionAtMs`, schedules cleanup TTL, notifies web API (best-effort).
   */
  private async failFinalizeJob(job: JobStateRow, checkpoint: Checkpoint, err: unknown) {
    const rawErrText = String((err as any)?.message ?? err);
    const fresh = this.getJobRow(job.jobId) ?? job;
    const jobId = fresh.jobId;

    console.error(`[zip-v2] Finalization permanently failed jobId=${jobId}`, {
      error: err,
      errorMessage: rawErrText,
      jobId,
      transferId: fresh.transferId,
      bundleObjectKey: checkpoint.outputKey,
      manifestKey: checkpoint.manifestKey,
      checkpoint: checkpointSummary(checkpoint),
      event: "finalize.failure.terminal" satisfies ZipV2LifecycleEvent,
    });

    const cleanupAtMs = this.getBundleCleanupTtlMs(jobId);
    this.upsertJob({
      ...fresh,
      status: "FAILED",
      updatedAtMs: nowMs(),
      errorMessage: rawErrText,
      lastFailureAtMs: nowMs(),
      lastErrorKind: "unknown",
      cleanupAtMs,
      nextActionAtMs: undefined,
    });

    await this.rescheduleStorageAlarmFromDb();
    console.log(`[zip-v2] Cleanup scheduled (finalize failed).`, {
      jobId,
      transferId: fresh.transferId,
      cleanupAtMs,
      event: "finalize.failure.terminal" satisfies ZipV2LifecycleEvent,
    });

    const failedRow = this.getJobRow(jobId) ?? fresh;
    await this.persistTransferStatusNotify(failedRow, {
      status: TransferStatus.READY_BUT_COMPRESSION_FAILED,
    });
  }

  /**
   * Completes the R2 multipart upload when all parts are persisted. Returns `false` when
   * completion failed with a retryable error — `applyRetryableBackoff` was already applied.
   * Throws on non-retryable errors or when retry budget is exhausted (caller marks `FAILED`).
   *
   *  Idempotency: if output already exists, consider it finalized.
   */
  private async finalizeMultipartIfNeeded(job: JobStateRow, checkpoint: Checkpoint) {
    const jobId = job.jobId;
    const existing = await this.env.OUTPUT_BUCKET.head(checkpoint.outputKey);

    if (existing) {
      console.log(`[zip-v2] Finalization skipped (output exists).`, {
        jobId,
        transferId: job.transferId,
        outputKey: checkpoint.outputKey,
        outputBytes: existing.size,
      });
      return true;
    }

    if (!checkpoint.uploadId) {
      throw new Error("Missing uploadId during finalization");
    }

    const parts = this.listUploadedPartsForComplete(jobId);
    if (!parts.length) {
      throw new Error("No uploaded parts found for finalization");
    }

    // Guard: parts must be contiguous from 1..N (R2/S3 expects all parts).
    // If this fails, completing the multipart will produce a corrupt object.
    let expected = 1;
    for (const p of parts) {
      if (p.partNumber !== expected) {
        const msg = `Missing multipart parts: expected partNumber=${expected} got=${p.partNumber}`;
        console.error(`[zip-v2] Finalization blocked (missing parts).`, {
          jobId,
          transferId: job.transferId,
          outputKey: checkpoint.outputKey,
          expectedPartNumber: expected,
          gotPartNumber: p.partNumber,
          partsCount: parts.length,
          firstPartNumber: parts[0]?.partNumber,
          lastPartNumber: parts.at(-1)?.partNumber,
        });
        throw new MissingMultipartPartsError(msg);
      }
      expected++;
    }

    console.log(`[zip-v2] Finalizing multipart.`, {
      jobId,
      transferId: job.transferId,
      outputKey: checkpoint.outputKey,
      uploadId: `${checkpoint.uploadId.slice(0, 8)}...`,
      partsCount: parts.length,
      firstPartNumber: parts[0]?.partNumber,
      lastPartNumber: parts[parts.length - 1]?.partNumber,
    });

    try {
      const up = this.env.OUTPUT_BUCKET.resumeMultipartUpload(
        checkpoint.outputKey,
        checkpoint.uploadId,
      );
      await up.complete(parts);
      console.log(`[zip-v2] Multipart complete succeeded.`, {
        jobId,
        transferId: job.transferId,
        outputKey: checkpoint.outputKey,
        partsCount: parts.length,
        event: "finalize.multipart_completed" satisfies ZipV2LifecycleEvent,
      });
      return true;
    } catch (e) {
      const errText = String((e as any)?.message ?? e);
      const { retryable, kind } = classifyFinalizeFailure(errText);
      console.warn(`[zip-v2] Multipart complete failed.`, {
        jobId,
        transferId: job.transferId,
        outputKey: checkpoint.outputKey,
        retryable,
        errorKind: kind,
        error: errText,
      });

      if (!retryable) {
        throw e;
      }

      const scheduled = await this.applyRetryableBackoff({
        jobId,
        holdStatus: "FINALIZING",
        errText,
        kind,
        event: "finalize.failure.retryable" satisfies ZipV2LifecycleEvent,
      });
      if (!scheduled.ok) {
        throw e;
      }
      return false;
    }
  }

  /**
   * Classifies container/chunk errors; retryable paths go through `applyRetryableBackoff`,
   * terminal paths mark `FAILED` and schedule cleanup (single failure policy).
   */
  private async handleTickFailure(
    err: Error,
    jobId: string,
    job: JobStateRow,
    checkpoint: Checkpoint,
    tickStartMs: number,
  ): Promise<TickJobResponse> {
    const errMsg = String(err?.message ?? err);
    const status = err instanceof ContainerRunError ? err.status : 500;
    const rawErrText = err instanceof ContainerRunError ? err.errText : errMsg;
    const { retryable, kind } = classifyContainerFailure(status, rawErrText);

    const dbJob = this.getJobRow(jobId) ?? job;

    if (retryable) {
      const scheduled = await this.applyRetryableBackoff({
        jobId,
        holdStatus: "RUNNING",
        errText: rawErrText,
        kind,
        event: "chunk.failure.retryable" satisfies ZipV2LifecycleEvent,
      });

      if (scheduled.ok) {
        return {
          status: "RUNNING",
          done: false,
          retryAfterSeconds: scheduled.retryAfterSeconds,
        };
      }
    }

    const consecutiveFailures = (dbJob.consecutiveFailures ?? 0) + 1;
    const maxConsecutiveFailures = toInt(
      (this.env as any).ZIP_V2_MAX_CONSECUTIVE_FAILURES,
      DEFAULT_MAX_CONSECUTIVE_FAILURES,
    );

    console.error(`[zip-v2] job failed jobId=${jobId}`, {
      error: err,
      errorMessage: errMsg,
      errorKind: kind,
      retryable,
      consecutiveFailures,
      maxConsecutiveFailures,
      jobId,
      transferId: dbJob.transferId,
      bundleObjectKey: checkpoint.outputKey,
      manifestKey: checkpoint.manifestKey,
      tickDurationMs: nowMs() - tickStartMs,
      checkpoint: checkpointSummary(checkpoint),
      event: "chunk.failure.terminal" satisfies ZipV2LifecycleEvent,
    });

    const cleanupAtMs = this.getBundleCleanupTtlMs(jobId);
    this.upsertJob({
      ...dbJob,
      status: "FAILED",
      updatedAtMs: nowMs(),
      errorMessage: rawErrText,
      consecutiveFailures,
      lastFailureAtMs: nowMs(),
      lastErrorKind: kind,
      cleanupAtMs,
      nextActionAtMs: undefined,
    });

    await this.rescheduleStorageAlarmFromDb();
    console.log(`[zip-v2] Cleanup scheduled (failed).`, {
      jobId,
      transferId: dbJob.transferId,
      cleanupAtMs,
      event: "chunk.failure.terminal" satisfies ZipV2LifecycleEvent,
    });

    const failedRow = this.getJobRow(jobId) ?? dbJob;
    await this.persistTransferStatusNotify(failedRow, {
      status: TransferStatus.READY_BUT_COMPRESSION_FAILED,
    });

    return { status: "FAILED", done: false };
  }

  private async acquireSemaphore(job: JobStateRow, checkpoint: Checkpoint) {
    const jobId = job.jobId;
    const semaphoreId = this.env.ZipSemaphore.idFromName("global");
    const semaphore = this.env.ZipSemaphore.get(semaphoreId);
    const desired = toInt(this.env.ZIP_GLOBAL_CONCURRENCY, 1);

    const acquireStartMs = nowMs();
    const acquiredResp = await semaphore.fetch("https://semaphore/acquire", {
      method: "POST",
      body: JSON.stringify({ jobId, limit: desired }),
    });

    const raw = await acquiredResp.text();
    let parsed: { acquired?: boolean } | null = null;

    try {
      parsed = JSON.parse(raw) as { acquired?: boolean };
    } catch {
      // leave parsed null
    }

    const acquired = acquiredResp.ok && parsed?.acquired === true;
    if (!acquired) {
      const acquireDurationMs = nowMs() - acquireStartMs;
      console.error(`[zip-v2] Failed to acquire semaphore.`, {
        error: raw.slice(0, 500),
        status: acquiredResp.status,
        durationMs: acquireDurationMs,
        jobId,
        transferId: job.transferId,
        bundleObjectKey: checkpoint.outputKey,
        manifestKey: checkpoint.manifestKey,
        event: "lock.unavailable" satisfies ZipV2LifecycleEvent,
      });

      await this.scheduleNextProgressAction(jobId);

      return false;
    }

    return true;
  }

  private async createMultipartUpload(job: JobStateRow, checkpoint: Checkpoint) {
    const jobId = job.jobId;
    const createUploadStartMs = nowMs();
    const upload = await this.env.OUTPUT_BUCKET.createMultipartUpload(checkpoint.outputKey, {
      customMetadata: {
        jobId,
        transferId: job.transferId,
        manifestKey: checkpoint.manifestKey,
        zipVersion: ZIP_VERSION,
      },
    });

    const createUploadDurationMs = nowMs() - createUploadStartMs;
    checkpoint.uploadId = upload.uploadId;

    this.upsertCheckpoint(jobId, checkpoint);

    console.log(`[zip-v2] Multipart upload created.`, {
      jobId,
      transferId: job.transferId,
      outputKey: checkpoint.outputKey,
      uploadId: `${upload.uploadId.slice(0, 8)}...`,
      durationMs: createUploadDurationMs,
    });

    return checkpoint;
  }

  // Used by the web API to get the job status
  // private async getJobStatus(jobId: string) {
  //   const job = this.getJobRow(jobId);
  //   const checkpoint = this.getCheckpoint(jobId);

  //   if (!job || !checkpoint) {
  //     return { jobId, exists: false };
  //   }

  //   return {
  //     jobId,
  //     transferId: job.transferId,
  //     status: job.status,
  //     errorMessage: job.errorMessage ?? null,
  //     manifestKey: checkpoint.manifestKey,
  //     outputKey: checkpoint.outputKey,
  //     uploadId: checkpoint.uploadId ?? null,
  //     partSize: checkpoint.partSize,
  //     nextPartNumber: checkpoint.nextPartNumber,
  //     fileIndex: checkpoint.fileIndex,
  //     zipOffset: checkpoint.zipOffset,
  //     bytesWrittenTotal: checkpoint.bytesWrittenTotal,
  //     filesDone: checkpoint.filesDone,
  //     done: checkpoint.done,
  //     updatedAtMs: job.updatedAtMs,
  //   };
  // }

  // --------------------------------------------------------------------------------
  // SQL helpers
  // --------------------------------------------------------------------------------
  private sql() {
    return this.state.storage.sql as any;
  }

  /**
   * Ensures SQLite schema exists; migrates legacy `nextTickAtMs` / `nextRetryAtMs`
   * into `nextActionAtMs`.
   * */
  private initIfNeeded() {
    const sql = this.sql();
    sql.exec(
      `CREATE TABLE IF NOT EXISTS job_state (
        jobId TEXT PRIMARY KEY,
        transferId TEXT NOT NULL,
        status TEXT NOT NULL,
        createdAtMs INTEGER NOT NULL,
        updatedAtMs INTEGER NOT NULL,
        errorMessage TEXT,
        consecutiveFailures INTEGER NOT NULL DEFAULT 0,
        lastFailureAtMs INTEGER,
        nextRetryAtMs INTEGER,
        lastErrorKind TEXT,
        cleanupAtMs INTEGER,
        nextTickAtMs INTEGER,
        nextActionAtMs INTEGER
      );`,
    );

    // Backward-compatible migrations for existing environments.
    // (CREATE TABLE IF NOT EXISTS does not add columns.)
    for (const stmt of [
      `ALTER TABLE job_state ADD COLUMN consecutiveFailures INTEGER NOT NULL DEFAULT 0;`,
      `ALTER TABLE job_state ADD COLUMN lastFailureAtMs INTEGER;`,
      `ALTER TABLE job_state ADD COLUMN nextRetryAtMs INTEGER;`,
      `ALTER TABLE job_state ADD COLUMN lastErrorKind TEXT;`,
      `ALTER TABLE job_state ADD COLUMN cleanupAtMs INTEGER;`,
      `ALTER TABLE job_state ADD COLUMN nextTickAtMs INTEGER;`,
      `ALTER TABLE job_state ADD COLUMN nextActionAtMs INTEGER;`,
      `ALTER TABLE job_state ADD COLUMN isTransferStatusUpdated INTEGER;`,
    ]) {
      try {
        sql.exec(stmt);
      } catch {
        // ignore: column already exists
      }
    }
    // One-time coalesce from legacy wake columns into nextActionAtMs (idempotent).
    sql.exec(
      `UPDATE job_state SET nextActionAtMs = COALESCE(nextTickAtMs, nextRetryAtMs)
       WHERE nextActionAtMs IS NULL
         AND (nextTickAtMs IS NOT NULL OR nextRetryAtMs IS NOT NULL);`,
    );
    sql.exec(
      `CREATE TABLE IF NOT EXISTS checkpoint (
        jobId TEXT PRIMARY KEY,
        manifestKey TEXT NOT NULL,
        outputKey TEXT NOT NULL,
        uploadId TEXT,
        partSize INTEGER NOT NULL,
        nextPartNumber INTEGER NOT NULL,
        fileIndex INTEGER NOT NULL,
        zipOffset TEXT NOT NULL,
        bytesWrittenTotal TEXT NOT NULL,
        filesDone INTEGER NOT NULL,
        done INTEGER NOT NULL
      );`,
    );
    sql.exec(
      `CREATE TABLE IF NOT EXISTS uploaded_parts (
        jobId TEXT NOT NULL,
        partNumber INTEGER NOT NULL,
        etag TEXT NOT NULL,
        sizeBytes INTEGER NOT NULL,
        PRIMARY KEY (jobId, partNumber)
      );`,
    );

    sql.exec(
      `CREATE TABLE IF NOT EXISTS zip_entries (
        jobId TEXT NOT NULL,
        fileIndex INTEGER NOT NULL,
        nameB64 TEXT NOT NULL,
        crc32 INTEGER NOT NULL,
        compressedSize TEXT NOT NULL,
        uncompressedSize TEXT NOT NULL,
        localHeaderOffset TEXT NOT NULL,
        modTime INTEGER NOT NULL,
        modDate INTEGER NOT NULL,
        PRIMARY KEY (jobId, fileIndex)
      );`,
    );
  }

  /**
   * Sets `storage` alarm to the earliest of `cleanupAtMs` or `nextActionAtMs` in `job_state`,
   * or clears the alarm when nothing is pending.
   */
  private async rescheduleStorageAlarmFromDb() {
    const sql = this.sql();
    const mins = sql.exec(
      `SELECT MIN(cleanupAtMs) AS nextCleanupAtMs, MIN(nextActionAtMs) AS nextActionAtMs FROM job_state;`,
    );
    const minRow = (mins?.toArray?.() ?? [])[0];
    const nextCleanupAtMs = minRow?.nextCleanupAtMs as number | null | undefined;
    const nextActionAtMs = minRow?.nextActionAtMs as number | null | undefined;
    const candidates = [nextCleanupAtMs, nextActionAtMs].filter(
      (v) => typeof v === "number" && Number.isFinite(v),
    ) as number[];

    if (candidates.length) {
      const nextAlarmAtMs = Math.min(...candidates);
      await this.state.storage.setAlarm(nextAlarmAtMs);
      console.log(`[zip-v2] Storage alarm rescheduled.`, {
        nextAlarmAtMs,
        nextCleanupAtMs,
        nextActionAtMs,
      });
    } else {
      await clearAlarmIfSupported(this.state.storage);
      console.log(`[zip-v2] Storage alarm cleared (no pending cleanups or actions).`);
    }
  }

  /**
   * Single retry/backoff path for transient errors while `RUNNING` or `FINALIZING`.
   * @returns `ok: false` when max consecutive failures exceeded — caller must terminal-fail the job.
   */
  private async applyRetryableBackoff(params: {
    jobId: string;
    holdStatus: "RUNNING" | "FINALIZING";
    errText: string;
    kind: ErrorKind;
    event: ZipV2LifecycleEvent;
  }): Promise<{ ok: true; retryAfterSeconds: number } | { ok: false }> {
    const { jobId, holdStatus, errText, kind, event } = params;
    const dbJob = this.getJobRow(jobId);
    if (!dbJob) {
      return { ok: false };
    }
    const consecutiveFailures = (dbJob.consecutiveFailures ?? 0) + 1;
    const maxConsecutiveFailures = toInt(
      (this.env as any).ZIP_V2_MAX_CONSECUTIVE_FAILURES,
      DEFAULT_MAX_CONSECUTIVE_FAILURES,
    );
    const baseDelaySeconds = toInt(
      (this.env as any).ZIP_V2_RETRY_BASE_DELAY_SECONDS,
      DEFAULT_RETRY_BASE_DELAY_SECONDS,
    );
    if (consecutiveFailures >= maxConsecutiveFailures) {
      return { ok: false };
    }
    const backoffSeconds = computeBackoffSeconds(consecutiveFailures, baseDelaySeconds);
    const backoffUntilMs = nowMs() + backoffSeconds * 1000 + jitterMs(1000);
    const intervalMs = toInt((this.env as any).ZIP_V2_TICK_INTERVAL_MS, DEFAULT_TICK_INTERVAL_MS);
    const nextActionAtMs = Math.max(nowMs() + intervalMs, backoffUntilMs);
    this.upsertJob({
      ...dbJob,
      status: holdStatus,
      updatedAtMs: nowMs(),
      errorMessage: errText,
      consecutiveFailures,
      lastFailureAtMs: nowMs(),
      lastErrorKind: kind,
      nextActionAtMs,
    });

    await this.rescheduleStorageAlarmFromDb();
    console.warn(`[zip-v2] Retry backoff scheduled.`, {
      jobId,
      transferId: dbJob.transferId,
      event,
      status: holdStatus,
      consecutiveFailures,
      maxConsecutiveFailures,
      retryAfterSeconds: backoffSeconds,
      errorKind: kind,
      nextActionAtMs,
    });
    return { ok: true, retryAfterSeconds: backoffSeconds };
  }

  /**
   * Schedules the next chunk tick after successful progress: `nextActionAtMs = now + tick interval`.
   */
  private async scheduleNextProgressAction(jobId: string) {
    const intervalMs = toInt((this.env as any).ZIP_V2_TICK_INTERVAL_MS, DEFAULT_TICK_INTERVAL_MS);
    const nextActionAtMs = nowMs() + Math.max(0, intervalMs);
    const fresh = this.getJobRow(jobId);
    if (!fresh) {
      console.warn(`[zip-v2] scheduleNextProgressAction: job row missing`, { jobId });
      return;
    }
    this.upsertJob({ ...fresh, updatedAtMs: nowMs(), nextActionAtMs });
    await this.rescheduleStorageAlarmFromDb();
    console.log(`[zip-v2] Next progress action scheduled.`, {
      jobId,
      transferId: fresh.transferId,
      nextActionAtMs,
      event: "chunk.succeeded" satisfies ZipV2LifecycleEvent,
    });
  }

  private getJobRow(jobId: string): JobStateRow | null {
    const sql = this.sql();
    const rs = sql.exec(
      `SELECT jobId, transferId, status, createdAtMs, updatedAtMs, errorMessage,
              consecutiveFailures, lastFailureAtMs, nextRetryAtMs, lastErrorKind, cleanupAtMs, nextTickAtMs, nextActionAtMs,
              isTransferStatusUpdated
       FROM job_state WHERE jobId = ?;`,
      jobId,
    );
    const rows = rs.toArray?.() ?? [];
    const row = rows[0];
    if (!row) {
      return null;
    }

    const r: any = row;
    // Prefer `nextActionAtMs`; fall back to legacy columns until migration UPDATE has run.
    const effectiveNextAction = r.nextActionAtMs ?? r.nextTickAtMs ?? r.nextRetryAtMs;
    const isTransferStatusUpdated = optionalTriStateBoolFromSql(r.isTransferStatusUpdated);

    return {
      jobId: r.jobId,
      transferId: r.transferId,
      status: r.status,
      createdAtMs: r.createdAtMs,
      updatedAtMs: r.updatedAtMs,
      errorMessage: r.errorMessage ?? undefined,
      consecutiveFailures: r.consecutiveFailures ?? 0,
      lastFailureAtMs: r.lastFailureAtMs ?? undefined,
      nextActionAtMs: effectiveNextAction ?? undefined,
      lastErrorKind: (r.lastErrorKind ?? undefined) as any,
      cleanupAtMs: r.cleanupAtMs ?? undefined,
      isTransferStatusUpdated,
    } satisfies JobStateRow;
  }

  private getActiveJobRow(jobId: string): JobStateRow | null {
    const job = this.getJobRow(jobId);
    const activeStatuses = ["PENDING", "RUNNING", "FINALIZING", "DONE"];

    if (!job || !activeStatuses.includes(job.status)) {
      return null;
    }

    return job;
  }

  private getCheckpoint(jobId: string): Checkpoint | null {
    const sql = this.sql();
    const rs = sql.exec(
      `SELECT jobId, manifestKey, outputKey, uploadId, partSize, nextPartNumber, fileIndex, zipOffset, bytesWrittenTotal, filesDone, done
       FROM checkpoint WHERE jobId = ?;`,
      jobId,
    );

    const rows = rs.toArray?.() ?? [];
    const row = rows[0];
    if (!row) {
      return null;
    }

    const r: any = row;

    return {
      manifestKey: r.manifestKey,
      outputKey: r.outputKey,
      uploadId: r.uploadId ?? undefined,
      partSize: r.partSize,
      nextPartNumber: r.nextPartNumber,
      fileIndex: r.fileIndex,
      zipOffset: r.zipOffset,
      bytesWrittenTotal: r.bytesWrittenTotal,
      filesDone: r.filesDone,
      done: Boolean(r.done),
    };
  }

  private upsertJob(row: JobStateRow) {
    const sql = this.sql();
    const notifySql = sqlIntegerFromOptionalTriStateBool(row.isTransferStatusUpdated);
    // Always null legacy wake columns so reads prefer `nextActionAtMs` after migration.
    sql.exec(
      `INSERT INTO job_state (
         jobId, transferId, status, createdAtMs, updatedAtMs, errorMessage,
         consecutiveFailures, lastFailureAtMs, nextRetryAtMs, lastErrorKind, cleanupAtMs, nextTickAtMs, nextActionAtMs,
         isTransferStatusUpdated
       )
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
       ON CONFLICT(jobId) DO UPDATE SET
         transferId=excluded.transferId,
         status=excluded.status,
         updatedAtMs=excluded.updatedAtMs,
         errorMessage=excluded.errorMessage,
         consecutiveFailures=excluded.consecutiveFailures,
         lastFailureAtMs=excluded.lastFailureAtMs,
         nextRetryAtMs=NULL,
         lastErrorKind=excluded.lastErrorKind,
         cleanupAtMs=excluded.cleanupAtMs,
         nextTickAtMs=NULL,
         nextActionAtMs=excluded.nextActionAtMs,
         isTransferStatusUpdated=excluded.isTransferStatusUpdated;`,
      row.jobId,
      row.transferId,
      row.status,
      row.createdAtMs,
      row.updatedAtMs,
      row.errorMessage ?? null,
      row.consecutiveFailures ?? 0,
      row.lastFailureAtMs ?? null,
      null,
      row.lastErrorKind ?? null,
      row.cleanupAtMs ?? null,
      null,
      row.nextActionAtMs ?? null,
      notifySql,
    );
  }

  private upsertCheckpoint(jobId: string, cp: Checkpoint) {
    const sql = this.sql();
    sql.exec(
      `INSERT INTO checkpoint
        (jobId, manifestKey, outputKey, uploadId, partSize, nextPartNumber, fileIndex, zipOffset, bytesWrittenTotal, filesDone, done)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
       ON CONFLICT(jobId) DO UPDATE SET
         manifestKey=excluded.manifestKey,
         outputKey=excluded.outputKey,
         uploadId=excluded.uploadId,
         partSize=excluded.partSize,
         nextPartNumber=excluded.nextPartNumber,
         fileIndex=excluded.fileIndex,
         zipOffset=excluded.zipOffset,
         bytesWrittenTotal=excluded.bytesWrittenTotal,
         filesDone=excluded.filesDone,
         done=excluded.done;`,
      jobId,
      cp.manifestKey,
      cp.outputKey,
      cp.uploadId ?? null,
      cp.partSize,
      cp.nextPartNumber,
      cp.fileIndex,
      cp.zipOffset,
      cp.bytesWrittenTotal,
      cp.filesDone,
      cp.done ? 1 : 0,
    );
  }

  private insertUploadedParts(jobId: string, parts: UploadedPart[]) {
    const sql = this.sql();
    for (const p of parts) {
      sql.exec(
        `INSERT INTO uploaded_parts (jobId, partNumber, etag, sizeBytes)
         VALUES (?, ?, ?, ?)
         ON CONFLICT(jobId, partNumber) DO UPDATE SET etag=excluded.etag, sizeBytes=excluded.sizeBytes;`,
        jobId,
        p.partNumber,
        p.etag,
        p.sizeBytes,
      );
    }
  }

  private listUploadedPartsForComplete(jobId: string): CompletePart[] {
    const sql = this.sql();
    const rs = sql.exec(
      `SELECT partNumber, etag
       FROM uploaded_parts WHERE jobId = ? ORDER BY partNumber ASC;`,
      jobId,
    );
    const rows = rs.toArray?.() ?? [];
    return rows.map((r: any) => ({
      partNumber: r.partNumber,
      etag: r.etag,
    })) satisfies CompletePart[];
  }

  private upsertZipEntry(
    jobId: string,
    fileIndex: number,
    entry: Omit<ZipEntryRow, "jobId" | "fileIndex">,
  ) {
    const sql = this.sql();
    sql.exec(
      `INSERT INTO zip_entries
        (jobId, fileIndex, nameB64, crc32, compressedSize, uncompressedSize, localHeaderOffset, modTime, modDate)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
       ON CONFLICT(jobId, fileIndex) DO UPDATE SET
         nameB64=excluded.nameB64,
         crc32=excluded.crc32,
         compressedSize=excluded.compressedSize,
         uncompressedSize=excluded.uncompressedSize,
         localHeaderOffset=excluded.localHeaderOffset,
         modTime=excluded.modTime,
         modDate=excluded.modDate;`,
      jobId,
      fileIndex,
      entry.nameB64,
      entry.crc32,
      entry.compressedSize,
      entry.uncompressedSize,
      entry.localHeaderOffset,
      entry.modTime,
      entry.modDate,
    );
  }

  private listZipEntries(jobId: string): Array<Omit<ZipEntryRow, "jobId">> {
    const sql = this.sql();
    const rs = sql.exec(
      `SELECT fileIndex, nameB64, crc32, compressedSize, uncompressedSize, localHeaderOffset, modTime, modDate
       FROM zip_entries WHERE jobId = ? ORDER BY fileIndex ASC;`,
      jobId,
    );
    const rows = rs.toArray?.() ?? [];
    return rows as Array<Omit<ZipEntryRow, "jobId">>;
  }
}

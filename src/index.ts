import {
  Env,
  NotificationProcessMessage,
  QueueMessage,
  QueueMessageType,
  RequestPath,
  TransferStatus,
  ZipJob,
  ZipV2LifecycleEvent,
  ZipV2TickMessage,
  ZipV2TickMessageData,
} from "./lib/types/types";
import { verifyHmac } from "./lib/crypto";
import { WebAPIService } from "./modules/web-api-service";
import { Zipper } from "./modules/zipper";
import { CronHandler } from "./modules/cron";
import { processNotificationMessage } from "./modules/notification-processor";
import { resolveOutputKey, writeZipManifest, toBool } from "./modules/job-manifest";
import { JobManagerDO } from "./modules/job-manager-do";
import { ZipSemaphoreDO } from "./modules/semaphore-do";
import { ZipContainerDO } from "./modules/zip-container";
import { ZipLocksDO } from "./modules/ziplock-do-v1";
import { StreamIngestor } from "./modules/stream-ingestor";

// Export the Durable Objects for use in other files
export { ContainerProxy } from "@cloudflare/containers";
export { JobManagerDO, ZipSemaphoreDO, ZipContainerDO, ZipLocksDO };

export default {
  /**
   * Simple HTTP producer(endpoint) to enqueue jobs
   * @param req - The incoming request
   * @param env - The environment variables
   */
  async fetch(req: Request, env: Env): Promise<Response> {
    if (req.method !== "POST") {
      return new Response(undefined, { status: 405 });
    }

    const url = new URL(req.url);
    console.log(`Executing worker for url ${url.pathname}`);

    if (![RequestPath.COMPRESS_FILES, RequestPath.STREAM_INGEST].includes(url.pathname as any)) {
      return new Response(undefined, { status: 404 });
    }

    if (env.SKIP_REQUEST_VERIFICATION) {
      console.warn("Skipping request verification");
    } else {
      try {
        await verifyHmac(req, env.SECRET_KEY);
      } catch (error: any) {
        console.error("HMAC verification failed:", error?.message, error);
        return new Response("Unauthorized", { status: 401 });
      }
    }

    try {
      if (url.pathname === RequestPath.COMPRESS_FILES) {
        const body = await req.json<ZipJob>();
        console.log("Compressing files:", JSON.stringify(body));

        if (!body.objectPrefix) {
          return new Response("Missing prefix", { status: 400 });
        }

        const job: ZipJob = {
          transferId: body.transferId,
          objectPrefix: body.objectPrefix,
          zipOutputKey: body.zipOutputKey,
          includeEmpty: body.includeEmpty ?? true,
          createdBy: body.createdBy ?? "api",
          files: body.files,
        };

        const useV2 = toBool(env.ZIP_USE_CONTAINERS, false);
        if (!useV2) {
          const message: QueueMessage = { type: QueueMessageType.ZIP, data: job };
          console.log("Sending job to queue:", JSON.stringify(message));
          await env.QUEUE_WORKER_MAIN.send(message);
          console.log("Job queued", JSON.stringify(message));
        } else {
          // Stable ID so repeated triggers resume the same JobManagerDO state.
          // One ZIP v2 job per transfer.
          const jobId = job.transferId;
          const outputKey = resolveOutputKey(env, job);

          // Idempotent start: if the output already exists, do not restart work.
          const existingOut = await env.OUTPUT_BUCKET.head(outputKey);
          if (existingOut) {
            console.log(`[zip-v2] Output already exists; skipping start.`, {
              jobId,
              transferId: job.transferId,
              outputKey,
              outputBytes: existingOut.size,
            });

            // Notify the Web API that the job is done
            const webAPIService = new WebAPIService(env.SECRET_KEY, env.WEB_API_BASE_URL);
            try {
              await webAPIService.updateTransferStatus(job.transferId, {
                status: TransferStatus.READY,
                bundleObjectKey: outputKey,
              });
            } catch (e) {
              console.warn(`[zip-v2] Failed to reconcile transfer status (output exists path)`, {
                error: e,
                jobId,
                transferId: job.transferId,
                outputKey,
              });
            }

            return new Response("Enqueued", { status: 202 });
          }

          const { manifestKey } = await writeZipManifest({
            env,
            jobId,
            zipJob: job,
            outputKey,
          });

          // Start a new zip v2 job by forwarding to JobManagerDO
          const jobManagerId = env.JobManager.idFromName(jobId);
          const jobManager = env.JobManager.get(jobManagerId);

          const resp = await jobManager.fetch("https://job/start", {
            method: "POST",
            body: JSON.stringify({
              jobId,
              transferId: job.transferId,
              manifestKey,
              outputKey,
            }),
          });

          if (!resp.ok) {
            throw new Error(`Failed to start zip v2 job: ${resp.status} ${await resp.text()}`);
          }

          // Enqueue a tick message to the queue to start the zip v2 job
          const tick: ZipV2TickMessageData = { jobId };
          const message: QueueMessage = {
            type: QueueMessageType.ZIP_V2_TICK,
            data: tick,
          };
          console.log("Sending zip v2 tick to queue:", JSON.stringify(message));

          await env.QUEUE_WORKER_MAIN.send(message);
          console.log("Zip v2 job queued", JSON.stringify({ jobId, manifestKey, outputKey }));
        }
      } else if (url.pathname === RequestPath.STREAM_INGEST) {
        const body = await req.json<any>();
        console.log("Stream ingest request:", JSON.stringify(body));

        if (!body?.transferId || !body?.fileId || !body?.r2PresignedGetUrl) {
          return new Response("Missing required fields", { status: 400 });
        }

        const message: QueueMessage = { type: QueueMessageType.STREAM_INGEST, data: body };
        console.log("Sending stream ingest job to queue:", JSON.stringify(message));
        await env.QUEUE_WORKER_MAIN.send(message);
        console.log("Stream ingest job queued", JSON.stringify(message));
      }
    } catch (error) {
      console.error("Failed to enqueue job:", error);
      return new Response("Failed to enqueue job", { status: 500 });
    }

    return new Response("Enqueued", { status: 202 });
  },

  /**
   * Queue consumer: does the actual ZIP work in the background. This function
   * processes a batch of ZIP jobs from the queue.
   * @param batch - The batch of messages to process
   * @param env - The environment variables
   * @param ctx - The execution context
   */
  async scheduled(event: ScheduledEvent, env: Env, ctx: ExecutionContext) {
    const now = Date.now();
    console.log(`[scheduled] Cron triggered: "${event.cron}" at ${now}`);

    const cronHandler = new CronHandler(env);
    const webAPIService = new WebAPIService(env.SECRET_KEY, env.WEB_API_BASE_URL);

    switch (event.cron) {
      // Every 3 hours
      case "0 */3 * * *": {
        ctx.waitUntil(cronHandler.handleCleanupExpiredTransfersCron(webAPIService, now));
        break;
      }

      // Every 6 hours at 01:00, 07:00, 13:00, 19:00 UTC (offset from */3 cleanup so both runs don't overlap)
      case "0 1,7,13,19 * * *": {
        ctx.waitUntil(cronHandler.handleCleanupAbandonedUploadsCron(webAPIService, now));
        break;
      }

      // Every 15 minutes — notification schedule (transfer expiry, review digest, etc.)
      case "*/15 * * * *": {
        ctx.waitUntil(cronHandler.handleNotificationScheduleCron(webAPIService, now));
        break;
      }

      default:
        console.warn(`[scheduled] Unhandled cron schedule: "${event.cron}"`);
    }
  },

  async queue(batch: MessageBatch<QueueMessage>, env: Env) {
    const webAPIService = new WebAPIService(env.SECRET_KEY, env.WEB_API_BASE_URL);

    for (const msg of batch.messages) {
      console.log(`Processing message ${msg.id} from batch`, JSON.stringify(msg.body));

      const messageType = msg.body.type;
      switch (messageType) {
        // Handle ZIP v1 jobs
        case QueueMessageType.ZIP:
          await new Zipper(env).zip(msg, webAPIService);
          break;

        // Handle ZIP v2 tick messages
        case QueueMessageType.ZIP_V2_TICK:
          await handleZipV2Tick(msg as Message<ZipV2TickMessage>, env);
          break;

        // Handle video stream ingest jobs
        case QueueMessageType.STREAM_INGEST:
          await new StreamIngestor(env).ingest(msg);
          break;

        case QueueMessageType.NOTIFICATION_PROCESS:
          await processNotificationMessage(
            msg as Message<NotificationProcessMessage>,
            webAPIService,
          );
          break;

        default:
          console.error(`Unknown message type: ${messageType}`);
          msg.ack();
          break;
      }
    }
  },
};

// ------------------------------------------------------------------------------
// Helper functions
// ------------------------------------------------------------------------------

/**
 * Trigger-only ZIP v2 tick.
 *
 * Forwards work to `JobManagerDO`. Retry/backoff is handled inside the DO
 * via `nextActionAtMs` and alarms, not here.
 *
 * Queue `retry()` should only be used for infrastructure failures, such as
 * RPC errors, an unreachable DO, or non-2xx responses.
 *
 * Always `ack()` after a successful DO response, even if the JSON body fails
 * to parse. The job may still have progressed.
 */
async function handleZipV2Tick(msg: Message<ZipV2TickMessage>, env: Env) {
  const { jobId } = msg.body.data;
  try {
    const id = env.JobManager.idFromName(jobId);
    const stub = env.JobManager.get(id);
    const resp = await stub.fetch("https://job/tick", {
      method: "POST",
      body: JSON.stringify({ jobId }),
    });

    if (!resp.ok) {
      throw new Error(`JobManager tick failed: ${resp.status} ${await resp.text()}`);
    }

    try {
      await resp.json();
    } catch (parseErr: unknown) {
      const errMsg = parseErr instanceof Error ? parseErr.message : String(parseErr);
      console.error(`[zip-v2] tick JSON parse failed. Ack message to allow job to progress.`, {
        jobId,
        error: errMsg,
        event: "tick.consumer.failure" satisfies ZipV2LifecycleEvent,
      });
    }

    msg.ack();
  } catch (e) {
    console.error("[zip-v2] tick failed (infrastructure):", {
      jobId,
      error: e,
      event: "tick.consumer.failure" satisfies ZipV2LifecycleEvent,
    });
    msg.retry({ delaySeconds: 30 });
  }
}

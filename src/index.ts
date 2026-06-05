import {
  Env,
  NOTIFICATION_QUEUE_NAMES,
  NotificationProcessMessage,
  QueueMessage,
  QueueMessageType,
  RequestPath,
  ZipV2TickMessage,
} from "./lib/types/types";
import { verifyRequest } from "./lib/crypto";
import { WebAPIService } from "./modules/web-api-service";
import { CronHandler } from "./modules/cron";
import { processNotificationBatch } from "./modules/notification/notification-processor";
import { handleCompressFilesRequest, processZipTick } from "./modules/zip-processor";
import { JobManagerDO } from "./modules/job-manager-do";
import { ZipSemaphoreDO } from "./modules/semaphore-do";
import { ZipContainerDO } from "./modules/zip-container";
import { StreamIngestor } from "./modules/stream/stream-ingestor";

// Export the Durable Objects for use in other files
export { ContainerProxy } from "@cloudflare/containers";
export { JobManagerDO, ZipSemaphoreDO, ZipContainerDO };

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

    const authError = await verifyRequest(req, env);
    if (authError) {
      return authError;
    }

    try {
      switch (url.pathname) {
        case RequestPath.COMPRESS_FILES:
          return await handleCompressFilesRequest(req, env);
        case RequestPath.STREAM_INGEST:
          return await StreamIngestor.handleStreamIngestRequest(req, env);
        default:
          return new Response(undefined, { status: 404 });
      }
    } catch (error) {
      console.error("Failed to enqueue job:", error);
      return new Response("Failed to enqueue job", { status: 500 });
    }
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

  /**
   * Queue consumer: processes messages from the queue.
   * @param batch - The batch of messages to process
   * @param env - The environment variables
   */
  async queue(batch: MessageBatch<QueueMessage>, env: Env) {
    const webAPIService = new WebAPIService(env.SECRET_KEY, env.WEB_API_BASE_URL);

    console.log(`Processing ${batch.messages.length} messages from queue ${batch.queue}`);

    if (NOTIFICATION_QUEUE_NAMES.has(batch.queue)) {
      await processNotificationBatch(
        batch.messages as Message<NotificationProcessMessage>[],
        webAPIService,
      );
      return;
    }

    for (const msg of batch.messages) {
      console.log(`Processing message ${msg.id} from batch`, JSON.stringify(msg.body.type));

      const messageType = msg.body.type;
      switch (messageType) {
        case QueueMessageType.ZIP_V2_TICK:
          await processZipTick(msg as Message<ZipV2TickMessage>, env);
          break;

        case QueueMessageType.STREAM_INGEST:
          await new StreamIngestor(env).ingest(msg);
          break;

        default:
          console.error(`Unknown message type: ${messageType}`);
          msg.ack();
          break;
      }
    }
  },
};

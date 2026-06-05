import {
  Env,
  QueueMessage,
  QueueMessageType,
  TransferStatus,
  ZipJob,
  ZipV2LifecycleEvent,
  ZipV2TickMessage,
  ZipV2TickMessageData,
} from "../lib/types/types";
import { resolveOutputKey, writeZipManifest } from "./job-manifest";
import { WebAPIService } from "./web-api-service";

export async function handleCompressFilesRequest(req: Request, env: Env): Promise<Response> {
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

  // Stable ID so repeated triggers resume the same JobManagerDO state.
  const jobId = job.transferId;
  const outputKey = resolveOutputKey(env, job);

  const existingOut = await env.OUTPUT_BUCKET.head(outputKey);
  if (existingOut) {
    console.log(`[zip] Output already exists; skipping start.`, {
      jobId,
      transferId: job.transferId,
      outputKey,
      outputBytes: existingOut.size,
    });

    const webAPIService = new WebAPIService(env.SECRET_KEY, env.WEB_API_BASE_URL);
    try {
      await webAPIService.updateTransferStatus(job.transferId, {
        status: TransferStatus.READY,
        bundleObjectKey: outputKey,
      });
    } catch (e) {
      console.warn(`[zip] Failed to reconcile transfer status (output exists path)`, {
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
    throw new Error(`Failed to start zip job: ${resp.status} ${await resp.text()}`);
  }

  const tick: ZipV2TickMessageData = { jobId };
  const message: QueueMessage = {
    type: QueueMessageType.ZIP_V2_TICK,
    data: tick,
  };
  console.log("Sending zip tick to queue:", JSON.stringify(message));

  await env.QUEUE_WORKER_MAIN.send(message);
  console.log("Zip job queued", JSON.stringify({ jobId, manifestKey, outputKey }));

  return new Response("Enqueued", { status: 202 });
}

/**
 * Trigger-only ZIP tick. Forwards work to `JobManagerDO`.
 * Retry/backoff is handled inside the DO via `nextActionAtMs` and alarms.
 */
export async function processZipTick(msg: Message<ZipV2TickMessage>, env: Env): Promise<void> {
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
      console.error(`[zip] tick JSON parse failed. Ack message to allow job to progress.`, {
        jobId,
        error: errMsg,
        event: "tick.consumer.failure" satisfies ZipV2LifecycleEvent,
      });
    }

    msg.ack();
  } catch (e) {
    console.error("[zip] tick failed (infrastructure):", {
      jobId,
      error: e,
      event: "tick.consumer.failure" satisfies ZipV2LifecycleEvent,
    });
    msg.retry({ delaySeconds: 30 });
  }
}

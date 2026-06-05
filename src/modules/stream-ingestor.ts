import { Env, QueueMessage, QueueMessageType, StreamIngestJob } from "../lib/types/types";
import { calculateExponentialBackoff } from "../lib/utils";

type CloudflareApiResponse<T> = {
  success: boolean;
  errors?: Array<{ code?: number; message?: string }>;
  messages?: Array<{ code?: number; message?: string }>;
  result?: T;
};

type StreamVideo = {
  uid?: string;
  created?: string;
  modified?: string;
  readyToStream?: boolean;
  preview?: string;
  thumbnail?: string;
  playback?: {
    hls?: string;
    dash?: string;
  };
  status?: {
    state?: string;
    pctComplete?: string;
    errorReasonCode?: string;
    errorReasonText?: string;
  };
  meta?: any;
};

export class StreamIngestor {
  constructor(private readonly env: Env) {}

  async ingest(msg: Message<QueueMessage>) {
    const job = msg.body.data as StreamIngestJob;

    try {
      const result = await this.copyFromMediaUrlToStreamService(job);
      console.log(
        `[stream] Created/updated Stream video for file ${job.fileId}: uid=${result.uid} state=${result.status?.state}`,
      );
      msg.ack();
    } catch (err) {
      const delaySeconds = calculateExponentialBackoff(
        msg.attempts,
        this.env.BASE_RETRY_DELAY_SECONDS,
      );
      console.error(
        `[stream] Ingest job failed. Attempt: ${msg.attempts}. Retry in ${delaySeconds} seconds:`,
        err,
      );
      msg.retry({ delaySeconds });
    }
  }

  private isDev(): boolean {
    return this.env.WEB_API_BASE_URL.includes("localhost");
  }

  private getAllowedOrigins(): string[] {
    const isDev = this.isDev();
    const allowedOrigins = ["moretransfer.com", "www.moretransfer.com"];

    if (isDev) {
      allowedOrigins.push("localhost:3000");
    }

    return allowedOrigins;
  }

  private getStreamDisplayName(fileId: string): string {
    const isDev = this.isDev();
    return isDev ? `dev_fi_${fileId}` : `fi_${fileId}`;
  }

  private getCreator(transferUserId: string | null | undefined): string | undefined {
    return typeof transferUserId === "string" && transferUserId.trim().length > 0
      ? transferUserId.trim()
      : undefined;
  }

  private getScheduledDeletion(transferExpiresAt: string | undefined): string | undefined {
    if (!transferExpiresAt) {
      return undefined;
    }
    let scheduledDeletion: string | undefined;
    const expiresAt = new Date(transferExpiresAt);
    if (Number.isNaN(expiresAt.getTime())) {
      console.warn(
        `[stream] Invalid transferExpiresAt, skipping scheduledDeletion: ${transferExpiresAt}`,
      );
    } else {
      // Cloudflare requires deletion at least 30 days after upload (Minimum retention period is 30 days).
      const minMs = Date.now() + 30 * 24 * 60 * 60 * 1000;
      scheduledDeletion = new Date(Math.max(expiresAt.getTime(), minMs)).toISOString();
    }

    return scheduledDeletion;
  }

  private async copyFromMediaUrlToStreamService(job: StreamIngestJob): Promise<StreamVideo> {
    if (!this.env.CLOUDFLARE_ACCOUNT_ID || !this.env.CLOUDFLARE_STREAM_API_TOKEN) {
      throw new Error("Missing CLOUDFLARE_ACCOUNT_ID or CLOUDFLARE_STREAM_API_TOKEN");
    }

    const url = `https://api.cloudflare.com/client/v4/accounts/${this.env.CLOUDFLARE_ACCOUNT_ID}/stream/copy`;
    const allowedOrigins = this.getAllowedOrigins();
    const streamDisplayName = this.getStreamDisplayName(job.fileId);
    const creator = this.getCreator(job.transferUserId);
    const scheduledDeletion = this.getScheduledDeletion(job.transferExpiresAt);
    const requireSignedURLs = this.env.STREAM_REQUIRE_SIGNED_URLS === true;

    const body: Record<string, unknown> = {
      url: job.r2PresignedGetUrl,
      meta: { ...job.meta, name: streamDisplayName },
      requireSignedURLs,
      allowedOrigins,
    };

    if (creator) {
      body.creator = creator;
    }
    if (scheduledDeletion) {
      body.scheduledDeletion = scheduledDeletion;
    }

    const headers: Record<string, string> = {
      "content-type": "application/json",
      authorization: `Bearer ${this.env.CLOUDFLARE_STREAM_API_TOKEN}`,
    };
    if (creator) {
      headers["Upload-Creator"] = creator;
    }

    const res = await fetch(url, {
      method: "POST",
      headers,
      body: JSON.stringify(body),
    });

    const data = (await res.json().catch(() => ({}))) as CloudflareApiResponse<StreamVideo>;
    if (!res.ok || !data.success || !data.result) {
      const errMsg =
        data?.errors
          ?.map((e) => e.message)
          .filter(Boolean)
          .join("; ") || `HTTP ${res.status} ${res.statusText}`;
      throw new Error(`[stream] Cloudflare API error: ${errMsg}`);
    }

    return data.result;
  }

  static async handleStreamIngestRequest(req: Request, env: Env): Promise<Response> {
    const body = await req.json<StreamIngestJob>();
    console.log("Stream ingest request:", JSON.stringify(body));

    if (!body?.transferId || !body?.fileId || !body?.r2PresignedGetUrl) {
      return new Response("Missing required fields", { status: 400 });
    }

    const message: QueueMessage = { type: QueueMessageType.STREAM_INGEST, data: body };
    console.log("Sending stream ingest job to queue:", JSON.stringify(message));
    await env.QUEUE_WORKER_MAIN.send(message);
    console.log("Stream ingest job queued", JSON.stringify(message));

    return new Response("Enqueued", { status: 202 });
  }
}

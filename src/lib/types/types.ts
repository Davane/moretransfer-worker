export interface Env {
  SOURCE_BUCKET: R2Bucket;
  OUTPUT_BUCKET: R2Bucket;

  // Queues
  QUEUE_WORKER_MAIN: Queue;

  // Durable Object
  ZipLocks: DurableObjectNamespace;
  JobManager: DurableObjectNamespace;
  ZipSemaphore: DurableObjectNamespace;
  ZipContainer: DurableObjectNamespace;

  // Environment variables
  SECRET_KEY: string;
  WEB_API_BASE_URL: string;
  ZIP_OUTPUT_PREFIX: string;
  ZIP_OUTPUT_FILE_NAME: string;
  MAX_FILES?: string;
  MAX_ZIP_BYTES?: string;
  BASE_RETRY_DELAY_SECONDS: number;
  SKIP_REQUEST_VERIFICATION: boolean | undefined;

  // ZIP v2 (containers)
  ZIP_USE_CONTAINERS: boolean | undefined;
  ZIP_GLOBAL_CONCURRENCY: string | undefined; // default 1
  ZIP_MANIFEST_PREFIX: string | undefined; // default "manifests"
  ZIP_PART_SIZE_BYTES: string | undefined; // default 134217728 (128 MiB)
  ZIP_MAX_PARTS_PER_TICK: string | undefined; // default 8
  BUNDLE_CLEANUP_TTL_DAYS: number | undefined;

  // Cloudflare Stream
  CLOUDFLARE_ACCOUNT_ID: string;
  CLOUDFLARE_STREAM_API_TOKEN: string;
  /** Set to "false" to ingest with public playback URLs (rollback). Omit or any other value = signed URLs required. */
  STREAM_REQUIRE_SIGNED_URLS: boolean | undefined;
}

export enum RequestPath {
  COMPRESS_FILES = "/compress-files",
  STREAM_INGEST = "/stream-ingest",
}

export type RequestCredentials = "include" | "omit" | "same-origin";

export interface ZipJob {
  transferId: string;
  objectPrefix: string; // R2 prefix to collect
  zipOutputKey?: string; // optional custom output key in OUTPUT_BUCKET
  includeEmpty?: boolean; // include zero-byte files (default true)
  createdBy?: string; // optional audit
  files?: Array<{ key: string; relativePath?: string }>; // file mappings for folder structure
}

export type ZipJobManifest = {
  version: "v1";
  jobId: string;
  transferId: string;
  outputKey: string;
  createdAtMs: number;
  includeEmpty: boolean;
  files: Array<{
    key: string;
    nameInZip: string;
    size: number;
  }>;
};

export type ZipV2TickMessageData = {
  jobId: string;
};

export interface StreamIngestJob {
  transferId: string;
  fileId: string;
  r2PresignedGetUrl: string;
  /**
   * Used to set Stream `creator` and `Upload-Creator` header.
   * Optional because transfers may have null userId (e.g. deleted user).
   */
  transferUserId?: string | null;
  /**
   * ISO string of transfers.expiresAt; used to set Stream scheduledDeletion.
   */
  transferExpiresAt?: string;
  meta: {
    transferId: string;
    fileId: string;
    filename?: string;
    mimeType?: string;
  };
}

export enum TransferStatus {
  PENDING = "pending", // currently uploading or about to start
  COMPRESSING = "compressing",
  READY_BUT_COMPRESSION_FAILED = "ready_but_compression_failed",
  READY = "ready",
  FAILED = "failed",
}

export interface TransferUpdateRequest {
  status: TransferStatus,
  bundleObjectKey?: string;
}

export enum QueueMessageType {
  ZIP = "zip",
  ZIP_V2_TICK = "zip_v2_tick",
  STREAM_INGEST = "stream_ingest",
  NOTIFICATION_PROCESS = "notification_process",
}

interface ZipMessage {
  type: QueueMessageType.ZIP;
  data: ZipJob;
}

export interface ZipV2TickMessage {
  type: QueueMessageType.ZIP_V2_TICK;
  data: ZipV2TickMessageData;
}

interface StreamIngestMessage {
  type: QueueMessageType.STREAM_INGEST;
  data: StreamIngestJob;
}

export interface NotificationProcessMessage {
  type: QueueMessageType.NOTIFICATION_PROCESS;
  data: { jobId: string; correlationId: string };
}

export type QueueMessage =
  | ZipMessage
  | ZipV2TickMessage
  | StreamIngestMessage
  | NotificationProcessMessage;


// ------------------------------------------------------------------------------
// Zip V2 Job Manager DO types
// ------------------------------------------------------------------------------

/**
 * ZIP v2 (containers): centralized lifecycle and retries.
 *
 * `JobManagerDO` is the single scheduler. It owns the SQLite job state and
 * `storage.setAlarm()`, and it is the only place where business backoff and
 * `nextActionAtMs` are decided.
 *
 * The queue carries trigger-only `zip_v2_tick` messages so ticks can run within
 * queue concurrency limits. Queue retries are reserved for infrastructure
 * delivery failures only, such as RPC errors, unreachable DOs, or non-2xx
 * responses from the DO.
 *
 * Canonical statuses:
 * `PENDING` → `RUNNING` → `FINALIZING` → `DONE` | `FAILED`
 *
 * Canonical events used for logs and reasoning:
 * - `startRequested`: row and checkpoint created; status is `PENDING`.
 * - `tickDispatched`: queue consumer invoked `/tick`; work runs under the global semaphore.
 * - `lockUnavailable`: global semaphore is busy; defer via `nextActionAtMs`.
 * - `chunkSucceeded`: container uploaded parts and checkpoint advanced.
 * - `chunkRetryableFailure`: container/R2 failure was classified as retryable.
 * - `chunkTerminalFailure`: container/R2 failure was classified as terminal.
 * - `allChunksUploaded`: checkpoint is complete; enter `FINALIZING` and complete multipart upload.
 * - `finalizeRetryableFailure`: multipart completion failed with a retryable error.
 * - `finalizeTerminalFailure`: multipart completion failed with a terminal error.
 * - `outputVerified`: R2 head succeeded after finalize; mark `DONE` and schedule cleanup.
 * - `cleanupDue`: alarm purges terminal row state after the cleanup TTL.
 *
 * Global semaphore:
 * `ZipSemaphoreDO` limits concurrent container work only. It does not define job
 * identity. Per-job idempotency is still enforced by the active `job_state` row,
 * `transferId` / `jobId`, and output object existence.
 *
 * Manual reliability checks before and after deploy:
 * - Container 429/5xx: job remains `RUNNING` / `FINALIZING`, `nextActionAtMs`
 *   advances, and `consecutiveFailures` increments until the cap is reached.
 * - Finalize transient then success: multipart completes, job becomes `DONE`,
 *   and cleanup is scheduled.
 * - Semaphore busy: a tick that did not acquire a token must not release one;
 *   the next wake should be interval-based.
 * - Duplicate `start` / existing R2 output: idempotent paths should avoid
 *   duplicate multipart owners.
 */
export type JobStatus = "PENDING" | "RUNNING" | "FINALIZING" | "DONE" | "FAILED" | "CANCELLED";

/** Log-friendly lifecycle event labels (not persisted). */
export type ZipV2LifecycleEvent =
  | "job.start.requested"
  | "tick.dispatched"
  | "tick.dispatch.failed"
  | "tick.consumer.failure"
  | "tick.consumer.ack"
  | "tick.processing"
  | "tick.deferred"
  | "lock.unavailable"
  | "chunk.succeeded"
  | "chunk.failure.retryable"
  | "chunk.failure.terminal"
  | "chunk.uploads_completed"
  | "finalize.failure.retryable"
  | "finalize.failure.terminal"
  | "finalize.deferred"
  | "finalize.multipart_completed"
  | "output.verified"
  | "cleanup.due"
  | "cleanup.failure";

export type UploadedPart = { partNumber: number; etag: string; sizeBytes: number };

export type CompletePart = { partNumber: number; etag: string };

export type ErrorKind =
  | "container_5xx"
  | "container_429"
  | "container_4xx"
  | "container_timeout"
  | "r2_eof"
  | "r2_transient"
  | "bad_manifest"
  | "unknown";

export type ZipEntryRow = {
  jobId: string;
  fileIndex: number;
  nameB64: string;
  crc32: number;
  compressedSize: string;
  uncompressedSize: string;
  localHeaderOffset: string;
  modTime: number;
  modDate: number;
};

export type Checkpoint = {
  manifestKey: string;
  outputKey: string;
  uploadId?: string;
  partSize: number;
  nextPartNumber: number;
  fileIndex: number;
  zipOffset: string; // bigint as decimal string
  bytesWrittenTotal: string; // bigint as decimal string
  filesDone: number;
  done: boolean;
};

export type JobStateRow = {
  jobId: string;
  transferId: string;
  status: JobStatus;
  createdAtMs: number;
  updatedAtMs: number;
  errorMessage?: string;
  consecutiveFailures: number;
  lastFailureAtMs?: number;
  /** When the next tick / retry is allowed; sole non-cleanup wake field (replaces legacy nextTickAtMs + nextRetryAtMs). */
  nextActionAtMs?: number;
  lastErrorKind?: ErrorKind;
  cleanupAtMs?: number;
  /** Whether the Web API accepted the last compression-status PUT for this job. */
  isTransferStatusUpdated?: boolean;
};

export type StartJobRequest = {
  jobId: string;
  transferId: string;
  manifestKey: string;
  outputKey: string;
};

export type TickJobRequest = {
  jobId: string;
};

export type TickJobResponse = {
  status: JobStatus;
  done: boolean;
  retryAfterSeconds?: number;
};

export type RunChunkResponse = {
  uploadedParts: UploadedPart[];
  nextPartNumber: number;
  fileIndex: number;
  zipOffset: string;
  bytesWrittenTotal: string;
  filesDone: number;
  done: boolean;
};



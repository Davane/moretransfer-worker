# Moretransfer Worker

A Cloudflare Worker that powers background processing for [Moretransfer](https://www.moretransfer.com): **ZIP bundling** (Cloudflare Containers), **Cloudflare Stream ingest**, **scheduled notifications**, and **transfer cleanup** crons. Jobs are processed asynchronously via Cloudflare Queues and Durable Objects.

## Features

- **ZIP (containers)**: Resumable ZIP64 STORE archives built in a Go container, coordinated by `JobManagerDO` with SQLite checkpointing and R2 multipart uploads
- **Global concurrency control**: `ZipSemaphoreDO` limits how many container ZIP jobs run at once
- **Cloudflare Stream ingest**: Copies video from R2 presigned URLs into Cloudflare Stream for preview playback
- **Notification workflow**: Cron-driven schedule → dedicated notification queue → batched delivery via the Web API
- **Transfer maintenance crons**: Expired-transfer cleanup and abandoned-upload cleanup
- **HMAC-authenticated HTTP producers**: Signed POST endpoints for the Next.js API

## Architecture

```
┌─────────────────┐     POST /compress-files      ┌──────────────────┐
│   Next.js API   │ ────────────────────────────► │  Worker (fetch)  │
│                 │     POST /stream-ingest       │                  │
└─────────────────┘                               └────────┬─────────┘
                                                           │
                    ┌──────────────────────────────────────┼──────────────────────────┐
                    │                                      │                          │
                    ▼                                      ▼                          ▼
           QUEUE_WORKER_MAIN                    QUEUE_NOTIFICATIONS            Cron triggers
           (zip_v2_tick, stream_ingest)         (notification_process)          (cleanup + schedule)
                    │                                      │
        ┌───────────┴───────────┐                          │
        ▼                       ▼                          ▼
  JobManagerDO            StreamIngestor          notification-processor
  ZipSemaphoreDO          (Cloudflare Stream)     → Web API /notifications/process
  ZipContainerDO
  (Go ZIP container)
        │
        ▼
   R2 (source + output)
```

### Components

| Component | Role |
|-----------|------|
| **HTTP producer** | Accepts signed POSTs to `/compress-files` and `/stream-ingest` |
| **Main queue** (`QUEUE_WORKER_MAIN`) | ZIP tick triggers and Stream ingest jobs |
| **Notification queue** (`QUEUE_NOTIFICATIONS`) | Batched notification delivery jobs |
| **JobManagerDO** | ZIP lifecycle: checkpointing, retries, multipart finalize, cleanup TTL |
| **ZipSemaphoreDO** | Global cap on concurrent container ZIP work |
| **ZipContainerDO** | Cloudflare Container (Go) that streams ZIP64 STORE output to R2 |
| **Cron handler** | Expired transfers, abandoned uploads, notification scheduling |

## Prerequisites

- Cloudflare account with Workers, R2, Queues, Durable Objects, and Containers enabled
- R2 bucket(s) for source and output objects
- Wrangler CLI (`npm install` includes it)
- For Stream ingest: `CLOUDFLARE_ACCOUNT_ID` and `CLOUDFLARE_STREAM_API_TOKEN` secrets

## Configuration

### Environment variables

Set in `wrangler.toml` per environment (`dev` / `prod`) or via the Cloudflare dashboard.

**Core**

| Variable | Description |
|----------|-------------|
| `SOURCE_BUCKET` / `OUTPUT_BUCKET` | R2 bindings |
| `ZIP_OUTPUT_PREFIX` | Prefix for generated ZIP paths (e.g. `bundle`) |
| `ZIP_OUTPUT_FILE_NAME` | Base name for output object (e.g. `moretransfer_bundle` → `.zip`) |
| `MAX_FILES` | Max files per ZIP manifest (default in code: 500) |
| `MAX_ZIP_BYTES` | Max total uncompressed bytes (default: 64 GiB) |
| `BASE_RETRY_DELAY_SECONDS` | Queue retry backoff base |
| `SECRET_KEY` | HMAC secret shared with the Web API |
| `WEB_API_BASE_URL` | Next.js API base URL |

**ZIP (containers)**

| Variable | Description |
|----------|-------------|
| `ZIP_GLOBAL_CONCURRENCY` | Max concurrent container ZIP jobs (dev: `1`, prod: `3`) |
| `ZIP_MANIFEST_PREFIX` | R2 prefix for job manifests (default: `manifests`) |
| `ZIP_PART_SIZE_BYTES` | Multipart part size (default: 128 MiB) |
| `ZIP_MAX_PARTS_PER_TICK` | Parts uploaded per container chunk (default: `8`) |
| `BUNDLE_CLEANUP_TTL_DAYS` | Days before terminal job state is purged from `JobManagerDO` |

**Cloudflare Stream**

| Variable | Description |
|----------|-------------|
| `CLOUDFLARE_ACCOUNT_ID` | Cloudflare account (secret in prod) |
| `CLOUDFLARE_STREAM_API_TOKEN` | Stream API token (secret in prod) |
| `STREAM_REQUIRE_SIGNED_URLS` | Require signed playback URLs (set `false` to allow public preview during ingest) |

### Required resources

- **R2 buckets**: Source and output bindings
- **Queues**:
  - Main worker queue + DLQ (`QUEUE_WORKER_MAIN`)
  - Notification queue + DLQ (`QUEUE_NOTIFICATIONS`)
- **Durable Objects**: `JobManagerDO`, `ZipSemaphoreDO`, `ZipContainerDO`
- **Container**: `ZipContainerDO` image from `container/Dockerfile`

### Cron triggers (prod)

| Schedule | Handler |
|----------|---------|
| `0 */3 * * *` | Cleanup expired transfers |
| `0 1,7,13,19 * * *` | Cleanup abandoned uploads (staggered from expired cleanup) |
| `*/15 * * * *` | Notification schedule → enqueue `notification_process` jobs |

## Installation

```bash
git clone <repository-url>
cd moretransfer-worker
npm install
```

Configure `wrangler.toml`, create queues, and set secrets:

```bash
npm run deploy:dev   # or deploy:prod
wrangler secret put CLOUDFLARE_STREAM_API_TOKEN --env prod
wrangler secret put SECRET_KEY --env prod
```

## Usage

### Enqueue a ZIP job

POST to `/compress-files` with a JSON body matching `ZipJob`:

```json
{
  "transferId": "uuid",
  "objectPrefix": "path/to/files/",
  "zipOutputKey": "optional/custom/key.zip",
  "includeEmpty": true,
  "createdBy": "api",
  "files": [{ "key": "path/obj1", "relativePath": "folder/a.mxf" }]
}
```

**Parameters**

- `transferId` (required): Transfer id for status callbacks
- `objectPrefix` (required): R2 prefix for the transfer
- `zipOutputKey` (optional): Custom output key in the output bucket
- `includeEmpty` (optional): Include zero-byte files (default: `true`)
- `createdBy` (optional): Audit field; stored in ZIP object metadata
- `files` (optional): R2 keys mapped to `relativePath` inside the ZIP

**ZIP flow**

1. Worker writes a manifest to R2 (`manifests/<jobId>.json`)
2. `JobManagerDO` starts the job (`jobId` = `transferId`)
3. A `zip_v2_tick` message is sent to the main queue
4. Each tick acquires the global semaphore, runs a container chunk (`/runChunk`), uploads multipart parts, and advances the checkpoint
5. When all files are written, the DO finalizes multipart upload, verifies output, updates transfer status to `ready`, and schedules state cleanup

Output objects include `customMetadata.zipVersion` = `zip64-store-container-v1`.

### Enqueue Stream ingest

POST to `/stream-ingest`:

```json
{
  "transferId": "uuid",
  "fileId": "uuid",
  "r2PresignedGetUrl": "https://...",
  "transferUserId": "optional-user-id",
  "transferExpiresAt": "2026-12-31T00:00:00.000Z",
  "meta": {
    "transferId": "uuid",
    "fileId": "uuid",
    "filename": "video.mp4",
    "mimeType": "video/mp4"
  }
}
```

The worker copies the R2 object into Cloudflare Stream via the [Stream copy API](https://developers.cloudflare.com/stream/uploading-videos/direct-creator-uploads/), with optional signed URLs, allowed origins, creator metadata, and scheduled deletion aligned to transfer expiry.

### Notification workflow

1. **Schedule** (cron every 15 minutes): Worker calls `POST /api/external/notifications/schedule` on the Web API
2. **Enqueue**: Web API returns `jobIds`; worker batches them onto `QUEUE_NOTIFICATIONS` as `notification_process` messages (with a `correlationId`)
3. **Process** (notification queue consumer): Messages grouped by `correlationId` are sent to `POST /api/external/notifications/process`
4. **Ack/retry**: Per-job results (`sent`, `skipped`, `failed`, `retry`) drive message ack or retry

## Development

### Local development

```bash
npm run dev
# or without HMAC for local testing:
npm run dev:no-auth
```

### Test cron handlers locally

```bash
npm run dev:cron
npm run cron:cleanup
npm run cron:abandoned-uploads
npm run cron:notifications
```

### Typecheck

```bash
npm run typecheck
```

### Enqueue a test ZIP job

Upload test objects to R2, start the worker, then:

```bash
./scripts/test-enqueue.sh
```

### Project structure

```
moretransfer-worker/
├── container/
│   ├── Dockerfile           # ZipContainerDO image
│   └── main.go              # ZIP64 STORE chunk writer (Go)
├── src/
│   ├── index.ts             # fetch / queue / scheduled handlers
│   ├── lib/
│   │   └── types/types.ts
│   └── modules/
│       ├── job-manager-do.ts    # ZIP coordinator
│       ├── zip-container.ts     # Container DO + R2 outbound proxy
│       ├── semaphore-do.ts        # Global ZIP concurrency
│       ├── zip-processor.ts
│       ├── stream-ingestor.ts     # Cloudflare Stream copy
│       ├── notification-processor.ts
│       ├── cron.ts
│       ├── job-manifest.ts
│       └── web-api-service.ts
├── scripts/
├── package.json
├── wrangler.toml
└── README.md
```

## Dependencies

- **@cloudflare/containers**: Cloudflare Containers SDK for `ZipContainerDO`
- **@cloudflare/workers-types**: TypeScript types for Workers
- **wrangler**: Cloudflare Workers CLI

## Limitations

- **STORE only** — no deflate, encryption, or symlinks in the ZIP writer
- **MAX_FILES** / **MAX_ZIP_BYTES** enforced at manifest creation
- Large jobs depend on R2 throughput, container availability, and Worker CPU limits (`cpu_ms` in `wrangler.toml`)
- Stream ingest requires Cloudflare Stream API credentials and respects Stream minimum retention (30 days) for scheduled deletion

## Validating ZIP output

After generating a ZIP, download it and verify:

```bash
unzip -t output.zip
ditto -x -k output.zip test-output
7zz t output.zip
```

Suggested cases: single file under 4 GiB; single file over 4 GiB; many small files; deep paths / Unicode names; empty files when `includeEmpty` is true.

## Error handling

- **Main queue**: Infrastructure failures on ZIP ticks retry with delay; business backoff is handled inside `JobManagerDO` via `nextActionAtMs` and alarms
- **Notification queue**: 5xx / parse errors retry the batch; 4xx acks; per-job `retry` status retries individual messages
- **Stream ingest**: Exponential backoff on failure
- **Dead letter queues**: Permanently failed messages (see `wrangler.toml`)
- **ZIP cleanup**: Terminal jobs purge DO state after `BUNDLE_CLEANUP_TTL_DAYS`; failed jobs best-effort abort multipart uploads

## License

ISC

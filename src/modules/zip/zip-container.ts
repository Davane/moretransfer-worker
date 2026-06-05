import { Container } from "@cloudflare/containers";
import { Env } from "../lib/types/types";

export { ContainerProxy } from "@cloudflare/containers";

function json(obj: unknown, status = 200, headers: Record<string, string> = {}) {
  return new Response(JSON.stringify(obj), {
    status,
    headers: { "content-type": "application/json; charset=utf-8", ...headers },
  });
}

export class ZipContainerDO extends Container {
  defaultPort = 8080;
  enableInternet = false;

  // Reduce churn between ticks/chunks. Alarms 
  // still stop idle containers eventually.
  sleepAfter = "2m";

  // Allow only our virtual hosts. These are 
  // evaluated before outbound handlers.
  allowedHosts = ["source.r2", "output.r2", "job.do"];
}

ZipContainerDO.outboundByHost = {
  "source.r2": async (request: Request, env: Env) => {
    const url = new URL(request.url);
    const key = decodeURIComponent(url.pathname.replace(/^\/object\//, "").replace(/^\/+/, ""));

    if (request.method === "GET") {
      const obj = await env.SOURCE_BUCKET.get(key);
      if (!obj) {
        return new Response("not found", { status: 404 });
      }
      const headers = new Headers();
      headers.set("content-type", obj.httpMetadata?.contentType ?? "application/octet-stream");
      headers.set("content-length", String(obj.size));
      headers.set("etag", obj.httpEtag ?? obj.etag);

      return new Response(obj.body, { status: 200, headers });
    }

    if (request.method === "HEAD") {
      const obj = await env.SOURCE_BUCKET.head(key);
      if (!obj) {
        return new Response(null, { status: 404 });
      }

      const headers = new Headers();
      headers.set("content-length", String(obj.size));
      headers.set("etag", obj.httpEtag ?? obj.etag);

      return new Response(null, { status: 200, headers });
    }

    return new Response("method not allowed", { status: 405 });
  },

  "output.r2": async (request: Request, env: Env) => {
    const url = new URL(request.url);

    // GET /object/<key> (used for manifest fetch)
    if (request.method === "GET" && url.pathname.startsWith("/object/")) {
      const key = decodeURIComponent(url.pathname.replace(/^\/object\//, "").replace(/^\/+/, ""));
      const obj = await env.OUTPUT_BUCKET.get(key);
      if (!obj) {
        return new Response("not found", { status: 404 });
      }

      const headers = new Headers();
      headers.set("content-type", obj.httpMetadata?.contentType ?? "application/octet-stream");
      headers.set("content-length", String(obj.size));
      headers.set("etag", obj.httpEtag ?? obj.etag);

      return new Response(obj.body, { status: 200, headers });
    }

    // POST /uploadPart?key=...&uploadId=...&partNumber=...
    if (request.method === "POST" && url.pathname === "/uploadPart") {
      const key = url.searchParams.get("key");
      const uploadId = url.searchParams.get("uploadId");
      const partNumber = Number(url.searchParams.get("partNumber"));

      if (!key || !uploadId || !Number.isFinite(partNumber) || partNumber <= 0) {
        return json({ error: "missing key/uploadId/partNumber" }, 400);
      }

      // IMPORTANT: stream the request body to avoid buffering large (e.g. 128MiB) parts in memory.
      const body = request.body;
      if (!body) {
        return json({ error: "missing request body" }, 400);
      }

      const contentLength = request.headers.get("content-length");
      const contentLengthNum = contentLength ? Number(contentLength) : Number.NaN;
      if (!Number.isFinite(contentLengthNum) || contentLengthNum <= 0) {
        return json({ error: "missing or invalid content-length" }, 411);
      }

      // R2 multipart upload requires a readable stream with a known length.
      // FixedLengthStream tags the readable side with a known length.
      const fls = new FixedLengthStream(contentLengthNum);
      const pump = body.pipeTo(fls.writable);

      const destinationBucket = env.OUTPUT_BUCKET.resumeMultipartUpload(key, uploadId);
      const uploaded = await destinationBucket.uploadPart(partNumber, fls.readable);
      await pump;

      return new Response(null, {
        status: 200,
        headers: {
          "x-etag": uploaded.etag,
          "x-size-bytes": String(contentLengthNum),
        },
      });
    }

    // POST /complete?key=...&uploadId=...
    if (request.method === "POST" && url.pathname === "/complete") {
      const key = url.searchParams.get("key");
      const uploadId = url.searchParams.get("uploadId");

      if (!key || !uploadId) {
        return json({ error: "missing key/uploadId" }, 400);
      }

      const parts = (await request.json()) as Array<{ partNumber: number; etag: string }>;
      const up = env.OUTPUT_BUCKET.resumeMultipartUpload(key, uploadId);
      await up.complete(parts);

      return json({ ok: true });
    }

    // POST /abort?key=...&uploadId=...
    if (request.method === "POST" && url.pathname === "/abort") {
      const key = url.searchParams.get("key");
      const uploadId = url.searchParams.get("uploadId");

      if (!key || !uploadId) {
        return json({ error: "missing key/uploadId" }, 400);
      }

      const up = env.OUTPUT_BUCKET.resumeMultipartUpload(key, uploadId);
      await up.abort();

      return json({ ok: true });
    }

    return new Response("not found", { status: 404 });
  },

  "job.do": async (request: Request, env: Env) => {
    const url = new URL(request.url);
    const jobId = url.searchParams.get("jobId");

    if (!jobId) {
      return json({ error: "missing jobId" }, 400);
    }

    const id = env.JobManager.idFromName(jobId);
    const stub = env.JobManager.get(id);

    // Forward to JobManagerDO /entries endpoints
    if (url.pathname === "/entries") {
      const forwardUrl = new URL(request.url);
      forwardUrl.host = "jobmanager";
      forwardUrl.protocol = "https:";

      return stub.fetch(forwardUrl.toString(), request);
    }

    return json({ error: "not found" }, 404);
  },
} satisfies Record<string, (request: Request, env: Env, ctx: any) => Promise<Response>>;

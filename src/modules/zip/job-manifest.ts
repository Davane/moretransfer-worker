import { Env, ZipJob, ZipJobManifest } from "../lib/types/types";
import { resolveNameInZip } from "../lib/utils";

export const DEFAULT_MANIFEST_PREFIX = "manifests";

export function toBool(v: unknown, defaultValue: boolean): boolean {
  if (typeof v === "boolean") return v;
  if (typeof v !== "string") return defaultValue;
  if (v.toLowerCase() === "true") return true;
  if (v.toLowerCase() === "false") return false;
  return defaultValue;
}

export function toInt(v: unknown, defaultValue: number): number {
  const n = typeof v === "string" ? Number.parseInt(v, 10) : Number.NaN;
  return Number.isFinite(n) ? n : defaultValue;
}

export function resolveOutputKey(env: Env, job: ZipJob): string {
  const currentObjectPrefix = job.objectPrefix.replace(/^\/+/, "").replace(/\/?$/, "/");
  const defaultZipOutputKey = `${env.ZIP_OUTPUT_PREFIX}/${currentObjectPrefix.replace(
    /\/?$/,
    "",
  )}/${env.ZIP_OUTPUT_FILE_NAME}.zip`;

  return job.zipOutputKey ?? defaultZipOutputKey;
}


export async function writeZipManifest(args: {
  env: Env;
  jobId: string;
  zipJob: ZipJob;
  outputKey: string;
  manifestPrefix?: string;
}): Promise<{ manifestKey: string; manifest: ZipJobManifest }> {
  const { env, jobId, zipJob, outputKey } = args;
  const manifestPrefix = (args.manifestPrefix ?? env.ZIP_MANIFEST_PREFIX ?? DEFAULT_MANIFEST_PREFIX)
    .replace(/^\/+/, "")
    .replace(/\/?$/, "");

  const includeEmpty = zipJob.includeEmpty ?? true;

  const files =
    zipJob.files?.map((f) => ({
      key: f.key,
      nameInZip: resolveNameInZip(f.key, zipJob.objectPrefix, f.relativePath),
    })) ?? [];

  const withSizes: ZipJobManifest["files"] = [];
  for (const f of files) {
    const head = await env.SOURCE_BUCKET.head(f.key);
    if (!head) {
      throw new Error(`Missing source object: ${f.key}`);
    }
    if (!includeEmpty && head.size === 0) {
      continue;
    }
    withSizes.push({ key: f.key, nameInZip: f.nameInZip, size: head.size });
  }

  const manifest: ZipJobManifest = {
    version: "v1",
    jobId,
    transferId: zipJob.transferId,
    outputKey,
    createdAtMs: Date.now(),
    includeEmpty,
    files: withSizes,
  };

  const manifestKey = `${manifestPrefix}/${jobId}.json`;
  await env.OUTPUT_BUCKET.put(manifestKey, JSON.stringify(manifest), {
    httpMetadata: { contentType: "application/json; charset=utf-8" },
    customMetadata: {
      jobId,
      transferId: zipJob.transferId,
      outputKey,
      manifestVersion: manifest.version,
    },
  });

  return { manifestKey, manifest };
}

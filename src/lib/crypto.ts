import { Env } from "./types/types";

/**
 * This file created and verifies HMAC signatures on edge runtime
 */
const MAX_SKEW_MS = 1 * 60 * 1000; // 1 minute

async function importKey(secret: string, keyUsage: string[], encoder?: TextEncoder) {
  const enc = encoder || new TextEncoder();
  return await crypto.subtle.importKey(
    "raw",
    enc.encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    keyUsage // ["sign", "verify"]
  );
}

export async function verifyHmac(req: Request, secret: string) {
  console.log("Verifying Request signature...");

  const sig = req.headers.get("x-signature");
  const ts = req.headers.get("x-timestamp");
  if (!sig || !ts) {
    throw new Error("Missing x-signature or x-timestamp headers");
  }

  const now = Date.now();
  const t = Number.isFinite(+ts) ? +ts : Date.parse(ts);
  if (!Number.isFinite(t) || Math.abs(now - t) > MAX_SKEW_MS) {
    throw new Error(`Timestamp skew check failed: skew=${Math.abs(now - t)}ms, max=${MAX_SKEW_MS}ms`);
  }

  const enc = new TextEncoder();
  const key = await importKey(secret, ["verify"], enc);

  const body = await req.clone().text();
  const message = enc.encode(`${ts}\n${body}`);

  const sigBytes = hexToBytes(sig);
  const isVerified = await crypto.subtle.verify("HMAC", key, sigBytes, message);

  if (!isVerified) {
    throw new Error("HMAC signature mismatch");
  }
}

export async function createHmacSha256Hex(payload: string, secret: string) {
  // Create HMAC SHA256 hash on edge runtime
  const enc = new TextEncoder();
  const data = enc.encode(payload);
  const keyUsage = ["sign"];

  const key = await importKey(secret, keyUsage, enc);
  const sig = await crypto.subtle.sign("HMAC", key, data);

  return [...new Uint8Array(sig)].map((b) => b.toString(16).padStart(2, "0")).join("");
}

// Helpers
function hexToBytes(hex: string): Uint8Array {
  if (hex.length % 2 !== 0) throw new Error("Invalid hex");
  const out = new Uint8Array(hex.length / 2);
  for (let i = 0; i < out.length; i++) {
    out[i] = parseInt(hex.slice(i * 2, i * 2 + 2), 16);
  }
  return out;
}


export async function verifyRequest(req: Request, env: Env): Promise<Response | undefined> {
  if (env.SKIP_REQUEST_VERIFICATION) {
    console.warn("Skipping request verification");
    return undefined;
  }

  try {
    await verifyHmac(req, env.SECRET_KEY);
    return undefined;
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    console.error("HMAC verification failed:", message, error);
    return new Response("Unauthorized", { status: 401 });
  }
}

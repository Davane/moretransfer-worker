import { COMPANY_NAME } from "../lib/constants";
import { createHmacSha256Hex } from "../lib/crypto";
import { TransferUpdateRequest } from "../lib/types";
import { createSafeUploadKey, fetchWithCredentials, runWithRetries, slugify } from "../lib/utils";

const STATUS_RETRY_ATTEMPTS = 7;
const STATUS_RETRY_BASE_DELAY_MS = 2000;

export enum NextApiErrorCode {
  TransferSessionException = "TransferSessionException",
  RegisterFilesException = "RegisterFilesException",
  SignPartException = "SignPartException",
  CompleteFileUploadException = "CompleteFileUploadException",
  CompleteTransferUploadException = "CompleteTransferUploadException",
  DownloadFileException = "DownloadFileException",
}

class WebAPIService {
  private readonly _secret: string;
  private readonly _baseUrl: string;

  constructor(secret: string, baseUrl: string) {
    this._secret = secret;
    this._baseUrl = baseUrl;
  }

  async getHeaderSignature(body: Record<string, any>) {
    if (!this._secret) {
      throw new Error("Missing SECRET_KEY");
    }

    const ts = Date.now().toString();
    const rawJson = JSON.stringify(body);
    const message = `${ts}\n${rawJson}`;

    const sig = await createHmacSha256Hex(message, this._secret);

    return {
      "x-timestamp": ts,
      "x-signature": sig,
    };
  }

  async updateTransferStatus(
    transferId: string,
    request: TransferUpdateRequest,
    options?: { retry?: boolean },
  ) {
    const url = `${this._baseUrl}/api/transfers/${transferId}/compression-status`;
    const maxAttempts = (options?.retry ?? true) ? STATUS_RETRY_ATTEMPTS : 1;

    return runWithRetries(
      async () => {
        console.log(`Updating transfer status for ${transferId}:`, url, JSON.stringify(request));
        const headers = await this.getHeaderSignature(request);
        return fetchWithCredentials<any>(url, {
          method: "PUT",
          body: JSON.stringify(request),
          headers,
        });
      },
      {
        maxAttempts,
        baseDelayMs: STATUS_RETRY_BASE_DELAY_MS,
        onFailure: (error, attempt, attempts) =>
          console.log(
            `Error updating transfer status for ${transferId} (attempt ${attempt}/${attempts}):`,
            error,
          ),
      },
    );
  }

  async sendCleanupExpiredTransfersRequest(body: Record<string, any>) {
    const url = `${this._baseUrl}/api/external/cron/cleanup-expired-transfers`;
    const headers = await this.getHeaderSignature(body);

    return await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json", ...headers },
      body: JSON.stringify(body),
    });
  }

  async sendCleanupAbandonedUploadsRequest(body: Record<string, any>) {
    const url = `${this._baseUrl}/api/external/cron/cleanup-abandoned-uploads`;
    const headers = await this.getHeaderSignature(body);

    return await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json", ...headers },
      body: JSON.stringify(body),
    });
  }

  async sendNotificationScheduleRequest(body: Record<string, unknown>, correlationId: string) {
    const url = `${this._baseUrl}/api/external/notifications/schedule`;
    const headers = await this.getHeaderSignature(body);

    return await fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-correlation-id": correlationId,
        ...headers,
      },
      body: JSON.stringify(body),
    });
  }

  async sendNotificationProcessRequest(body: Record<string, unknown>, correlationId: string) {
    const url = `${this._baseUrl}/api/external/notifications/process`;
    const headers = await this.getHeaderSignature(body);

    return await fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-correlation-id": correlationId,
        ...headers,
      },
      body: JSON.stringify(body),
    });
  }

  /**
   * Get the payload for the file compression job
   */
  getEnqueueCompressionPayload(transfer: { fileName: string; id: string }) {
    const transferId = transfer.id;
    const objectFullPath = createSafeUploadKey(transferId, transfer.fileName ?? "");
    const objectPrefix = objectFullPath.substring(0, objectFullPath.lastIndexOf("/") + 1);

    const date = new Date().toISOString().slice(0, 10);
    const filename = transfer.fileName ?? "bundle";

    const outputKey = createSafeUploadKey(
      transferId,
      `${slugify(COMPANY_NAME)}_${filename}_${date}.zip`,
      "compressed",
    );

    const payload = {
      objectPrefix,
      zipOutputKey: outputKey,
      createdBy: "next-api",
    };

    console.info(`Enqueuing zip for transfer ${transferId}: ${JSON.stringify(payload)}`);

    return payload;
  }
}

export { WebAPIService };

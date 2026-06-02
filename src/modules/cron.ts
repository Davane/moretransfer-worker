import { Env } from "../lib/types/types";
import { WebAPIService } from "./web-api-service";

enum EmailNotificationType {
  TransferExpiryReminder = "transfer-expiry-reminder",
  ReviewCommentDigest = "review-comment-digest",
}

export class CronHandler {
  constructor(private readonly env: Env) {}

  async handleCleanupExpiredTransfersCron(webAPIService: WebAPIService, timestamp: number) {
    const body = { trigger: "scheduled", timestamp };

    return await webAPIService
      .sendCleanupExpiredTransfersRequest(body)
      .then(async (res) => {
        const text = await res.text();
        console.log(`[scheduled] Cleanup response: ${res.status}`, text);
      })
      .catch((err) => {
        console.error("[scheduled] Cleanup request failed:", err);
      });
  }

  async handleCleanupAbandonedUploadsCron(webAPIService: WebAPIService, timestamp: number) {
    const body = { trigger: "scheduled", timestamp };

    return await webAPIService
      .sendCleanupAbandonedUploadsRequest(body)
      .then(async (res) => {
        const text = await res.text();
        console.log(`[scheduled] Abandoned uploads cleanup response: ${res.status}`, text);
      })
      .catch((err) => {
        console.error("[scheduled] Abandoned uploads cleanup request failed:", err);
      });
  }

  async handleExpiryReminderCron(webAPIService: WebAPIService, timestamp: number) {
    const body = {
      type: EmailNotificationType.TransferExpiryReminder,
      trigger: "scheduled",
      timestamp,
    };

    return await webAPIService
      .sendEmailNotificationRequest(body)
      .then(async (res) => {
        const text = await res.text();
        console.log(`[scheduled] Expiry reminder response: ${res.status}`, text);
      })
      .catch((err) => {
        console.error("[scheduled] Expiry reminder request failed:", err);
      });
  }

  async handleReviewCommentDigestCron(webAPIService: WebAPIService, timestamp: number) {
    const body = {
      type: EmailNotificationType.ReviewCommentDigest,
      trigger: "scheduled",
      timestamp,
    };

    return await webAPIService
      .sendEmailNotificationRequest(body)
      .then(async (res) => {
        const text = await res.text();
        console.log(`[scheduled] Review comment digest response: ${res.status}`, text);
      })
      .catch((err) => {
        console.error("[scheduled] Review comment digest request failed:", err);
      });
  }
}

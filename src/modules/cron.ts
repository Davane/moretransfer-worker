import { Env, QueueMessageType } from "../lib/types";
import { WebAPIService } from "./web-api-service";

type ScheduleResponse = {
  jobIds?: string[];
};


const QUEUE_SEND_BATCH_LIMIT = 25; // Max batch limit is 100. 

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

  async handleNotificationScheduleCron(webAPIService: WebAPIService, timestamp: number) {
    const body = { trigger: "scheduled", timestamp };
    const correlationId = crypto.randomUUID();
    try {
      const res = await webAPIService.sendNotificationScheduleRequest(body, correlationId);
      const text = await res.text();

      if (!res.ok) {
        console.error(`[notification-schedule] Notification schedule failed`, {
          text,
          status: res.status,
          correlationId,
        });
        return;
      }

      let payload: ScheduleResponse = {};
      try {
        payload = JSON.parse(text) as ScheduleResponse;
      } catch (parseErr) {
        console.error(
          "[notification-schedule] Notification schedule response parse failed:",
          parseErr,
          {
            text,
            correlationId,
          },
        );
        return;
      }

      const jobIds = Array.isArray(payload.jobIds)
        ? payload.jobIds.filter((id): id is string => typeof id === "string")
        : [];

      console.log(`[notification-schedule] Notification schedule`, {
        jobIds,
        jobIdsLength: jobIds.length,
        status: res.status,
        correlationId,
      });

      for (let offset = 0; offset < jobIds.length; offset += QUEUE_SEND_BATCH_LIMIT) {
        const end = offset + QUEUE_SEND_BATCH_LIMIT;
        const batch = jobIds.slice(offset, end).map((jobId) => ({
          body: {
            type: QueueMessageType.NOTIFICATION_PROCESS,
            data: { jobId, correlationId },
          },
        }));

        await this.env.QUEUE_NOTIFICATIONS.sendBatch(batch);
      }
    } catch (err) {
      console.error(
        "[notification-schedule] Notification schedule request failed:",
        { correlationId },
        err,
      );
    }
  }

  // async handleAccountDeletionCron(webAPIService: WebAPIService, timestamp: number) {
  //   const body = { trigger: "scheduled", timestamp };

  //   try {
  //     const response = await webAPIService.sendAccountDeletionProcessRequest(body);
  //     const text = await response.text();
  //     if (!response.ok) {
  //       console.error("[account-deletion] Processing request failed", {
  //         status: response.status,
  //         text,
  //       });
  //       return;
  //     }

  //     console.log("[account-deletion] Processing response", {
  //       status: response.status,
  //       text,
  //     });
  //   } catch (error) {
  //     console.error("[account-deletion] Processing request failed", error);
  //   }
  // }

  /** Rejections reach waitUntil so HTTP, network, and invalid-response failures are observable. */
  async handle24HourCron(webAPIService: WebAPIService, timestamp: number): Promise<void> {
    const body = { trigger: "scheduled", timestamp };

    try {
      const response = await webAPIService.send24HourCronRequest(body);
      if (!response.ok) {
        throw new Error(`Daily API request failed with status ${response.status}`);
      }
      const summary: unknown = await response.json();
      console.log("[24-hour-cron] Completed", { status: response.status, summary });
    } catch (error) {
      console.error("[24-hour-cron] Processing request failed", error);
      throw error;
    }
  }
}

import type { NotificationProcessMessage } from "../lib/types/types";
import { WebAPIService } from "./web-api-service";

const RETRY_DELAY_SECONDS = 30;
export async function processNotificationMessage(
  msg: Message<NotificationProcessMessage>,
  webAPIService: WebAPIService,
): Promise<void> {
  const { jobId, correlationId } = msg.body.data;

  try {
    const res = await webAPIService.sendNotificationProcessRequest(
      {
        jobIds: [jobId],
      },
      correlationId,
    );
    const text = await res.text();

    if (res.ok) {
      console.log(`[notification-process] completed successfully`, {
        correlationId,
        jobId,
        status: res.status,
        text,
      });
      msg.ack();
      return;
    }

    if (res.status >= 500) {
      console.error(
        `[notification-process] failed. Retrying... in ${RETRY_DELAY_SECONDS} seconds.`,
        {
          correlationId,
          jobId,
          status: res.status,
          text,
        },
      );
      msg.retry({ delaySeconds: RETRY_DELAY_SECONDS });
      return;
    }

    console.error(`[notification-process] rejected (ack, no retry)`, {
      correlationId,
      jobId,
      status: res.status,
      text,
    });
    msg.ack();
  } catch (err) {
    console.error(`[notification-process] request failed`, {
      correlationId,
      jobId,
      error: err,
    });
    msg.retry({ delaySeconds: RETRY_DELAY_SECONDS });
  }
}

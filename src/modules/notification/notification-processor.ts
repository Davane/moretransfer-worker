import type {
  NotificationProcessMessage,
  ProcessBatchResult,
  ProcessJobResult,
} from "../../lib/types";
import { WebAPIService } from "../web-api-service";

const RETRY_DELAY_SECONDS = 60;

const TERMINAL_STATUSES = new Set<ProcessJobResult["status"]>(["sent", "skipped", "failed"]);

function groupByCorrelationId(
  messages: Message<NotificationProcessMessage>[],
): Map<string, Message<NotificationProcessMessage>[]> {
  const groups = new Map<string, Message<NotificationProcessMessage>[]>();
  for (const msg of messages) {
    const { correlationId } = msg.body.data;
    const group = groups.get(correlationId);
    if (group) {
      group.push(msg);
    } else {
      groups.set(correlationId, [msg]);
    }
  }
  return groups;
}

function retryAll(messages: Message<NotificationProcessMessage>[]): void {
  for (const msg of messages) {
    msg.retry({ delaySeconds: RETRY_DELAY_SECONDS });
  }
}

function ackAll(messages: Message<NotificationProcessMessage>[]): void {
  for (const msg of messages) {
    msg.ack();
  }
}

async function processGroup(
  messages: Message<NotificationProcessMessage>[],
  webAPIService: WebAPIService,
): Promise<void> {
  const correlationId = messages[0]?.body?.data?.correlationId;
  const jobIdToMessage = new Map<string, Message<NotificationProcessMessage>>();
  const jobIds: string[] = [];

  for (const msg of messages) {
    const { jobId } = msg.body.data;
    jobIds.push(jobId);
    jobIdToMessage.set(jobId, msg);
  }

  try {
    console.log(`[notification-process] processing group`, {
      correlationId,
      jobIds,
      jobCount: messages.length,
    });
    const res = await webAPIService.sendNotificationProcessRequest({ jobIds }, correlationId);
    const text = await res.text();

    if (res.status >= 500) {
      console.error(`[notification-process] server error, retrying all`, {
        correlationId,
        status: res.status,
        text,
        jobCount: messages.length,
      });
      retryAll(messages);
      return;
    }

    if (!res.ok) {
      console.error(`[notification-process] client error, acking all`, {
        correlationId,
        status: res.status,
        text,
        jobCount: messages.length,
      });
      ackAll(messages);
      return;
    }

    let payload: ProcessBatchResult;
    try {
      payload = JSON.parse(text) as ProcessBatchResult;
    } catch (parseErr) {
      console.error(`[notification-process] response parse failed, retrying all`, {
        correlationId,
        text,
        error: parseErr,
        jobCount: messages.length,
      });
      retryAll(messages);
      return;
    }

    console.log(`[notification-process] batch completed`, {
      correlationId,
      jobIds,
      counts: payload.counts,
      requested: payload.requested,
      batches: payload.batches,
      jobCount: messages.length,
    });

    const handledJobIds = new Set<string>();
    for (const result of payload.results ?? []) {
      const msg = jobIdToMessage.get(result.jobId);
      if (!msg) {
        continue;
      }
      handledJobIds.add(result.jobId);

      if (TERMINAL_STATUSES.has(result.status)) {
        msg.ack();
      } else {
        // Retry the message if it is not 
        // terminal. Meaning is the status "retry".
        msg.retry({ delaySeconds: RETRY_DELAY_SECONDS });
      }
    }

    for (const msg of messages) {
      // check if the job was not handled by the API or the result is missing.
      if (!handledJobIds.has(msg.body.data.jobId)) {
        console.error(`[notification-process] job missing from results, retrying`, {
          correlationId,
          jobId: msg.body.data.jobId,
        });
        msg.retry({ delaySeconds: RETRY_DELAY_SECONDS });
      }
    }
  } catch (err) {
    console.error(`[notification-process] request failed, retrying all`, {
      correlationId,
      error: err,
      jobCount: messages.length,
    });
    retryAll(messages);
  }
}

export async function processNotificationBatch(
  messages: Message<NotificationProcessMessage>[],
  webAPIService: WebAPIService,
): Promise<void> {
  if (messages.length === 0) {
    return;
  }

  const groups = groupByCorrelationId(messages);
  for (const group of groups.values()) {
    await processGroup(group, webAPIService);
  }
}

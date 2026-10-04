import { createWriteStream } from "node:fs";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { pipeline } from "node:stream/promises";
import { Worker, type Job } from "bullmq";
import { Redis } from "ioredis";
import { getConfig } from "./config/env.js";
import { prisma } from "./lib/prisma.js";
import { deleteObject, getObjectStream, putObject } from "./lib/s3.js";
import { describeMediaFailure, generatePeaks, probeAudio } from "./lib/media.js";
import { buildUserExport } from "./lib/export.js";

const config = getConfig();
const redis = new Redis(config.REDIS_URL, { maxRetriesPerRequest: null });
const log = (level: "info" | "error" | "warn", data: Record<string, unknown>, message: string) => {
  const output = JSON.stringify({ timestamp: new Date().toISOString(), level, service: "worker", ...data, message });
  if (level === "error") console.error(output);
  else if (level === "warn") console.warn(output);
  else console.log(output);
};

/**
 * 同一探测任务可能被 BullMQ 重试或在异常恢复时重复投递。
 * 处理前用条件更新原子认领（UPLOADED / PROCESSING -> PROCESSING），
 * 落终态时在与媒体记录绑定的事务级咨询锁内再次条件更新，
 * 保证 READY / FAILED 全程只会落一份结果、练习状态最多推进一次。
 */
async function processMedia(mediaId: string, job?: Job): Promise<void> {
  const media = await prisma.mediaAsset.findUnique({ where: { id: mediaId } });
  if (!media) return;
  if (!["UPLOADED", "PROCESSING"].includes(media.status)) return;

  // 原子认领：同一任务的多次投递只有一个执行者继续；他方已落终态时 count 为 0，直接跳过。
  const claim = await prisma.mediaAsset.updateMany({
    where: { id: mediaId, status: { in: ["UPLOADED", "PROCESSING"] } },
    data: { status: "PROCESSING", failureCode: null, failureMessage: null },
  });
  if (claim.count === 0) return;

  const workDir = await mkdtemp(path.join(tmpdir(), "practice-media-"));
  const extension = path.extname(media.originalName).slice(0, 12);
  const localPath = path.join(workDir, `audio${extension}`);
  try {
    const stream = await getObjectStream(media.objectKey);
    await pipeline(stream, createWriteStream(localPath));
    const [probe, peaks] = await Promise.all([probeAudio(localPath), generatePeaks(localPath)]);
    await prisma.$transaction(async (tx) => {
      await tx.$executeRawUnsafe(
        "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
        `media-probe:${mediaId}`,
      );
      const result = await tx.mediaAsset.updateMany({
        where: { id: mediaId, status: "PROCESSING" },
        data: {
          status: "READY",
          durationMs: probe.durationMs,
          codec: probe.codec,
          sampleRate: probe.sampleRate,
          channels: probe.channels,
          peaks,
          processedAt: new Date(),
          expiresAt: null,
          failureCode: null,
          failureMessage: null,
        },
      });
      if (result.count === 0) return;
      await tx.practiceSession.updateMany({
        where: { id: media.sessionId, userId: media.userId, status: "DRAFT" },
        data: { status: "IN_REVIEW", version: { increment: 1 } },
      });
    });
    log("info", { mediaId, durationMs: Number(probe.durationMs) }, "media probe completed");
  } catch (error) {
    const failure = describeMediaFailure(error);
    const errMessage = error instanceof Error ? error.message : "UNKNOWN_MEDIA_ERROR";
    const attempts = job?.opts.attempts ?? 1;
    const attemptMade = job?.attemptsMade ?? 0;
    const hasRetriesLeft = !failure.permanent && attemptMade + 1 < attempts;

    if (failure.permanent) {
      await prisma.mediaAsset.updateMany({
        where: { id: mediaId, status: "PROCESSING" },
        data: {
          status: "FAILED",
          failureCode: failure.code,
          failureMessage: failure.message,
          processedAt: new Date(),
        },
      });
      // 永久失败（文件内容问题）重试无意义，直接结束，不再抛给 BullMQ 重试。
      log("error", { mediaId, code: failure.code, attempt: attemptMade + 1, attempts }, "media probe failed permanently");
      return;
    }

    if (hasRetriesLeft) {
      // 瞬时故障（S3/网络/ffprobe 超时等）：交还 UPLOADED，抛出异常让 BullMQ 按退避策略自动重试。
      await prisma.mediaAsset.updateMany({
        where: { id: mediaId, status: "PROCESSING" },
        data: { status: "UPLOADED", failureCode: null, failureMessage: null },
      });
      log("warn", { mediaId, err: errMessage, attempt: attemptMade + 1, attempts }, "media probe transient failure, retrying");
    } else {
      await prisma.$transaction(async (tx) => {
        await tx.$executeRawUnsafe("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", `media-probe:${mediaId}`);
        await tx.mediaAsset.updateMany({
          where: { id: mediaId, status: "PROCESSING" },
          data: {
            status: "FAILED",
            failureCode: "MEDIA_PROBE_FAILED",
            failureMessage: "音频解析服务暂时不可用，请稍后重试",
            processedAt: new Date(),
          },
        });
      });
      log("error", { mediaId, err: errMessage, attempt: attemptMade + 1, attempts }, "media probe failed after retries");
    }
    throw error;
  } finally {
    await rm(workDir, { recursive: true, force: true });
  }
}

async function cleanupSession(sessionId: string) {
  const session = await prisma.practiceSession.findUnique({
    where: { id: sessionId },
    include: { mediaAssets: { select: { objectKey: true } } },
  });
  if (!session) return;
  try {
    const keys = new Set(session.mediaAssets.map((media) => media.objectKey));
    for (const objectKey of keys) {
      const references = await prisma.mediaAsset.count({ where: { objectKey, sessionId: { not: sessionId } } });
      if (references === 0) await deleteObject(objectKey);
    }
    await prisma.practiceSession.delete({ where: { id: sessionId } });
    log("info", { sessionId }, "session cleanup completed");
  } catch (error) {
    await prisma.practiceSession.updateMany({ where: { id: sessionId }, data: { status: "DELETE_FAILED" } });
    throw error;
  }
}

async function exportData(exportId: string) {
  const task = await prisma.dataExport.findUnique({ where: { id: exportId } });
  if (!task || !task.objectKey) return;
  await prisma.dataExport.update({ where: { id: exportId }, data: { status: "PROCESSING" } });
  try {
    const output = await buildUserExport(task.userId, task.format);
    await putObject(task.objectKey, output.body, output.contentType);
    await prisma.dataExport.update({ where: { id: exportId }, data: { status: "READY", failure: null } });
  } catch (error) {
    await prisma.dataExport.update({
      where: { id: exportId },
      data: { status: "FAILED", failure: error instanceof Error ? error.message.slice(0, 500) : "EXPORT_FAILED" },
    });
    throw error;
  }
}

async function scanOverdueGoals() {
  const startOfToday = new Date();
  startOfToday.setUTCHours(0, 0, 0, 0);
  const result = await prisma.goal.updateMany({
    where: {
      dueDate: { lt: startOfToday },
      status: { in: ["OPEN", "IN_PROGRESS"] },
    },
    data: { status: "MISSED" },
  });
  if (result.count > 0) log("info", { count: result.count }, "overdue goals marked missed");
}

const worker = new Worker(
  "media-processing",
  async (job) => {
    if (job.name === "probe-media") return processMedia(String(job.data.mediaId), job);
    if (job.name === "cleanup-session") return cleanupSession(String(job.data.sessionId));
    if (job.name === "export-data") return exportData(String(job.data.exportId));
    throw new Error(`Unknown job: ${job.name}`);
  },
  { connection: redis, concurrency: config.WORKER_CONCURRENCY },
);

worker.on("failed", (job, error) => log("error", { jobId: job?.id, jobName: job?.name, err: error.message }, "job failed"));
worker.on("error", (error) => log("error", { err: error.message }, "worker error"));

const heartbeat = setInterval(async () => {
  await redis.set("worker:heartbeat", new Date().toISOString(), "EX", 30);
}, 10_000);
await redis.set("worker:heartbeat", new Date().toISOString(), "EX", 30);
await scanOverdueGoals().catch((error) => log("error", { err: error instanceof Error ? error.message : String(error) }, "overdue scan failed"));
const overdueInterval = setInterval(() => {
  void scanOverdueGoals().catch((error) => log("error", { err: error instanceof Error ? error.message : String(error) }, "overdue scan failed"));
}, 24 * 60 * 60_000);

async function shutdown(signal: string) {
  log("info", { signal }, "shutting down worker");
  clearInterval(heartbeat);
  clearInterval(overdueInterval);
  await worker.close();
  await redis.quit();
  await prisma.$disconnect();
  process.exit(0);
}

process.on("SIGTERM", () => void shutdown("SIGTERM"));
process.on("SIGINT", () => void shutdown("SIGINT"));
log("info", {}, "worker started");

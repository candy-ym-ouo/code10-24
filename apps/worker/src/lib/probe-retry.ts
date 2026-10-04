import { isPermanentProbeError, type PermanentProbeError } from "./media.js";

export type FailureCode = "NO_AUDIO_STREAM" | "INVALID_DURATION" | "MEDIA_PROBE_FAILED";

export interface ProbeFailure {
  code: FailureCode;
  message: string;
  permanent: boolean;
}

const PERMANENT_FAILURE_MESSAGES: Record<"NO_AUDIO_STREAM" | "INVALID_DURATION", string> = {
  NO_AUDIO_STREAM: "文件中没有可用的音轨",
  INVALID_DURATION: "音频时长异常，请替换文件后重试",
};

/**
 * 判定一次探测失败的性质：
 * - 永久失败（无音轨、时长非法）：重试也不会改变结果，应立即终结。
 * - 瞬时失败（ffprobe/ffmpeg 退出、超时、S3/数据库抖动等）：应由队列退避重试。
 */
export function classifyProbeError(error: unknown): ProbeFailure {
  if (isPermanentProbeError(error)) {
    return {
      code: error.failureCode,
      message: PERMANENT_FAILURE_MESSAGES[error.failureCode],
      permanent: true,
    };
  }
  return {
    code: "MEDIA_PROBE_FAILED",
    message: "音频处理遇到临时问题，系统将自动重试",
    permanent: false,
  };
}

export function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : "UNKNOWN_MEDIA_ERROR";
}

export type { PermanentProbeError };

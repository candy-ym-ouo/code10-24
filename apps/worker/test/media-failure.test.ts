import { describe, expect, it } from "vitest";
import { MediaContentError, describeMediaFailure } from "../src/lib/media.js";

describe("describeMediaFailure", () => {
  it("classifies missing audio streams as permanent failures", () => {
    const failure = describeMediaFailure(new MediaContentError("NO_AUDIO_STREAM"));
    expect(failure).toEqual({ permanent: true, code: "NO_AUDIO_STREAM", message: "文件中没有可用的音轨" });
  });

  it("classifies invalid durations as permanent failures", () => {
    const failure = describeMediaFailure(new MediaContentError("INVALID_DURATION"));
    expect(failure.permanent).toBe(true);
    expect(failure.code).toBe("INVALID_DURATION");
    expect(failure.message).toContain("时长");
  });

  it("classifies empty decoder output as permanent failures", () => {
    const failure = describeMediaFailure(new MediaContentError("EMPTY_PCM"));
    expect(failure).toMatchObject({ permanent: true, code: "EMPTY_PCM" });
  });

  it("treats ordinary errors (network/timeout/process crashes) as transient", () => {
    for (const error of [
      new Error("connect ETIMEDOUT 10.0.0.1:9000"),
      new Error("ffprobe exited with null"),
      new Error("Command timed out"),
      "unexpected",
    ]) {
      expect(describeMediaFailure(error)).toEqual({ permanent: false, code: null, message: null });
    }
  });
});

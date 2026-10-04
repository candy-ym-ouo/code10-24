import { describe, expect, it } from "vitest";
import { PermanentProbeError } from "../src/lib/media.js";
import { classifyProbeError, errorMessage } from "../src/lib/probe-retry.js";

describe("classifyProbeError", () => {
  it("treats a missing audio stream as permanent failure", () => {
    const failure = classifyProbeError(new PermanentProbeError("NO_AUDIO_STREAM"));
    expect(failure.permanent).toBe(true);
    expect(failure.code).toBe("NO_AUDIO_STREAM");
  });

  it("treats an invalid duration as permanent failure", () => {
    const failure = classifyProbeError(new PermanentProbeError("INVALID_DURATION"));
    expect(failure.permanent).toBe(true);
    expect(failure.code).toBe("INVALID_DURATION");
  });

  it("treats ffprobe process crashes as transient", () => {
    const failure = classifyProbeError(new Error("Command failed: ffprobe ... exited with 1"));
    expect(failure.permanent).toBe(false);
    expect(failure.code).toBe("MEDIA_PROBE_FAILED");
  });

  it("treats timeouts and empty PCM output as transient", () => {
    expect(classifyProbeError(new Error("EMPTY_PCM")).permanent).toBe(false);
    expect(classifyProbeError({}).permanent).toBe(false);
  });
});

describe("errorMessage", () => {
  it("reads the message of Error instances", () => {
    expect(errorMessage(new Error("boom"))).toBe("boom");
  });

  it("falls back for non-Error throws", () => {
    expect(errorMessage("string")).toBe("UNKNOWN_MEDIA_ERROR");
    expect(errorMessage(undefined)).toBe("UNKNOWN_MEDIA_ERROR");
  });
});

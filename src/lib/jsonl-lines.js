"use strict";

/**
 * Yield physical JSONL records. Unlike node:readline, this intentionally treats
 * only LF as a record separator so valid U+2028/U+2029 characters inside JSON
 * strings remain part of the record. Reading raw bytes keeps byte budgets exact
 * and lets malformed UTF-8 fail closed.
 *
 * `maxPhysicalBytes` caps one physical record, not the stream: a transcript may
 * hold any number of small records, while a single oversized one is either
 * rejected (`oversized: "throw"`, the default) or skipped without ever being
 * buffered or decoded (`oversized: "skip"`). A skipped record still reports its
 * exact `physicalBytes` so callers can resume at the following boundary.
 */
class PhysicalJsonlLimitError extends Error {
  constructor(maxPhysicalBytes) {
    super(`physical JSONL byte limit exceeded: ${maxPhysicalBytes}`);
    this.name = "PhysicalJsonlLimitError";
    this.code = "PHYSICAL_JSONL_LIMIT_EXCEEDED";
  }
}

const fatalUtf8Decoder = new TextDecoder("utf-8", { fatal: true });

function decodePhysicalLine(buffer, { stripCr = false } = {}) {
  const content = stripCr && buffer.length > 0 && buffer[buffer.length - 1] === 0x0d
    ? buffer.subarray(0, buffer.length - 1)
    : buffer;
  return fatalUtf8Decoder.decode(content);
}

async function* physicalJsonlRecords(input, {
  invalidUtf8 = "throw",
  maxPhysicalBytes = Infinity,
  oversized = "throw",
} = {}) {
  if (invalidUtf8 !== "throw" && invalidUtf8 !== "record") {
    throw new TypeError(`unsupported invalidUtf8 policy: ${invalidUtf8}`);
  }
  if (oversized !== "throw" && oversized !== "skip") {
    throw new TypeError(`unsupported oversized policy: ${oversized}`);
  }
  const byteLimit = Number.isFinite(maxPhysicalBytes)
    ? Math.max(0, Number(maxPhysicalBytes))
    : Infinity;
  const fragments = [];
  let fragmentsBytes = 0;
  // Once a record exceeds the limit its bytes are still counted, but no longer
  // buffered, so skipping one costs O(1) memory instead of its own size.
  let oversizedRecord = false;

  function resetRecord() {
    fragments.length = 0;
    fragmentsBytes = 0;
    oversizedRecord = false;
  }

  for await (const value of input) {
    if (!Buffer.isBuffer(value) && !(value instanceof Uint8Array)) {
      throw new TypeError("physicalJsonlRecords input must emit bytes");
    }
    const chunk = Buffer.isBuffer(value)
      ? value
      : Buffer.from(value.buffer, value.byteOffset, value.byteLength);

    let start = 0;
    let newline = chunk.indexOf(0x0a);
    while (newline !== -1) {
      const fragment = chunk.subarray(start, newline);
      const physicalBytes = fragmentsBytes + fragment.length + 1;
      if (oversizedRecord || physicalBytes > byteLimit) {
        if (oversized === "throw") throw new PhysicalJsonlLimitError(byteLimit);
        resetRecord();
        yield { line: null, utf8Valid: false, oversized: true, physicalBytes, terminated: true };
        start = newline + 1;
        newline = chunk.indexOf(0x0a, start);
        continue;
      }
      const lineBuffer = fragments.length === 0
        ? fragment
        : Buffer.concat([...fragments, fragment], fragmentsBytes + fragment.length);
      resetRecord();
      start = newline + 1;
      newline = chunk.indexOf(0x0a, start);
      let line;
      try {
        line = decodePhysicalLine(lineBuffer, { stripCr: true });
      } catch (error) {
        if (invalidUtf8 === "throw") throw error;
        yield { line: null, utf8Valid: false, physicalBytes, terminated: true };
        continue;
      }
      yield { line, utf8Valid: true, physicalBytes, terminated: true };
    }

    if (start < chunk.length) {
      const fragment = chunk.subarray(start);
      if (oversizedRecord) {
        fragmentsBytes += fragment.length;
      } else if (fragmentsBytes + fragment.length > byteLimit) {
        if (oversized === "throw") throw new PhysicalJsonlLimitError(byteLimit);
        oversizedRecord = true;
        fragments.length = 0;
        fragmentsBytes += fragment.length;
      } else {
        fragments.push(Buffer.from(fragment));
        fragmentsBytes += fragment.length;
      }
    }
  }

  if (fragmentsBytes > 0) {
    if (oversizedRecord) {
      // The terminator is still missing, so its length (and therefore the next
      // record's start) is unknown; report the span read so far and let the
      // caller decide whether to stop or retry once the record is completed.
      yield { line: null, utf8Valid: false, oversized: true, physicalBytes: fragmentsBytes, terminated: false };
      return;
    }
    const lineBuffer = fragments.length === 1
      ? fragments[0]
      : Buffer.concat(fragments, fragmentsBytes);
    let line = null;
    let utf8Valid = true;
    try {
      line = decodePhysicalLine(lineBuffer);
    } catch (error) {
      if (invalidUtf8 === "throw") throw error;
      utf8Valid = false;
    }
    yield { line, utf8Valid, physicalBytes: fragmentsBytes, terminated: false };
  }
}

module.exports = {
  PhysicalJsonlLimitError,
  physicalJsonlRecords,
};

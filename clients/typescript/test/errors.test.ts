import assert from "node:assert/strict";
import { test } from "node:test";
import {
  fromStatus,
  PolicyError,
  UnsupportedFormatError,
} from "../src/errors.ts";
import { Status } from "../src/protocol.ts";

test("fromStatus maps PolicyRejected and UnsupportedFormat", () => {
  const policy = fromStatus(Status.PolicyRejected, "Put", Buffer.from("quota"));
  assert.ok(policy instanceof PolicyError);
  assert.equal(policy.status, Status.PolicyRejected);

  const fmt = fromStatus(Status.UnsupportedFormat, "Open", Buffer.alloc(0));
  assert.ok(fmt instanceof UnsupportedFormatError);
  assert.equal(fmt.status, Status.UnsupportedFormat);
});

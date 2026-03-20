import { describe, expect, it } from "vitest";

import { serializeCsv } from "./browserExport";

describe("browserExport", () => {
  it("serializes csv values with quotes and escaped commas", () => {
    const csv = serializeCsv(["Name", "Amount"], [["ACME, Inc.", 42], ['He said "hi"', null]]);

    expect(csv).toBe(
      '"Name","Amount"\n"ACME, Inc.","42"\n"He said ""hi""",""',
    );
  });
});

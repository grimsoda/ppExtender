import { describe, it, expect } from "vitest";
import {
  MOD_ACRONYMS,
  ACRONYM_BITS,
  decodeMods,
  encodeMods,
  normalizeMods,
  getSpeedMod,
  modsToKey,
} from "../src/mods";

describe("decodeMods", () => {
  it("should return empty array for 0", () => {
    expect(decodeMods(0)).toEqual([]);
  });

  it("should decode single mods correctly", () => {
    expect(decodeMods(1)).toEqual(["NF"]);
    expect(decodeMods(2)).toEqual(["EZ"]);
    expect(decodeMods(4)).toEqual(["TD"]);
    expect(decodeMods(8)).toEqual(["HD"]);
    expect(decodeMods(16)).toEqual(["HR"]);
    expect(decodeMods(32)).toEqual(["SD"]);
    expect(decodeMods(64)).toEqual(["DT"]);
    expect(decodeMods(128)).toEqual(["RX"]);
    expect(decodeMods(256)).toEqual(["HT"]);
    expect(decodeMods(512)).toEqual(["NC"]);
    expect(decodeMods(1024)).toEqual(["FL"]);
  });

  it("should decode combinations correctly", () => {
    // DT + HD = 64 + 8 = 72
    expect(new Set(decodeMods(72))).toEqual(new Set(["DT", "HD"]));

    // NF + HR = 1 + 16 = 17
    expect(new Set(decodeMods(17))).toEqual(new Set(["NF", "HR"]));

    // All speed mods: DT + HT = 64 + 256 = 320
    expect(new Set(decodeMods(320))).toEqual(new Set(["DT", "HT"]));
  });

  it("should handle NC correctly", () => {
    expect(decodeMods(512)).toEqual(["NC"]);
    // NC + HD = 512 + 8 = 520
    expect(new Set(decodeMods(520))).toEqual(new Set(["NC", "HD"]));
  });

  it("should skip unknown bits gracefully", () => {
    // Unknown bit 2048 + known HD (8) = 2056
    expect(decodeMods(2056)).toEqual(["HD"]);
  });
});

describe("encodeMods", () => {
  it("should return 0 for empty array", () => {
    expect(encodeMods([])).toBe(0);
  });

  it("should encode single mods correctly", () => {
    expect(encodeMods(["NF"])).toBe(1);
    expect(encodeMods(["EZ"])).toBe(2);
    expect(encodeMods(["TD"])).toBe(4);
    expect(encodeMods(["HD"])).toBe(8);
    expect(encodeMods(["HR"])).toBe(16);
    expect(encodeMods(["SD"])).toBe(32);
    expect(encodeMods(["DT"])).toBe(64);
    expect(encodeMods(["RX"])).toBe(128);
    expect(encodeMods(["HT"])).toBe(256);
    expect(encodeMods(["NC"])).toBe(512);
    expect(encodeMods(["FL"])).toBe(1024);
  });

  it("should encode combinations correctly", () => {
    // DT + HD = 64 + 8 = 72
    expect(encodeMods(["DT", "HD"])).toBe(72);

    // NF + HR = 1 + 16 = 17
    expect(encodeMods(["NF", "HR"])).toBe(17);
  });

  it("should be order independent", () => {
    expect(encodeMods(["HD", "DT"])).toBe(encodeMods(["DT", "HD"]));
    expect(encodeMods(["NF", "EZ", "HD"])).toBe(encodeMods(["HD", "NF", "EZ"]));
  });

  it("should skip unknown mods gracefully", () => {
    expect(encodeMods(["UNKNOWN", "HD"])).toBe(8);
  });
});

describe("normalizeMods", () => {
  it("should return empty array for empty input", () => {
    expect(normalizeMods([])).toEqual([]);
  });

  it("should not change mods that need no normalization", () => {
    const result = normalizeMods(["DT", "HD"]);
    expect(new Set(result)).toEqual(new Set(["DT", "HD"]));
  });

  it("should convert NC to DT", () => {
    const result = normalizeMods(["NC", "HD"]);
    expect(new Set(result)).toEqual(new Set(["DT", "HD"]));

    // Just NC should become just DT
    expect(normalizeMods(["NC"])).toEqual(["DT"]);
  });

  it("should preserve other mods", () => {
    const result = normalizeMods(["NC", "HR", "HD", "EZ"]);
    expect(new Set(result)).toEqual(new Set(["DT", "HR", "HD", "EZ"]));
  });
});

describe("getSpeedMod", () => {
  it("should return null for no speed mods", () => {
    expect(getSpeedMod([])).toBeNull();
    expect(getSpeedMod(["HD"])).toBeNull();
    expect(getSpeedMod(["HR", "FL"])).toBeNull();
  });

  it("should return DT for DT mod", () => {
    expect(getSpeedMod(["DT"])).toBe("DT");
    expect(getSpeedMod(["DT", "HD"])).toBe("DT");
    expect(getSpeedMod(["HR", "DT"])).toBe("DT");
  });

  it("should return HT for HT mod", () => {
    expect(getSpeedMod(["HT"])).toBe("HT");
    expect(getSpeedMod(["HT", "EZ"])).toBe("HT");
  });

  it("should return DT for NC (treated as DT)", () => {
    expect(getSpeedMod(["NC"])).toBe("DT");
    expect(getSpeedMod(["NC", "HD"])).toBe("DT");
  });

  it("should prioritize DT over HT", () => {
    // DT + HT - should return DT
    expect(getSpeedMod(["DT", "HT"])).toBe("DT");
  });
});

describe("modsToKey", () => {
  it("should return empty string for empty array", () => {
    expect(modsToKey([])).toBe("");
  });

  it("should handle single mod", () => {
    expect(modsToKey(["HD"])).toBe("HD");
    expect(modsToKey(["DT"])).toBe("DT");
  });

  it("should sort multiple mods alphabetically", () => {
    expect(modsToKey(["HD", "DT"])).toBe("DT,HD");
    expect(modsToKey(["HR", "EZ", "NF"])).toBe("EZ,HR,NF");
  });

  it("should sort unsorted input", () => {
    expect(modsToKey(["HR", "DT", "HD", "EZ"])).toBe("DT,EZ,HD,HR");
  });

  it("should include NC (not normalized)", () => {
    expect(modsToKey(["NC", "HD"])).toBe("HD,NC");
  });
});

describe("roundtrip", () => {
  it("should encode and decode correctly", () => {
    const testCases = [
      ["NF"],
      ["DT"],
      ["DT", "HD"],
      ["HR", "DT", "HD"],
      ["EZ", "NF"],
    ];

    for (const mods of testCases) {
      const encoded = encodeMods(mods);
      const decoded = decodeMods(encoded);
      expect(new Set(decoded)).toEqual(new Set(mods));
    }
  });
});

describe("MOD_ACRONYMS", () => {
  it("should have expected values", () => {
    expect(MOD_ACRONYMS[1]).toBe("NF");
    expect(MOD_ACRONYMS[2]).toBe("EZ");
    expect(MOD_ACRONYMS[4]).toBe("TD");
    expect(MOD_ACRONYMS[8]).toBe("HD");
    expect(MOD_ACRONYMS[16]).toBe("HR");
    expect(MOD_ACRONYMS[32]).toBe("SD");
    expect(MOD_ACRONYMS[64]).toBe("DT");
    expect(MOD_ACRONYMS[128]).toBe("RX");
    expect(MOD_ACRONYMS[256]).toBe("HT");
    expect(MOD_ACRONYMS[512]).toBe("NC");
    expect(MOD_ACRONYMS[1024]).toBe("FL");
  });
});

describe("ACRONYM_BITS", () => {
  it("should be reverse of MOD_ACRONYMS", () => {
    for (const [bit, acronym] of Object.entries(MOD_ACRONYMS)) {
      expect(ACRONYM_BITS[acronym]).toBe(parseInt(bit, 10));
    }
  });
});

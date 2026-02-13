/**
 * Mod encoding utilities for osu! mods.
 *
 * This module provides functions to encode/decode osu! mod combinations
 * using bitset representation. Mods are represented as bit flags where
 * each mod has a unique bit value.
 *
 * Mod Bit Values (osu! API):
 *   1: NF (No Fail)
 *   2: EZ (Easy)
 *   4: TD (Touch Device)
 *   8: HD (Hidden)
 *   16: HR (Hard Rock)
 *   32: SD (Sudden Death)
 *   64: DT (Double Time)
 *   128: RX (Relax)
 *   256: HT (Half Time)
 *   512: NC (Nightcore)
 *   1024: FL (Flashlight)
 *
 * Note: NC (512) is typically treated as DT for consistency, but both
 * representations are supported.
 */

/** Mapping of bit value to mod acronym */
export const MOD_ACRONYMS: Record<number, string> = {
  1: "NF", // No Fail
  2: "EZ", // Easy
  4: "TD", // Touch Device
  8: "HD", // Hidden
  16: "HR", // Hard Rock
  32: "SD", // Sudden Death
  64: "DT", // Double Time
  128: "RX", // Relax
  256: "HT", // Half Time
  512: "NC", // Nightcore
  1024: "FL", // Flashlight
};

/** Reverse mapping: acronym to bit value */
export const ACRONYM_BITS: Record<string, number> = {
  NF: 1,
  EZ: 2,
  TD: 4,
  HD: 8,
  HR: 16,
  SD: 32,
  DT: 64,
  RX: 128,
  HT: 256,
  NC: 512,
  FL: 1024,
};

/** Speed mods for categorization */
export const SPEED_MODS: Set<string> = new Set(["DT", "HT", "NC"]);

/**
 * Decode a bitset into a list of mod acronyms.
 *
 * @param bitset - Integer representing the mod combination as bit flags.
 * @returns List of mod acronyms (e.g., ['DT', 'HD']).
 *          Unknown bits are silently skipped.
 *
 * @example
 * decodeMods(72) // Returns ['DT', 'HD'] (DT=64 + HD=8)
 * decodeMods(0)  // Returns []
 */
export function decodeMods(bitset: number): string[] {
  if (bitset === 0) {
    return [];
  }

  const mods: string[] = [];
  for (const [bit, acronym] of Object.entries(MOD_ACRONYMS)) {
    if (bitset & parseInt(bit, 10)) {
      mods.push(acronym);
    }
  }
  return mods;
}

/**
 * Encode a list of mod acronyms into a bitset.
 *
 * @param acronyms - List of mod acronyms (e.g., ['DT', 'HD']).
 * @returns Integer bitset representing the mod combination.
 *          Unknown acronyms are silently skipped.
 *
 * @example
 * encodeMods(['DT', 'HD']) // Returns 72 (64 + 8)
 * encodeMods([])           // Returns 0
 */
export function encodeMods(acronyms: string[]): number {
  let bitset = 0;
  for (const acronym of acronyms) {
    const bit = ACRONYM_BITS[acronym];
    if (bit !== undefined) {
      bitset |= bit;
    }
  }
  return bitset;
}

/**
 * Normalize a list of mod acronyms.
 *
 * Normalization rules:
 * - NC (Nightcore) is converted to DT (Double Time)
 * - Unknown mods are preserved as-is
 * - Duplicates are removed
 *
 * @param acronyms - List of mod acronyms.
 * @returns Normalized list of mod acronyms.
 *
 * @example
 * normalizeMods(['NC', 'HD']) // Returns ['DT', 'HD']
 * normalizeMods(['DT', 'HD']) // Returns ['DT', 'HD']
 */
export function normalizeMods(acronyms: string[]): string[] {
  const normalized: string[] = [];
  const seen = new Set<string>();

  for (let acronym of acronyms) {
    // Normalize NC to DT
    if (acronym === "NC") {
      acronym = "DT";
    }

    if (!seen.has(acronym)) {
      normalized.push(acronym);
      seen.add(acronym);
    }
  }

  return normalized;
}

/**
 * Get the speed mod from a list of mods.
 *
 * Speed mods are: DT (Double Time), HT (Half Time), NC (Nightcore).
 * NC is treated as DT. If both DT and HT are present, DT takes priority.
 *
 * @param acronyms - List of mod acronyms.
 * @returns 'DT' if DT or NC is present, 'HT' if HT is present (and no DT),
 *          or null if no speed mod is present.
 *
 * @example
 * getSpeedMod(['DT', 'HD'])     // Returns 'DT'
 * getSpeedMod(['NC'])           // Returns 'DT' (Nightcore treated as DT)
 * getSpeedMod(['HT', 'EZ'])     // Returns 'HT'
 * getSpeedMod(['HD', 'HR'])     // Returns null
 * getSpeedMod(['DT', 'HT'])     // Returns 'DT' (DT takes priority)
 */
export function getSpeedMod(acronyms: string[]): string | null {
  const hasDt = acronyms.includes("DT") || acronyms.includes("NC");
  const hasHt = acronyms.includes("HT");

  if (hasDt) {
    return "DT";
  } else if (hasHt) {
    return "HT";
  } else {
    return null;
  }
}

/**
 * Convert a list of mod acronyms to a canonical key string.
 *
 * The key is comma-separated and alphabetically sorted for consistency.
 * This ensures that ['HD', 'DT'] and ['DT', 'HD'] produce the same key.
 *
 * @param acronyms - List of mod acronyms.
 * @returns Comma-separated, sorted string of mods. Empty string if no mods.
 *
 * @example
 * modsToKey(['HD', 'DT'])           // Returns 'DT,HD'
 * modsToKey(['NF', 'HR', 'HD'])     // Returns 'HD,HR,NF'
 * modsToKey([])                     // Returns ''
 */
export function modsToKey(acronyms: string[]): string {
  if (acronyms.length === 0) {
    return "";
  }

  const sortedMods = [...acronyms].sort();
  return sortedMods.join(",");
}

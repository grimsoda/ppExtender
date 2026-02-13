/**
 * Shared TypeScript utilities for osu! recommender system
 */

// Re-export shared types and utilities
export type BeatmapId = number;
export type UserId = number;
export type Mods = string;

export interface Beatmap {
  beatmap_id: BeatmapId;
  beatmapset_id?: number;
  title?: string;
  artist?: string;
  version?: string;
  creator?: string;
  difficulty_rating?: number;
  bpm?: number;
  total_length?: number;
  mode?: number;
  status?: number;
}

export interface UserScore {
  user_id: UserId;
  beatmap_id: BeatmapId;
  pp: number;
  mods: Mods;
  accuracy: number;
  score: number;
}

export interface CohortFilters {
  pp_lower?: number;
  pp_upper?: number;
  mods?: string[];
}

export interface Recommendation {
  beatmap_id: BeatmapId;
  title?: string;
  artist?: string;
  version?: string;
  creator?: string;
  difficulty_rating?: number;
  bpm?: number;
  total_length?: number;
  play_count?: number;
  avg_pp?: number;
  avg_accuracy?: number;
  similarity_score?: number;
}

// Utility functions
export function formatMods(mods: string | null | undefined): string {
  if (!mods) return 'NM';
  return mods || 'NM';
}

export function formatPP(pp: number | null | undefined): string {
  if (pp === null || pp === undefined) return 'N/A';
  return `${pp.toFixed(1)}pp`;
}

export function formatAccuracy(acc: number | null | undefined): string {
  if (acc === null || acc === undefined) return 'N/A';
  return `${acc.toFixed(2)}%`;
}

export {
  MOD_ACRONYMS,
  ACRONYM_BITS,
  SPEED_MODS,
  decodeMods,
  encodeMods,
  normalizeMods,
  getSpeedMod,
  modsToKey,
} from './mods';

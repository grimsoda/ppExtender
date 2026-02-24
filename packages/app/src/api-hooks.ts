import { trpc } from './lib/trpc';

export interface HealthResponse {
  status: 'ok';
  database: 'connected' | 'disconnected';
}

export type {
  CohortParams,
  CohortData,
  Recommendation,
} from './api';

export type { Play } from './components/CohortPreview';

export function useHealth(enabled: boolean = true) {
  return trpc.recommender.health.useQuery(undefined, {
    enabled,
    staleTime: 0,
    refetchInterval: enabled ? 30000 : false,
  });
}

export function useGetCohort(beatmap_id: number, options?: { pp_lower?: number; pp_upper?: number; mods?: string[]; top_k?: number }) {
  const mods = options?.mods && options.mods.length > 0 ? options.mods : undefined;
  return trpc.recommender.getCohort.useQuery(
    {
      beatmap_id,
      pp_lower: options?.pp_lower ?? 0,
      pp_upper: options?.pp_upper ?? 10000,
      mods,
      top_k: options?.top_k ?? 200,
    },
    {
      enabled: !!beatmap_id,
      refetchOnWindowFocus: false,
      staleTime: Infinity,
      select: (data) => ({
        size: data.cohort_size,
        ppDistribution: data.pp_distribution,
        accuracyDistribution: data.accuracy_distribution,
        topPlayers: data.top_players,
        plays: data.plays,
        seedPpRange: data.seed_pp_range,
      }),
    }
  );
}

export function useGetBeatmapPlays(beatmap_id: number, options?: { mods?: string[]; top_k?: number }) {
  const mods = options?.mods && options.mods.length > 0 ? options.mods : undefined;
  return trpc.recommender.getBeatmapPlays.useQuery(
    {
      beatmap_id,
      mods,
      top_k: options?.top_k ?? 200,
    },
    {
      enabled: !!beatmap_id,
      refetchOnWindowFocus: false,
      staleTime: Infinity,
      select: (data) => data.all_plays,
    }
  );
}

export function useGetRecommendations(beatmap_id: number, options?: { pp_lower?: number; pp_upper?: number; mods?: string[]; top_k?: number; limit?: number }) {
  const mods = options?.mods && options.mods.length > 0 ? options.mods : undefined;
  return trpc.recommender.getRecommendations.useQuery(
    {
      beatmap_id,
      pp_lower: options?.pp_lower ?? 0,
      pp_upper: options?.pp_upper ?? 10000,
      mods,
      limit: options?.limit ?? 10,
    },
    {
      enabled: !!beatmap_id,
      refetchOnWindowFocus: false,
      staleTime: Infinity,
      select: (data) =>
        (data.recommendations || []).map((rec) => ({
          beatmapId: rec.beatmap_id,
          beatmapsetId: rec.beatmapset_id,
          title: rec.title,
          artist: rec.artist,
          difficulty: rec.version,
          stars: rec.difficulty_rating,
          pp: rec.avg_pp,
          accuracy: rec.avg_accuracy || 95,
          mods: [],
          coverUrl: `https://assets.ppy.sh/beatmaps/${rec.beatmapset_id}/covers/list.jpg`,
          bpm: rec.bpm,
          totalLength: rec.total_length,
        })),
    }
  );
}

export function useGetBeatmaps(beatmap_ids: number[]) {
  return trpc.recommender.getBeatmaps.useQuery(
    {
      beatmap_ids,
    },
    {
      enabled: beatmap_ids.length > 0,
      select: (data) => data.beatmaps,
    }
  );
}

export function useGetUser(user_id: number) {
  return trpc.recommender.getUser.useQuery(
    {
      user_id,
    },
    {
      enabled: !!user_id,
    }
  );
}

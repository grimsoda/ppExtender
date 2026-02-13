import { useQuery } from '@tanstack/react-query';

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

async function fetchApi<T>(endpoint: string, options?: RequestInit): Promise<T> {
  const baseUrl = import.meta.env.VITE_API_URL || 'http://localhost:3000';
  const response = await fetch(`${baseUrl}${endpoint}`, {
    headers: { 'Content-Type': 'application/json' },
    ...options,
  });

  if (!response.ok) {
    const error = await response.json().catch(() => ({ error: 'API request failed' }));
    throw new Error(error.error || `API error: ${response.status} ${response.statusText}`);
  }

  return response.json();
}

export function useHealth(enabled: boolean = true) {
  return useQuery({
    queryKey: ['health'],
    queryFn: () => fetchApi<HealthResponse>('/health'),
    enabled,
    staleTime: 0,
    refetchInterval: enabled ? 30000 : false,
  });
}

export function useGetCohort(beatmap_id: number, options?: { pp_lower?: number; pp_upper?: number; mods?: string[]; top_k?: number }) {
  return useQuery({
    queryKey: ['cohort', beatmap_id, options],
    queryFn: async () => {
      const data = await fetchApi<{
        beatmap_id: number;
        cohort_size: number;
        pp_distribution: { min: number; max: number; mean: number; median: number };
        accuracy_distribution: { min: number; max: number; mean: number; median: number };
        top_players: Array<{ userId: number; username: string; pp: number; accuracy: number }>;
        plays: Array<{
          scoreId: number;
          userId: number;
          pp: number;
          accuracy: number;
          score: number;
          mods: string;
          rank: string;
          maxCombo: number;
          beatmapMaxCombo: number;
          count300: number;
          count100: number;
          count50: number;
          countMiss: number;
          countSliderBreaks: number;
        }>;
        seed_pp_range: { lower: number; upper: number };
      }>('/api/cohort', {
        method: 'POST',
        body: JSON.stringify({ beatmap_id, ...options }),
      });

      return {
        size: data.cohort_size,
        ppDistribution: data.pp_distribution,
        accuracyDistribution: data.accuracy_distribution || {
          min: 0,
          max: 100,
          mean: 50,
          median: 50,
        },
        topPlayers: data.top_players || [],
        plays: data.plays || [],
        seedPpRange: data.seed_pp_range || { lower: 0, upper: 2000 },
      };
    },
    enabled: !!beatmap_id,
  });
}

export function useGetBeatmapPlays(beatmap_id: number, options?: { mods?: string[]; top_k?: number }) {
  return useQuery({
    queryKey: ['beatmap-plays', beatmap_id, options],
    queryFn: async () => {
      const data = await fetchApi<{ all_plays: Array<{
        userId: number;
        pp: number;
        accuracy: number;
        score: number;
        mods: string;
        rank: string;
        maxCombo: number;
        beatmapMaxCombo: number;
        count300: number;
        count100: number;
        count50: number;
        countMiss: number;
        countSliderBreaks: number;
      }> }>('/api/beatmap-plays', {
        method: 'POST',
        body: JSON.stringify({ beatmap_id, ...options }),
      });

      return data.all_plays || [];
    },
    enabled: !!beatmap_id,
  });
}

export function useGetRecommendations(beatmap_id: number, options?: { pp_lower?: number; pp_upper?: number; mods?: string[]; top_k?: number; limit?: number }) {
  return useQuery({
    queryKey: ['recommendations', beatmap_id, options],
    queryFn: async () => {
      const data = await fetchApi<{
        beatmap_id: number;
        total: number;
        recommendations: Array<{
          beatmap_id: number;
          beatmapset_id: number;
          title: string;
          artist: string;
          version: string;
          creator: string;
          difficulty_rating: number;
          bpm: number;
          total_length: number;
          play_count: number;
          avg_pp: number;
          avg_accuracy: number;
          similarity_score: number;
        }>;
      }>('/api/recommend', {
        method: 'POST',
        body: JSON.stringify({ beatmap_id, ...options }),
      });

      return (data.recommendations || []).map((rec) => ({
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
      }));
    },
    enabled: !!beatmap_id,
  });
}

export function useGetBeatmaps(beatmap_ids: number[]) {
  return useQuery({
    queryKey: ['beatmaps', beatmap_ids],
    queryFn: async () => {
      const data = await fetchApi<{
        beatmaps: Array<{
          beatmap_id: number;
          title: string;
          artist: string;
          version: string;
          creator: string;
          difficulty: number;
          bpm: number;
          total_length: number;
          mode: number;
          status: number;
        }>;
      }>('/api/beatmaps', {
        method: 'POST',
        body: JSON.stringify({ beatmap_ids }),
      });

      return data.beatmaps || [];
    },
    enabled: beatmap_ids.length > 0,
  });
}

export function useGetUser(user_id: number) {
  return useQuery({
    queryKey: ['user', user_id],
    queryFn: async () => {
      const data = await fetchApi<{
        user_id: number;
        stats: {
          total_plays: number;
          average_pp: number;
          peak_rank: number | null;
        };
        top_plays: Array<{
          beatmap_id: number;
          title: string;
          artist: string;
          version: string;
          pp: number;
          mods: string;
          accuracy: number;
          score: number;
          playcount: number;
        }>;
      }>(`/api/user/${user_id}`);

      return {
        user_id: data.user_id,
        stats: data.stats,
        top_plays: data.top_plays || [],
      };
    },
    enabled: !!user_id,
  });
}

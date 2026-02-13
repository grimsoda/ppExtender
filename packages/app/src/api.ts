const API_BASE_URL = 'http://localhost:3000';

export interface CohortParams {
  beatmapId: string;
  minPp: number;
  maxPp: number;
  mods: string[];
  topK: number;
}

export interface CohortData {
  size: number;
  ppDistribution: {
    min: number;
    max: number;
    mean: number;
    median: number;
  };
  accuracyDistribution: {
    min: number;
    max: number;
    mean: number;
    median: number;
  };
  topPlayers: Array<{
    userId: number;
    username: string;
    pp: number;
    accuracy: number;
  }>;
  plays: Array<{
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
  seedPpRange: {
    lower: number;
    upper: number;
  };
}

export interface Recommendation {
  beatmapId: number;
  beatmapsetId: number;
  title: string;
  artist: string;
  difficulty: string;
  stars: number;
  pp: number;
  accuracy: number;
  mods: string[];
  coverUrl: string;
  bpm?: number;
  totalLength?: number;
}

export async function fetchCohort(params: CohortParams): Promise<CohortData> {
  const response = await fetch(`${API_BASE_URL}/api/cohort`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      beatmap_id: parseInt(params.beatmapId, 10),
      pp_lower: params.minPp > 0 ? params.minPp : undefined,
      pp_upper: params.maxPp < 1000 ? params.maxPp : undefined,
      mods: params.mods.length > 0 ? params.mods : undefined,
      top_k: params.topK || 200,
    }),
  });

  if (!response.ok) {
    const error = await response.json().catch(() => ({ error: 'Failed to fetch cohort' }));
    throw new Error(error.error || `HTTP error! status: ${response.status}`);
  }

  const data = await response.json();
  
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
}

export async function fetchAllPlays(params: { beatmapId: string; mods: string[]; topK?: number }): Promise<Array<{
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
}>> {
  const response = await fetch(`${API_BASE_URL}/api/beatmap-plays`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      beatmap_id: parseInt(params.beatmapId, 10),
      mods: params.mods.length > 0 ? params.mods : undefined,
      top_k: params.topK || 200,
    }),
  });

  if (!response.ok) {
    const error = await response.json().catch(() => ({ error: 'Failed to fetch all plays' }));
    throw new Error(error.error || `HTTP error! status: ${response.status}`);
  }

  const data = await response.json();
  return data.all_plays || [];
}

export async function fetchRecommendations(params: CohortParams): Promise<Recommendation[]> {
  const response = await fetch(`${API_BASE_URL}/api/recommend`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      beatmap_id: parseInt(params.beatmapId, 10),
      pp_lower: params.minPp > 0 ? params.minPp : undefined,
      pp_upper: params.maxPp < 1000 ? params.maxPp : undefined,
      mods: params.mods.length > 0 ? params.mods : undefined,
      top_k: params.topK || 200,
    }),
  });

  if (!response.ok) {
    const error = await response.json().catch(() => ({ error: 'Failed to fetch recommendations' }));
    throw new Error(error.error || `HTTP error! status: ${response.status}`);
  }

  const data = await response.json();
  
  return (data.recommendations || []).map((rec: {
    beatmap_id: number;
    beatmapset_id: number;
    title: string;
    artist: string;
    version: string;
    difficulty_rating: number;
    avg_pp: number;
    avg_accuracy?: number;
    bpm?: number;
    total_length?: number;
    mods?: string[];
  }) => ({
    beatmapId: rec.beatmap_id,
    beatmapsetId: rec.beatmapset_id,
    title: rec.title,
    artist: rec.artist,
    difficulty: rec.version,
    stars: rec.difficulty_rating,
    pp: rec.avg_pp,
    accuracy: rec.avg_accuracy || 95,
    mods: rec.mods || [],
    coverUrl: `https://assets.ppy.sh/beatmaps/${rec.beatmapset_id}/covers/list.jpg`,
    bpm: rec.bpm,
    totalLength: rec.total_length,
  }));
}

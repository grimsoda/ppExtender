const API_BASE_URL = 'http://localhost:3000';

interface CohortParams {
  beatmapId: string;
  minPp: number;
  maxPp: number;
  mods: string[];
}

interface CohortData {
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
}

interface Recommendation {
  beatmapId: number;
  title: string;
  artist: string;
  difficulty: string;
  stars: number;
  pp: number;
  accuracy: number;
  mods: string[];
  coverUrl: string;
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
  };
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
    }),
  });

  if (!response.ok) {
    const error = await response.json().catch(() => ({ error: 'Failed to fetch recommendations' }));
    throw new Error(error.error || `HTTP error! status: ${response.status}`);
  }

  const data = await response.json();
  
  return (data.recommendations || []).map((rec: {
    beatmap_id: number;
    title: string;
    artist: string;
    difficulty: string;
    stars: number;
    avg_pp: number;
    accuracy?: number;
    mods?: string[];
    cover_url?: string;
  }) => ({
    beatmapId: rec.beatmap_id,
    title: rec.title,
    artist: rec.artist,
    difficulty: rec.difficulty,
    stars: rec.stars,
    pp: rec.avg_pp,
    accuracy: rec.accuracy || 95,
    mods: rec.mods || [],
    coverUrl: rec.cover_url || `https://assets.ppy.sh/beatmaps/${rec.beatmap_id}/covers/cover.jpg`,
  }));
}

import { useState, useEffect } from 'react';
import { useGetCohort, useGetBeatmapPlays, useGetRecommendations } from '../api-hooks';

interface SeedInputProps {
  onSubmitStart?: () => void;
  onLoadingChange?: (isCohortLoading: boolean, isRecommendationsLoading: boolean) => void;
  onCohortData?: (data: {
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
    seedPpRange: {
      lower: number;
      upper: number;
    };
  }) => void;
  onAllPlays?: (plays: Array<{
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
  }>) => void;
  onRecommendations?: (recommendations: Array<{
    beatmapId: number;
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
  }>) => void;
  onError?: (error: string) => void;
}

const AVAILABLE_MODS = ['HD', 'HR', 'DT', 'FL', 'EZ', 'HT', 'NC'];

const MOD_EXCLUSIVE_PAIRS: Record<string, string[]> = {
  'HR': ['EZ'],
  'EZ': ['HR'],
  'DT': ['HT'],
  'HT': ['DT'],
  'NC': ['HT'],
};

export function SeedInput({ onSubmitStart, onLoadingChange, onCohortData, onAllPlays, onRecommendations, onError }: SeedInputProps) {
  const [beatmapId, setBeatmapId] = useState('');
  const [minPp, setMinPp] = useState(0);
  const [maxPp, setMaxPp] = useState(500);
  const [topK, setTopK] = useState(200);
  const [selectedMods, setSelectedMods] = useState<string[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [submittedParams, setSubmittedParams] = useState<{
    beatmapId: number;
    minPp: number;
    maxPp: number;
    mods: string[];
    topK: number;
  } | null>(null);

  // React Query hooks for data fetching
  const cohortQuery = useGetCohort(
    submittedParams?.beatmapId || 0,
    {
      pp_lower: submittedParams?.minPp,
      pp_upper: submittedParams?.maxPp,
      mods: submittedParams?.mods,
      top_k: submittedParams?.topK,
    }
  );

  const beatmapPlaysQuery = useGetBeatmapPlays(
    submittedParams?.beatmapId || 0,
    {
      mods: submittedParams?.mods,
      top_k: submittedParams?.topK,
    }
  );

  const recommendationsQuery = useGetRecommendations(
    submittedParams?.beatmapId || 0,
    {
      pp_lower: submittedParams?.minPp,
      pp_upper: submittedParams?.maxPp,
      mods: submittedParams?.mods,
      top_k: submittedParams?.topK,
      limit: 20,
    }
  );

  useEffect(() => {
    if (cohortQuery.data && onCohortData) {
      onCohortData(cohortQuery.data);
    }
  }, [cohortQuery.data, onCohortData]);

  useEffect(() => {
    if (beatmapPlaysQuery.data && onAllPlays) {
      onAllPlays(beatmapPlaysQuery.data);
    }
  }, [beatmapPlaysQuery.data, onAllPlays]);

  useEffect(() => {
    if (recommendationsQuery.data && onRecommendations) {
      onRecommendations(recommendationsQuery.data);
    }
  }, [recommendationsQuery.data, onRecommendations]);

  useEffect(() => {
    if (cohortQuery.error && onError) {
      onError('Failed to fetch cohort');
    }
  }, [cohortQuery.error, onError]);

  const isLoading = cohortQuery.isLoading || beatmapPlaysQuery.isLoading || recommendationsQuery.isLoading;

  useEffect(() => {
    if (onLoadingChange) {
      onLoadingChange(cohortQuery.isLoading, recommendationsQuery.isLoading);
    }
  }, [cohortQuery.isLoading, recommendationsQuery.isLoading, onLoadingChange]);

  const handleModToggle = (mod: string) => {
    setSelectedMods(prev => {
      if (prev.includes(mod)) {
        return prev.filter(m => m !== mod);
      }
      
      const exclusiveMods = MOD_EXCLUSIVE_PAIRS[mod] || [];
      const filtered = prev.filter(m => !exclusiveMods.includes(m));
      
      if (mod === 'DT' || mod === 'NC') {
        if (!filtered.includes('DT') && !filtered.includes('NC')) {
          return [...filtered, mod];
        }
        return filtered;
      }
      
      return [...filtered, mod];
    });
  };

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    setError(null);

    if (!beatmapId.trim()) {
      setError('Beatmap ID is required');
      return;
    }

    if (!/^\d+$/.test(beatmapId)) {
      setError('Invalid beatmap ID');
      return;
    }

    onSubmitStart?.();

    setSubmittedParams({
      beatmapId: parseInt(beatmapId, 10),
      minPp,
      maxPp,
      mods: selectedMods,
      topK,
    });
  };

  return (
    <div className="bg-white p-6 rounded-lg shadow-md">
      <h2 className="text-xl font-semibold text-gray-800 mb-4">Find Similar Beatmaps</h2>
      
      <form onSubmit={handleSubmit} className="space-y-4">
        <div>
          <label htmlFor="beatmapId" className="block text-sm font-medium text-gray-700 mb-1">
            Beatmap ID
          </label>
          <input
            type="text"
            id="beatmapId"
            value={beatmapId}
            onChange={(e) => setBeatmapId(e.target.value)}
            placeholder="Enter beatmap ID"
            className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
            disabled={isLoading}
          />
        </div>

        <div className="grid grid-cols-2 gap-4">
          <div>
            <label htmlFor="minPp" className="block text-sm font-medium text-gray-700 mb-1">
              Min PP
            </label>
            <input
              type="number"
              id="minPp"
              min="0"
              max="2000"
              value={minPp}
              onChange={(e) => setMinPp(Math.max(0, Number(e.target.value)))}
              className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
              disabled={isLoading}
            />
          </div>

          <div>
            <label htmlFor="maxPp" className="block text-sm font-medium text-gray-700 mb-1">
              Max PP
            </label>
            <input
              type="number"
              id="maxPp"
              min="0"
              max="2000"
              value={maxPp}
              onChange={(e) => setMaxPp(Math.max(0, Number(e.target.value)))}
              className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
              disabled={isLoading}
            />
          </div>
        </div>

        <div>
          <label htmlFor="topK" className="block text-sm font-medium text-gray-700 mb-1">
            Top K per user
          </label>
          <input
            type="number"
            id="topK"
            min="50"
            max="500"
            step="50"
            value={topK}
            onChange={(e) => setTopK(Math.min(Math.max(Number(e.target.value) || 200, 50), 500))}
            className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
            disabled={isLoading}
          />
          <p className="mt-1 text-xs text-gray-500">
            Include players who have this beatmap in their top {topK} plays
          </p>
        </div>

        <div>
          <span className="block text-sm font-medium text-gray-700 mb-2">Mods</span>
          <div className="flex flex-wrap gap-2">
            {AVAILABLE_MODS.map((mod) => (
              <label key={mod} className="inline-flex items-center">
                <input
                  type="checkbox"
                  checked={selectedMods.includes(mod)}
                  onChange={() => handleModToggle(mod)}
                  disabled={isLoading}
                  className="sr-only"
                />
                <span
                  className={`px-3 py-1 rounded-full text-sm font-medium cursor-pointer transition-colors ${
                    selectedMods.includes(mod)
                      ? 'bg-blue-600 text-white'
                      : 'bg-gray-200 text-gray-700 hover:bg-gray-300'
                  } ${isLoading ? 'opacity-50 cursor-not-allowed' : ''}`}
                >
                  {mod}
                </span>
              </label>
            ))}
          </div>
        </div>

        {error && (
          <div className="p-3 bg-red-50 border border-red-200 rounded-md">
            <p className="text-sm text-red-600">{error}</p>
          </div>
        )}

        <button
          type="submit"
          disabled={isLoading}
          aria-label="Submit"
          className="w-full py-2 px-4 bg-blue-600 text-white font-medium rounded-md hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-offset-2 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
        >
          {isLoading ? (
            <span className="flex items-center justify-center gap-2">
              <svg data-testid="submit-loading-spinner" className="animate-spin h-4 w-4 text-white" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
                <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
              </svg>
              Loading...
            </span>
          ) : (
            'Submit'
          )}
        </button>
      </form>
    </div>
  );
}

export default SeedInput;

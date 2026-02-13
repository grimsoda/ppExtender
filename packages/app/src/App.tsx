import { useState } from 'react';
import { SeedInput } from './components/SeedInput';
import { CohortPreview } from './components/CohortPreview';
import type { Play } from './components/CohortPreview';
import { RecommendationsList } from './components/RecommendationsList';
import { useHealth } from './api-hooks';

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
  plays: Play[];
  seedPpRange: {
    lower: number;
    upper: number;
  };
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
  bpm?: number;
  totalLength?: number;
}

function App() {
  const healthQuery = useHealth();
  const [error, setError] = useState<string | null>(null);
  const [cohortData, setCohortData] = useState<CohortData | null>(null);
  const [recommendations, setRecommendations] = useState<Recommendation[]>([]);
  const [beatmapParams, setBeatmapParams] = useState<{
    beatmapId: number;
    mods: string[];
    topK: number;
  } | null>(null);
  const [isLoadingRecommendations, setIsLoadingRecommendations] = useState(false);

  const handleCohortData = (data: CohortData) => {
    setCohortData(data);
  };

  const handleBeatmapParams = (params: {
    beatmapId: number;
    mods: string[];
    topK: number;
  }) => {
    setBeatmapParams(params);
  };

  const handleRecommendations = (recs: Recommendation[]) => {
    setRecommendations(recs);
  };

  const handleError = (errorMessage: string) => {
    setError(errorMessage);
  };

  const handleLoadingChange = (_isCohortLoading: boolean, isRecommendationsLoading: boolean) => {
    setIsLoadingRecommendations(isRecommendationsLoading);
  };

  const handleSubmitStart = () => {
    setError(null);
  };

  const handleSelectRecommendation = (recommendation: Recommendation) => {
    console.log('Selected recommendation:', recommendation);
  };

  return (
    <div className="min-h-screen bg-gray-100">
      <header className="bg-white shadow-sm">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-4">
          <h1 className="text-2xl font-bold text-gray-900">osu! Recommender</h1>
          <p className="text-sm text-gray-600">Beatmap recommendation system</p>
        </div>
      </header>

      {healthQuery.isLoading && (
        <div className="flex items-center justify-center min-h-[400px]">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-gray-900"></div>
        </div>
      )}

      {healthQuery.isError && (
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
          <div className="bg-red-50 border border-red-200 rounded-lg p-4">
            <div className="flex items-start">
              <svg
                className="h-5 w-5 text-red-400 mt-0.5 mr-3"
                fill="none"
                viewBox="0 0 24 24"
                stroke="currentColor"
              >
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth={2}
                  d="M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"
                />
              </svg>
              <div>
                <h3 className="text-sm font-medium text-red-800">Connection Error</h3>
                <p className="text-sm text-red-700 mt-1">Failed to connect to the API server. Please ensure the backend is running.</p>
              </div>
            </div>
          </div>
        </div>
      )}

      {healthQuery.isSuccess && (
        <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
          <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
            <div className="lg:col-span-1">
              <SeedInput
                onSubmitStart={handleSubmitStart}
                onLoadingChange={handleLoadingChange}
                onCohortData={handleCohortData}
                onBeatmapParams={handleBeatmapParams}
                onRecommendations={handleRecommendations}
                onError={handleError}
              />
            </div>

            <div className="lg:col-span-2 space-y-6">
              {error && (
                <div className="bg-red-50 border border-red-200 rounded-lg p-4">
                  <div className="flex items-start">
                    <svg
                      className="h-5 w-5 text-red-400 mt-0.5 mr-3"
                      fill="none"
                      viewBox="0 0 24 24"
                      stroke="currentColor"
                    >
                      <path
                        strokeLinecap="round"
                        strokeLinejoin="round"
                        strokeWidth={2}
                        d="M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"
                      />
                    </svg>
                    <div>
                      <h3 className="text-sm font-medium text-red-800">Error</h3>
                      <p className="text-sm text-red-700 mt-1">{error}</p>
                    </div>
                  </div>
                </div>
              )}

              <CohortPreview
                cohort={cohortData}
                beatmapId={beatmapParams?.beatmapId}
                mods={beatmapParams?.mods}
                topK={beatmapParams?.topK}
              />

              <RecommendationsList
                recommendations={recommendations}
                onSelect={handleSelectRecommendation}
                isLoading={isLoadingRecommendations}
              />
            </div>
          </div>
        </main>
      )}
    </div>
  );
}

export default App;

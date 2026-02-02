import { useState } from 'react';
import { SeedInput } from './components/SeedInput';
import { CohortPreview } from './components/CohortPreview';
import { RecommendationsList } from './components/RecommendationsList';
import { fetchCohort, fetchRecommendations } from './api';

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

function App() {
  const [isLoadingCohort, setIsLoadingCohort] = useState(false);
  const [isLoadingRecommendations, setIsLoadingRecommendations] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [cohortData, setCohortData] = useState<CohortData | null>(null);
  const [recommendations, setRecommendations] = useState<Recommendation[]>([]);

  const handleSubmit = async (params: { 
    beatmapId: string; 
    minPp: number; 
    maxPp: number; 
    mods: string[] 
  }) => {
    setIsLoadingCohort(true);
    setIsLoadingRecommendations(false);
    setError(null);
    setCohortData(null);
    setRecommendations([]);

    let cohort: CohortData;
    try {
      cohort = await fetchCohort(params);
      setCohortData(cohort);
    } catch (err) {
      setError('Failed to fetch cohort');
      setIsLoadingCohort(false);
      return;
    }
    setIsLoadingCohort(false);

    setIsLoadingRecommendations(true);
    try {
      const recs = await fetchRecommendations(params);
      setRecommendations(recs);
    } catch (err) {
      console.error('Failed to fetch recommendations:', err);
    }
    setIsLoadingRecommendations(false);
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

      <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
          <div className="lg:col-span-1">
            <SeedInput onSubmit={handleSubmit} isLoading={isLoadingCohort} />
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

            <CohortPreview cohort={cohortData} isLoading={isLoadingCohort} />

            <RecommendationsList 
              recommendations={recommendations} 
              onSelect={handleSelectRecommendation}
              isLoading={isLoadingRecommendations}
            />
          </div>
        </div>
      </main>
    </div>
  );
}

export default App;

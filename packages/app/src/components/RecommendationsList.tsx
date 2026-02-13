import { BeatmapCard } from './BeatmapCard';

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

interface RecommendationsListProps {
  recommendations: Recommendation[];
  onSelect: (recommendation: Recommendation) => void;
  isLoading?: boolean;
}

export function RecommendationsList({ 
  recommendations = [], 
  onSelect,
  isLoading = false 
}: RecommendationsListProps) {
  if (isLoading) {
    return (
      <div className="bg-white p-6 rounded-lg shadow-md">
        <div className="flex items-center justify-center py-8">
          <svg data-testid="loading-spinner" className="animate-spin h-8 w-8 text-blue-600" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
            <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
            <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
          </svg>
        </div>
        <p className="text-center text-gray-600">Loading recommendations...</p>
      </div>
    );
  }

  if (recommendations.length === 0) {
    return (
      <div className="bg-white p-6 rounded-lg shadow-md">
        <p className="text-center text-gray-500">No recommendations found</p>
      </div>
    );
  }

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h2 className="text-xl font-semibold text-gray-800">Recommended Beatmaps</h2>
        <span className="text-sm text-gray-500">
          {recommendations.length} recommendations
        </span>
      </div>

      <div className="space-y-3">
        {recommendations.map((recommendation) => (
          <BeatmapCard
            key={recommendation.beatmapId}
            beatmap={recommendation}
            onClick={onSelect}
          />
        ))}
      </div>
    </div>
  );
}

export default RecommendationsList;

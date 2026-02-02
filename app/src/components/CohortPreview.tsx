interface CohortPreviewProps {
  cohort: {
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
  } | null;
  isLoading?: boolean;
}

export function CohortPreview({ cohort, isLoading = false }: CohortPreviewProps) {
  if (isLoading) {
    return (
      <div className="bg-white p-6 rounded-lg shadow-md">
        <div className="flex items-center justify-center py-8">
          <svg data-testid="loading-spinner" className="animate-spin h-8 w-8 text-blue-600" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
            <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
            <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
          </svg>
        </div>
        <p className="text-center text-gray-600">Loading cohort data...</p>
      </div>
    );
  }

  if (!cohort) {
    return (
      <div className="bg-white p-6 rounded-lg shadow-md">
        <p className="text-center text-gray-500">No cohort data available</p>
      </div>
    );
  }

  return (
    <div className="bg-white p-6 rounded-lg shadow-md">
      <h2 className="text-xl font-semibold text-gray-800 mb-4">Cohort Analysis</h2>
      
      <div className="mb-6">
        <div className="flex items-center justify-between mb-2">
          <span className="text-sm font-medium text-gray-600">Cohort Size</span>
          <span className="text-2xl font-bold text-blue-600">{cohort.size}</span>
        </div>
      </div>

      <div className="border-t border-gray-200 pt-4">
        <h3 className="text-sm font-medium text-gray-700 mb-3">PP Distribution</h3>
        <div data-testid="pp-histogram" className="h-32 bg-gray-100 rounded-md mb-4 flex items-end justify-around p-2">
          {Array.from({ length: 10 }).map((_, i) => (
            <div
              key={i}
              className="w-6 bg-blue-500 rounded-t"
              style={{ height: `${Math.random() * 80 + 20}%` }}
            />
          ))}
        </div>
        <div className="grid grid-cols-4 gap-2 text-sm">
          <div>Min: {cohort.ppDistribution.min}</div>
          <div>Max: {cohort.ppDistribution.max}</div>
          <div>Mean: {cohort.ppDistribution.mean}</div>
          <div>Median: {cohort.ppDistribution.median}</div>
        </div>
      </div>

      <div className="border-t border-gray-200 pt-4 mt-4">
        <h3 className="text-sm font-medium text-gray-700 mb-3">Accuracy Distribution</h3>
        <div data-testid="accuracy-histogram" className="h-32 bg-gray-100 rounded-md mb-4 flex items-end justify-around p-2">
          {Array.from({ length: 10 }).map((_, i) => (
            <div
              key={i}
              className="w-6 bg-green-500 rounded-t"
              style={{ height: `${Math.random() * 80 + 20}%` }}
            />
          ))}
        </div>
        <div className="grid grid-cols-4 gap-2 text-sm">
          <div>Min: {cohort.accuracyDistribution.min}</div>
          <div>Max: {cohort.accuracyDistribution.max}</div>
          <div>Mean: {cohort.accuracyDistribution.mean}</div>
          <div>Median: {cohort.accuracyDistribution.median}</div>
        </div>
      </div>

      <div className="border-t border-gray-200 pt-4 mt-4">
        <h3 className="text-sm font-medium text-gray-700 mb-3">Top Players</h3>
        <div className="space-y-2">
          {cohort.topPlayers.map((player) => (
            <div key={player.userId} className="flex items-center justify-between p-2 bg-gray-50 rounded">
              <span className="font-medium">{player.username}</span>
              <div className="text-sm text-gray-600">
                <span>{player.pp} pp</span>
                <span className="ml-2">{player.accuracy}%</span>
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}

export default CohortPreview;

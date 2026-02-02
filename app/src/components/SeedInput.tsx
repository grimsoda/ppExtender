import { useState } from 'react';

interface SeedInputProps {
  onSubmit: (params: { 
    beatmapId: string; 
    minPp: number; 
    maxPp: number; 
    mods: string[] 
  }) => void;
  isLoading?: boolean;
}

const AVAILABLE_MODS = ['HD', 'HR', 'DT', 'FL', 'EZ', 'HT', 'NC', 'SO', 'PF', 'SD'];

export function SeedInput({ onSubmit, isLoading = false }: SeedInputProps) {
  const [beatmapId, setBeatmapId] = useState('');
  const [minPp, setMinPp] = useState(0);
  const [maxPp, setMaxPp] = useState(500);
  const [selectedMods, setSelectedMods] = useState<string[]>([]);
  const [error, setError] = useState<string | null>(null);

  const handleModToggle = (mod: string) => {
    setSelectedMods(prev =>
      prev.includes(mod)
        ? prev.filter(m => m !== mod)
        : [...prev, mod]
    );
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

    onSubmit({
      beatmapId,
      minPp,
      maxPp,
      mods: selectedMods,
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
              type="range"
              id="minPp"
              min="0"
              max="1000"
              value={minPp}
              onChange={(e) => setMinPp(Number(e.target.value))}
              className="w-full"
              disabled={isLoading}
            />
            <span className="text-sm text-gray-600">{minPp}</span>
          </div>

          <div>
            <label htmlFor="maxPp" className="block text-sm font-medium text-gray-700 mb-1">
              Max PP
            </label>
            <input
              type="range"
              id="maxPp"
              min="0"
              max="1000"
              value={maxPp}
              onChange={(e) => setMaxPp(Number(e.target.value))}
              className="w-full"
              disabled={isLoading}
            />
            <span className="text-sm text-gray-600">{maxPp}</span>
          </div>
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

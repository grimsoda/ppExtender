interface Beatmap {
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

interface BeatmapCardProps {
  beatmap: Beatmap;
  onClick: (beatmap: Beatmap) => void;
}

export function BeatmapCard({ beatmap, onClick }: BeatmapCardProps) {
  return (
    <div
      data-testid="beatmap-card"
      onClick={() => onClick(beatmap)}
      className="bg-white rounded-lg shadow-md overflow-hidden cursor-pointer hover:shadow-lg transition-shadow"
    >
      <div className="flex">
        <div className="w-32 h-32 flex-shrink-0">
          <img
            src={beatmap.coverUrl}
            alt={beatmap.title}
            className="w-full h-full object-cover"
          />
        </div>
        
        <div className="flex-grow p-4">
          <div className="flex items-start justify-between">
            <div>
              <h3 className="text-lg font-semibold text-gray-900">{beatmap.title}</h3>
              <p className="text-sm text-gray-600">{beatmap.artist}</p>
            </div>
            <span className="text-sm text-gray-500">#{beatmap.beatmapId}</span>
          </div>
          
          <div className="mt-2">
            <span className="text-sm font-medium text-gray-700">{beatmap.difficulty}</span>
            <span className="text-sm text-yellow-600"> ★{beatmap.stars}</span>
          </div>
          
          <div className="mt-3 flex items-center gap-4 text-sm">
            <span className="text-blue-600 font-medium">{beatmap.pp} pp</span>
            <span className="text-green-600 font-medium">{beatmap.accuracy}%</span>
          </div>
          
          {beatmap.mods.length > 0 && (
            <div className="mt-2 flex gap-1">
              {beatmap.mods.map((mod) => (
                <span
                  key={mod}
                  data-testid="mod-tag"
                  className="px-2 py-0.5 bg-purple-100 text-purple-700 text-xs rounded font-medium"
                >
                  {mod}
                </span>
              ))}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

export default BeatmapCard;

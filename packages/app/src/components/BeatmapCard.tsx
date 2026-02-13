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
  beatmapsetId?: number;
  bpm?: number;
  totalLength?: number;
}

interface BeatmapCardProps {
  beatmap: Beatmap;
  onClick: (beatmap: Beatmap) => void;
}

const formatDuration = (seconds: number): string => {
  const mins = Math.floor(seconds / 60);
  const secs = seconds % 60;
  return `${mins}:${secs.toString().padStart(2, '0')}`;
};

export function BeatmapCard({ beatmap, onClick }: BeatmapCardProps) {
  const handleClick = () => {
    window.open(`https://osu.ppy.sh/beatmaps/${beatmap.beatmapId}`, '_blank');
    onClick(beatmap);
  };

  const coverUrl = beatmap.coverUrl || 
    `https://assets.ppy.sh/beatmaps/${beatmap.beatmapsetId}/covers/list.jpg`;

  return (
    <div
      data-testid="beatmap-card"
      onClick={handleClick}
      className="bg-white rounded-lg shadow-md overflow-hidden cursor-pointer hover:shadow-lg transition-shadow"
    >
      <div className="flex">
        <div className="w-32 h-32 flex-shrink-0 bg-gray-200">
          <img
            src={coverUrl}
            alt={beatmap.title}
            className="w-full h-full object-cover"
            onError={(e) => {
              (e.target as HTMLImageElement).src = 'https://osu.ppy.sh/images/beatmaps/default-bg.png';
            }}
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
          
          <div className="mt-2 flex items-center gap-2">
            <span className="text-sm font-medium text-gray-700">{beatmap.difficulty}</span>
            <span className="text-sm text-yellow-600">★ {beatmap.stars.toFixed(2)}</span>
          </div>
          
          <div className="mt-1 flex items-center gap-4 text-xs text-gray-500">
            {beatmap.bpm && <span>BPM: {beatmap.bpm}</span>}
            {beatmap.totalLength && <span>Length: {formatDuration(beatmap.totalLength)}</span>}
          </div>
          
          <div className="mt-2 flex items-center gap-4 text-sm">
            <span className="text-blue-600 font-medium">{Math.round(beatmap.pp)} pp</span>
            <span className="text-green-600 font-medium">{beatmap.accuracy.toFixed(1)}%</span>
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

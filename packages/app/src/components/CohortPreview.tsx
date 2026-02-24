import { useState } from 'react';
import { ResponsiveScatterPlot } from '@nivo/scatterplot';
import type { ScatterPlotNodeProps } from '@nivo/scatterplot';
import { useGetBeatmapPlays } from '../api-hooks';

interface Play {
  scoreId?: number;
  userId: number;
  username?: string;
  globalRank?: number | null;
  playRank?: number | null;
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
}

const hasClassicMod = (modsJson: string): boolean => {
  try {
    const mods = JSON.parse(modsJson);
    if (!Array.isArray(mods)) return false;
    return mods.some((mod: { acronym?: string }) => mod.acronym === 'CL');
  } catch {
    return false;
  }
};

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
      globalRank?: number | null;
      pp: number;
      accuracy: number;
    }>;
    plays: Play[];
    seedPpRange: {
      lower: number;
      upper: number;
    };
  } | null;
  beatmapId?: number;
  mods?: string[];
  topK?: number;
}

const getRankColor = (rank: string): string => {
  switch (rank) {
    case 'SS':
    case 'X':
    case 'XH':
      return '#FFD700';
    case 'S':
    case 'SH':
      return '#FFA500';
    case 'A': return '#7CFC00';
    case 'B': return '#00BFFF';
    case 'C': return '#FF8C00';
    default: return '#DC143C';
  }
};

const normalizeRank = (rank: string): string => {
  if (rank === 'X' || rank === 'XH') return 'SS';
  if (rank === 'SH') return 'S';
  return rank;
};

const CustomNode = (props: ScatterPlotNodeProps<any>) => {
  const { node, style, onMouseEnter, onMouseMove, onMouseLeave, onClick } = props;
  const isClassic = node.data?.hasCl ?? false;

  const x = typeof style.x === 'number' ? style.x : style.x.get();
  const y = typeof style.y === 'number' ? style.y : style.y.get();
  const size = typeof style.size === 'number' ? style.size : style.size.get();
  const color = typeof style.color === 'string' ? style.color : style.color.get();

  const radius = typeof size === 'number' ? size / 2 : 6;

  const handleMouseEnter = (event: React.MouseEvent) => {
    if (onMouseEnter) onMouseEnter(node, event);
  };
  const handleMouseMove = (event: React.MouseEvent) => {
    if (onMouseMove) onMouseMove(node, event);
  };
  const handleMouseLeave = (event: React.MouseEvent) => {
    if (onMouseLeave) onMouseLeave(node, event);
  };
  const handleClick = (event: React.MouseEvent) => {
    if (onClick) onClick(node, event);
  };

  if (isClassic) {
    return (
      <g transform={`translate(${x},${y})`}>
        <circle
          r={radius}
          fill="transparent"
          stroke={color}
          strokeWidth={2}
          onMouseEnter={handleMouseEnter}
          onMouseMove={handleMouseMove}
          onMouseLeave={handleMouseLeave}
          onClick={handleClick}
        />
      </g>
    );
  }

  return (
    <g transform={`translate(${x},${y})`}>
      <circle
        r={radius}
        fill={color}
        stroke={color}
        strokeWidth={2}
        onMouseEnter={handleMouseEnter}
        onMouseMove={handleMouseMove}
        onMouseLeave={handleMouseLeave}
        onClick={handleClick}
      />
    </g>
  );
};

export function CohortPreview({ cohort, beatmapId, mods, topK }: CohortPreviewProps) {
  const [showAllPlays, setShowAllPlays] = useState(false);

  const allPlaysQuery = useGetBeatmapPlays(
    beatmapId || 0,
    {
      mods,
      top_k: topK,
    }
  );

  const allPlays = allPlaysQuery.data || [];
  const isLoading = allPlaysQuery.isLoading;

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

  // Use allPlays when showAllPlays is checked, otherwise use cohort.plays
  const playsSource = showAllPlays && allPlays ? allPlays : cohort.plays;

  const playsByRank = playsSource?.reduce((acc: Record<string, any[]>, play: Play) => {
    const rank = normalizeRank(play.rank || 'D');
    if (!acc[rank]) acc[rank] = [];
    acc[rank].push({
      x: play.pp,
      y: play.accuracy,
      scoreId: play.scoreId,
      userId: play.userId,
      username: play.username,
      globalRank: play.globalRank,
      playRank: play.playRank,
      score: play.score,
      mods: play.mods,
      hasCl: hasClassicMod(play.mods),
      rank,
      maxCombo: play.maxCombo,
      beatmapMaxCombo: play.beatmapMaxCombo,
      count300: play.count300,
      count100: play.count100,
      count50: play.count50,
      countMiss: play.countMiss,
      countSliderBreaks: play.countSliderBreaks,
    });
    return acc;
  }, {} as Record<string, any[]>);

  const isInSeedRange = (pp: number) => {
    const lower = cohort.seedPpRange.lower;
    const upper = cohort.seedPpRange.upper;
    // Handle inverted bounds - show all if range is invalid
    if (lower > upper) return true;
    return pp >= lower && pp <= upper;
  };

  // Create filtered plays by rank based on checkbox state
  const filteredPlaysByRank = showAllPlays 
    ? playsByRank  // Show all when checked
    : Object.entries(playsByRank || {}).reduce((acc, [rank, plays]) => {
        acc[rank] = plays.filter((play: any) => {
          // Skip plays with null/undefined pp
          if (play.x == null) return false;
          return isInSeedRange(play.x as number);
        });
        return acc;
      }, {} as Record<string, any[]>);

  const chartData = ['SS', 'S', 'A', 'B', 'C', 'D'].map((rank) => ({
    id: rank,
    data: filteredPlaysByRank?.[rank] || [],
  })).filter((series) => series.data.length > 0);

  const allPpValues = Object.values(filteredPlaysByRank || {})
    .flat()
    .map((p: any) => p.x as number)
    .filter((v: number) => v != null);
  const allAccValues = Object.values(filteredPlaysByRank || {})
    .flat()
    .map((p: any) => p.y as number)
    .filter((v: number) => v != null);
  
  const dataMinPp = allPpValues.length > 0 ? Math.min(...allPpValues) : 0;
  const dataMaxPp = allPpValues.length > 0 ? Math.max(...allPpValues) : 0;
  const dataMinAcc = allAccValues.length > 0 ? Math.min(...allAccValues) : 90;
  const dataMaxAcc = allAccValues.length > 0 ? Math.max(...allAccValues) : 100;

  const seedLower = cohort.seedPpRange.lower;
  const seedUpper = cohort.seedPpRange.upper;

  const yMin = dataMinAcc;
  const yMax = Math.max(100, dataMaxAcc);
  
  const chartXMin = showAllPlays ? dataMinPp : seedLower;
  const chartXMax = showAllPlays ? dataMaxPp : seedUpper;

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
        <div className="flex items-center justify-between mb-3">
          <h3 className="text-sm font-medium text-gray-700">PP vs Accuracy Distribution</h3>
          <label className="flex items-center gap-2 text-sm text-gray-600 cursor-pointer">
            <input
              type="checkbox"
              checked={showAllPlays}
              onChange={(e) => setShowAllPlays(e.target.checked)}
              className="rounded border-gray-300 text-blue-600 focus:ring-blue-500"
            />
            Show all top plays
          </label>
        </div>
        <div data-testid="pp-accuracy-scatter" className="h-80 bg-gray-50 rounded-md mb-4">
          {playsSource && playsSource.length > 0 ? (
            <ResponsiveScatterPlot
              key={`scatter-${showAllPlays}-${chartXMin}-${chartXMax}`}
              data={chartData}
              margin={{ top: 20, right: 20, bottom: 50, left: 60 }}
              xScale={{ type: 'linear', min: chartXMin, max: chartXMax, nice: false }}
              yScale={{ type: 'linear', min: yMin, max: yMax, nice: false }}
              blendMode="normal"
              nodeComponent={CustomNode}
              axisTop={null}
              axisRight={null}
              axisBottom={{
                tickSize: 5,
                tickPadding: 5,
                tickRotation: 0,
                legend: 'PP',
                legendPosition: 'middle',
                legendOffset: 40,
              }}
              axisLeft={{
                tickSize: 5,
                tickPadding: 5,
                tickRotation: 0,
                legend: 'Accuracy (%)',
                legendPosition: 'middle',
                legendOffset: -45,
              }}

              tooltip={({ node }) => {
                if (!node.data) return null;
                const data = node.data as any;
                const displayName = data.username || `User ${data.userId}`;
                const globalRankText = data.globalRank ? `(Global #${data.globalRank.toLocaleString()})` : '';
                const playRankText = data.playRank ? `~#${data.playRank} top play` : '';
                return (
                  <div className="bg-white p-3 rounded shadow-lg border border-gray-200 min-w-[220px]">
                    <div className="border-b border-gray-200 pb-2 mb-2">
                      <div className="flex items-center gap-1 flex-wrap">
                        <p className="text-sm font-semibold text-gray-900">{displayName}</p>
                        {globalRankText && (
                          <span className="text-xs text-gray-500">{globalRankText}</span>
                        )}
                      </div>
                      <div className="flex items-center gap-2 mt-1">
                        <span
                          className="text-sm font-bold px-2 py-0.5 rounded"
                          style={{
                            backgroundColor: getRankColor(data.rank),
                            color: data.rank === 'SS' || data.rank === 'X' || data.rank === 'XH' ? '#000' : '#fff'
                          }}
                        >
                          {data.rank}
                        </span>
                        {data.hasCl && (
                          <span className="text-xs bg-gray-100 text-gray-700 px-2 py-0.5 rounded font-medium">
                            +CL
                          </span>
                        )}
                        <span className="text-sm text-gray-600">{data.score?.toLocaleString()} pts</span>
                        {playRankText && (
                          <span className="text-xs text-gray-500 ml-auto">{playRankText}</span>
                        )}
                      </div>
                    </div>

                    <div className="space-y-1">
                      <div className="flex justify-between">
                        <span className="text-sm text-gray-600">PP:</span>
                        <span className="text-sm font-medium">{Number(data.x).toFixed(2)}</span>
                      </div>
                      <div className="flex justify-between">
                        <span className="text-sm text-gray-600">Accuracy:</span>
                        <span className="text-sm font-medium">{data.y}%</span>
                      </div>
                      <div className="flex justify-between">
                        <span className="text-sm text-gray-600">Combo:</span>
                        <span className="text-sm font-medium">{data.maxCombo?.toLocaleString()}/{data.beatmapMaxCombo?.toLocaleString()}</span>
                      </div>
                    </div>

                      <div className="border-t border-gray-200 pt-2 mt-2">
                        <p className="text-xs text-gray-500 mb-1">Hit Distribution</p>
                        <div className="grid grid-cols-3 gap-2 text-center">
                          <div>
                            <p className="text-xs text-gray-500">300</p>
                            <p className="text-sm font-medium text-green-600">{data.count300}</p>
                          </div>
                          <div>
                            <p className="text-xs text-gray-500">100</p>
                            <p className="text-sm font-medium text-yellow-600">{data.count100}</p>
                          </div>
                          <div>
                            <p className="text-xs text-gray-500">50</p>
                            <p className="text-sm font-medium text-orange-600">{data.count50}</p>
                          </div>
                        </div>
                        <div className="flex justify-center gap-4 mt-2">
                          <div className="text-center">
                            <p className="text-xs text-gray-500">Miss</p>
                            <p className="text-sm font-medium text-red-600">{data.countMiss || 0}</p>
                          </div>
                          <div className="text-center">
                            <p className="text-xs text-gray-500">SB</p>
                            <p className="text-sm font-medium text-red-600">{data.countSliderBreaks || 0}</p>
                          </div>
                        </div>
                      </div>
                  </div>
                );
              }}
              onClick={(node) => {
                if (node.data?.scoreId) {
                  window.open(`https://osu.ppy.sh/scores/${node.data.scoreId}`, '_blank');
                }
              }}
              colors={({ serieId }) => getRankColor(serieId as string)}
              nodeSize={8}
              enableGridX={true}
              enableGridY={true}
              layers={[
                'grid',
                'axes',
                ({ xScale, innerHeight }: any) => {
                  const x1 = xScale(Math.max(chartXMin, seedLower));
                  const x2 = xScale(Math.min(chartXMax, seedUpper));

                  if (x1 === null || x2 === null) return null;

                  return (
                    <rect
                      x={Math.min(x1, x2)}
                      y={0}
                      width={Math.abs(x2 - x1)}
                      height={innerHeight}
                      fill="rgba(59, 130, 246, 0.1)"
                      stroke="rgba(59, 130, 246, 0.5)"
                      strokeWidth={2}
                    />
                  );
                },
                'nodes',
              ]}
            />
          ) : (
            <div className="flex items-center justify-center h-full text-gray-500">
              No play data available
            </div>
          )}
        </div>
        <div className="grid grid-cols-2 gap-4 text-sm">
          <div>
            <span className="font-medium">PP Range:</span> {cohort.ppDistribution.min} - {cohort.ppDistribution.max}
          </div>
          <div>
            <span className="font-medium">Accuracy Range:</span> {cohort.accuracyDistribution.min}% - {cohort.accuracyDistribution.max}%
          </div>
        </div>
      </div>

      <div className="border-t border-gray-200 pt-4 mt-4">
        <h3 className="text-sm font-medium text-gray-700 mb-3">Legend</h3>
        <div className="flex flex-wrap gap-4 text-sm">
          {(['SS', 'S', 'A', 'B', 'C', 'D'] as const)
            .filter((rank) => (filteredPlaysByRank?.[rank]?.length || 0) > 0)
            .map((rank) => (
              <div key={rank} className="flex items-center gap-2">
                <div
                  className="w-4 h-4 rounded-full"
                  style={{ backgroundColor: getRankColor(rank) }}
                />
                <span>{rank}</span>
              </div>
            ))}
          <div className="flex items-center gap-2">
            <div className="w-4 h-4 rounded-full border-2 border-gray-500" />
            <span className="text-gray-600">+CL (Classic mod)</span>
          </div>
        </div>
      </div>

      <div className="border-t border-gray-200 pt-4 mt-4">
        <h3 className="text-sm font-medium text-gray-700 mb-3">Top Players</h3>
        <div className="space-y-2">
          {cohort.topPlayers && cohort.topPlayers.length > 0 ? (
            cohort.topPlayers.map((player, index) => {
              const matchingPlay = cohort.plays?.find(
                (play) => play.userId === player.userId && play.pp === player.pp
              );
              return (
                <div
                  key={player.userId}
                  className="flex items-center justify-between p-2 bg-gray-50 rounded hover:bg-gray-100 cursor-pointer"
                  onClick={() => {
                    if (matchingPlay?.scoreId) {
                      window.open(`https://osu.ppy.sh/scores/${matchingPlay.scoreId}`, '_blank');
                    } else {
                      window.open(`https://osu.ppy.sh/users/${player.userId}`, '_blank');
                    }
                  }}
                >
                  <div className="flex items-center gap-2">
                    <span className="text-sm text-gray-400 w-5">#{index + 1}</span>
                    <div>
                      <span className="font-medium">{player.username}</span>
                      {player.globalRank && (
                        <span className="text-xs text-gray-500 ml-2">
                          (Global #{player.globalRank.toLocaleString()})
                        </span>
                      )}
                    </div>
                  </div>
                  <div className="text-sm text-gray-600 text-right">
                    <div>{player.pp.toFixed(2)} pp</div>
                    <div className="text-xs">{player.accuracy.toFixed(2)}%</div>
                  </div>
                </div>
              );
            })
          ) : (
            <p className="text-sm text-gray-500">No top players data available</p>
          )}
        </div>
      </div>
    </div>
  );
}

export type { Play };
export default CohortPreview;

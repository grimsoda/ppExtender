import { z } from 'zod';
import { router, publicProcedure } from '../trpc';
import { getGlobalDatabase } from '../db';
import * as path from 'path';

/**
 * Recommender router with full tRPC procedures
 * Mirrors the Express routes functionality for tRPC migration
 * 
 * Procedures:
 * - health: Health check endpoint
 * - getCohort: Get cohort for a beatmap (POST /api/cohort)
 * - getBeatmapPlays: Get plays for a beatmap (POST /api/beatmap-plays)
 * - getRecommendations: Get recommendations for a beatmap (POST /api/recommend)
 * - getBeatmaps: Batch get beatmaps by IDs (POST /api/beatmaps)
 * - getUser: Get user by ID (GET /api/user/:id)
 */

const DB_PATH = process.env.DUCKDB_PATH || path.resolve(__dirname, '../../../../data/warehouse/2026-02/osu.duckdb');
const db = getGlobalDatabase(DB_PATH);

// Helper function to calculate median
function calculateMedian(sortedValues: number[]): number {
  if (sortedValues.length === 0) return 0;
  const mid = Math.floor(sortedValues.length / 2);
  if (sortedValues.length % 2 === 0) {
    return (sortedValues[mid - 1] + sortedValues[mid]) / 2;
  }
  return sortedValues[mid];
}

// Input validation schemas
const GetCohortInput = z.object({
  beatmap_id: z.number(),
  pp_lower: z.number().default(0),
  pp_upper: z.number().default(10000),
  mods: z.array(z.string()).optional(),
  top_k: z.number().default(200),
});

const GetBeatmapPlaysInput = z.object({
  beatmap_id: z.number(),
  mods: z.array(z.string()).optional(),
  top_k: z.number().default(200),
});

const GetRecommendationsInput = z.object({
  beatmap_id: z.number(),
  pp_lower: z.number().default(0),
  pp_upper: z.number().default(10000),
  mods: z.array(z.string()).optional(),
  limit: z.number().default(10),
});

const GetBeatmapsInput = z.object({
  beatmap_ids: z.array(z.number()).min(1).max(100),
});

const GetUserInput = z.object({
  user_id: z.number(),
});

export const recommenderRouter = router({
  /**
   * Health check endpoint
   * Mirrors GET /health
   */
  health: publicProcedure
    .query(async () => {
      try {
        await db.connect();
        return {
          status: 'ok',
          database: 'connected',
        };
      } catch (error) {
        return {
          status: 'ok',
          database: 'disconnected',
        };
      }
    }),

  /**
   * Get cohort for a beatmap
   * Mirrors POST /api/cohort
   */
  getCohort: publicProcedure
    .input(GetCohortInput)
    .query(async ({ input }) => {
      const { beatmap_id, pp_lower = 0, pp_upper = 10000, mods, top_k = 200 } = input;
      
      const k = Math.min(Math.max(Number(top_k) || 200, 50), 500);
      const connection = await db.getConnection();

      try {
        let query = `
          SELECT
            t.id,
            t.user_id,
            t.beatmap_id,
            t.pp,
            t.mods_key as mods,
            t.accuracy,
            t.score,
            t.rank,
            t.speed_mod,
            json_extract(t.data, '$.statistics.great') as count300,
            json_extract(t.data, '$.statistics.ok') as count100,
            json_extract(t.data, '$.statistics.meh') as count50,
            json_extract(t.data, '$.statistics.miss') as countmiss,
            json_extract(t.data, '$.statistics.large_tick_miss') as count_slider_breaks,
            su.username,
            st.rank_score_index as global_rank,
            (SELECT COUNT(*) + 1 FROM mart_best_scores b
             WHERE b.user_id = t.user_id
             AND b.pp > t.pp) as play_rank_approx
          FROM mart_user_topk t
          LEFT JOIN raw_sample_users su ON t.user_id = su.user_id
          LEFT JOIN raw_osu_user_stats st ON t.user_id = st.user_id
          WHERE t.beatmap_id = ?
            AND t.pp BETWEEN ? AND ?
        `;

        const params: (number | string)[] = [beatmap_id, pp_lower, pp_upper];
        const modsArray = mods !== undefined && mods !== null ? mods : [];
        const availableMods = ['HD', 'HR', 'DT', 'FL', 'EZ', 'HT', 'NC'];
        
        if (modsArray.length > 0) {
          const filteredMods = modsArray.filter((m: string) => m !== 'CL').sort();
          if (filteredMods.length === 1) {
            const mod = filteredMods[0];
            query += ` AND (t.mods_key = ? OR t.mods_key LIKE ? OR t.mods_key LIKE ?)`;
            params.push(
              JSON.stringify([{ acronym: mod }]),
              `%${JSON.stringify([{ acronym: mod }])}%`,
              `%${JSON.stringify([{ acronym: mod }, { acronym: "CL" }])}%`
            );
          } else {
            const modsJson = JSON.stringify(filteredMods.map((m: string) => ({ acronym: m })));
            const modsWithCl = JSON.stringify([...filteredMods.map((m: string) => ({ acronym: m })), { acronym: "CL" }]);
            query += ` AND (t.mods_key = ? OR t.mods_key LIKE ?)`;
            params.push(modsJson, `%${modsWithCl}%`);
          }
        } else {
          for (const mod of availableMods) {
            query += ` AND t.mods_key NOT LIKE ?`;
            params.push(`%acronym":"${mod}"%`);
          }
        }

        query += ` ORDER BY t.pp DESC LIMIT ${k}`;

        const rows = await connection.query(query, params);

        if (rows.length === 0) {
          // Return empty cohort instead of error - beatmap may have no plays with these filters
          return {
            beatmap_id,
            cohort_size: 0,
            pp_distribution: { min: 0, max: 0, mean: 0, median: 0 },
            accuracy_distribution: { min: 0, max: 0, mean: 0, median: 0 },
            top_players: [],
            plays: [],
            seed_pp_range: { lower: pp_lower, upper: pp_upper }
          };
        }

        const beatmapRows = await connection.query(
          `SELECT max_combo FROM raw_osu_beatmaps WHERE beatmap_id = ?`,
          [beatmap_id]
        );
        const beatmapMaxCombo = beatmapRows.length > 0 ? Number(beatmapRows[0].max_combo) || 0 : 0;

        const ppValues = rows.map((row: any) => row.pp as number).sort((a: number, b: number) => a - b);
        const accuracyValues = rows.map((row: any) => {
          const acc = typeof row.accuracy === 'object' && row.accuracy !== null
            ? Number(row.accuracy.value) / Math.pow(10, row.accuracy.scale || 0)
            : Number(row.accuracy) || 0.95;
          return acc * 100;
        }).sort((a: number, b: number) => a - b);
        
        const minPp = ppValues[0] || 0;
        const maxPp = ppValues[ppValues.length - 1] || 0;
        const meanPp = ppValues.length > 0
          ? ppValues.reduce((a: number, b: number) => a + b, 0) / ppValues.length
          : 0;
        const medianPp = calculateMedian(ppValues);

        const minAcc = accuracyValues[0] || 0;
        const maxAcc = accuracyValues[accuracyValues.length - 1] || 0;
        const meanAcc = accuracyValues.length > 0
          ? accuracyValues.reduce((a: number, b: number) => a + b, 0) / accuracyValues.length
          : 0;
        const medianAcc = calculateMedian(accuracyValues);

        const topPlayers = rows.slice(0, 10).map((row: any) => ({
          userId: Number(row.user_id),
          username: row.username || String(row.user_id),
          globalRank: row.global_rank || null,
          pp: Number(row.pp),
          accuracy: Math.round((Number(row.accuracy) || 0.95) * 100 * 100) / 100
        }));

        const allPlays = rows.map((row: any) => ({
          scoreId: Number(row.id),
          userId: Number(row.user_id),
          username: row.username || String(row.user_id),
          globalRank: row.global_rank || null,
          playRank: row.play_rank_approx || null,
          pp: Number(row.pp),
          accuracy: Math.round((Number(row.accuracy) || 0.95) * 100 * 100) / 100,
          score: Number(row.score) || 0,
          mods: row.mods || '[]',
          rank: row.rank || 'D',
          maxCombo: Number(row.max_combo) || 0,
          beatmapMaxCombo,
          count300: Number(row.count300) || 0,
          count100: Number(row.count100) || 0,
          count50: Number(row.count50) || 0,
          countMiss: Number(row.countmiss) || 0,
          countSliderBreaks: Number(row.count_slider_breaks) || 0
        }));

        const uniqueUserIds = new Set(rows.map((row: any) => row.user_id)).size;

        return {
          beatmap_id,
          cohort_size: uniqueUserIds,
          pp_distribution: {
            min: Math.round(minPp * 100) / 100,
            max: Math.round(maxPp * 100) / 100,
            mean: Math.round(meanPp * 100) / 100,
            median: Math.round(medianPp * 100) / 100
          },
          accuracy_distribution: {
            min: Math.round(minAcc * 100) / 100,
            max: Math.round(maxAcc * 100) / 100,
            mean: Math.round(meanAcc * 100) / 100,
            median: Math.round(medianAcc * 100) / 100
          },
          top_players: topPlayers,
          plays: allPlays,
          seed_pp_range: {
            lower: pp_lower,
            upper: pp_upper
          }
        };
      } finally {
        connection.close();
      }
    }),

  /**
   * Get plays for a beatmap
   * Mirrors POST /api/beatmap-plays
   */
  getBeatmapPlays: publicProcedure
    .input(GetBeatmapPlaysInput)
    .query(async ({ input }) => {
      const { beatmap_id, mods, top_k = 200 } = input;

      const k = Math.min(Math.max(Number(top_k) || 200, 50), 500);
      const connection = await db.getConnection();

      try {
        let query = `
          SELECT
            id,
            user_id,
            beatmap_id,
            pp,
            mods_key as mods,
            accuracy,
            score,
            rank,
            json_extract(data, '$.statistics.great') as count300,
            json_extract(data, '$.statistics.ok') as count100,
            json_extract(data, '$.statistics.meh') as count50,
            json_extract(data, '$.statistics.miss') as countmiss,
            json_extract(data, '$.statistics.large_tick_miss') as count_slider_breaks
          FROM mart_user_topk
          WHERE beatmap_id = ?
        `;

        const params: (number | string)[] = [beatmap_id];
        const modsArray = mods !== undefined && mods !== null ? mods : [];
        const availableMods = ['HD', 'HR', 'DT', 'FL', 'EZ', 'HT', 'NC'];
        
        if (modsArray.length > 0) {
          const filteredMods = modsArray.filter((m: string) => m !== 'CL').sort();
          if (filteredMods.length === 1) {
            const mod = filteredMods[0];
            query += ` AND (mods_key = ? OR mods_key LIKE ? OR mods_key LIKE ?)`;
            params.push(
              JSON.stringify([{ acronym: mod }]),
              `%${JSON.stringify([{ acronym: mod }])}%`,
              `%${JSON.stringify([{ acronym: mod }, { acronym: "CL" }])}%`
            );
          } else {
            const modsJson = JSON.stringify(filteredMods.map((m: string) => ({ acronym: m })));
            const modsWithCl = JSON.stringify([...filteredMods.map((m: string) => ({ acronym: m })), { acronym: "CL" }]);
            query += ` AND (mods_key = ? OR mods_key LIKE ?)`;
            params.push(modsJson, `%${modsWithCl}%`);
          }
        } else {
          for (const mod of availableMods) {
            query += ` AND mods_key NOT LIKE ?`;
            params.push(`%acronym":"${mod}"%`);
          }
        }

        query += ` ORDER BY pp DESC LIMIT ${k}`;

        const rows = await connection.query(query, params);

        if (rows.length === 0) {
          return {
            beatmap_id,
            total_plays: 0,
            all_plays: []
          };
        }

        const beatmapRows = await connection.query(
          `SELECT max_combo FROM raw_osu_beatmaps WHERE beatmap_id = ?`,
          [beatmap_id]
        );
        const beatmapMaxCombo = beatmapRows.length > 0 ? Number(beatmapRows[0].max_combo) || 0 : 0;

        const allPlays = rows.map((row: any) => ({
          scoreId: Number(row.id),
          userId: Number(row.user_id),
          pp: Number(row.pp),
          accuracy: Math.round((Number(row.accuracy) || 0.95) * 100 * 100) / 100,
          score: Number(row.score) || 0,
          mods: row.mods || '[]',
          rank: row.rank || 'D',
          maxCombo: Number(row.max_combo) || 0,
          beatmapMaxCombo,
          count300: Number(row.count300) || 0,
          count100: Number(row.count100) || 0,
          count50: Number(row.count50) || 0,
          countMiss: Number(row.countmiss) || 0,
          countSliderBreaks: Number(row.count_slider_breaks) || 0
        }));

        return {
          beatmap_id,
          total_plays: rows.length,
          all_plays: allPlays
        };
      } finally {
        connection.close();
      }
    }),

  /**
   * Get recommendations for a beatmap
   * Uses a single optimized query with CTE for better performance
   */
  getRecommendations: publicProcedure
    .input(GetRecommendationsInput)
    .query(async ({ input }) => {
      const { beatmap_id, pp_lower = 0, pp_upper = 10000, mods, limit = 10 } = input;

      const connection = await db.getConnection();

      try {
        const beatmapCheckRows = await connection.query(
          'SELECT beatmap_id FROM beatmaps WHERE beatmap_id = ?',
          [beatmap_id]
        );

        if (beatmapCheckRows.length === 0) {
          throw new Error('Beatmap not found');
        }

        const modsArray = mods !== undefined && mods !== null ? mods : [];
        const availableMods = ['HD', 'HR', 'DT', 'FL', 'EZ', 'HT', 'NC'];
        
        let modsFilter = '';
        const modsParams: string[] = [];
        
        if (modsArray.length > 0) {
          const filteredMods = modsArray.filter((m: string) => m !== 'CL').sort();
          if (filteredMods.length === 1) {
            const mod = filteredMods[0];
            modsFilter = ` AND (mods = ? OR mods LIKE ? OR mods LIKE ?)`;
            modsParams.push(
              JSON.stringify([{ acronym: mod }]),
              `%${JSON.stringify([{ acronym: mod }])}%`,
              `%${JSON.stringify([{ acronym: mod }, { acronym: "CL" }])}%`
            );
          } else {
            const modsJson = JSON.stringify(filteredMods.map((m: string) => ({ acronym: m })));
            const modsWithCl = JSON.stringify([...filteredMods.map((m: string) => ({ acronym: m })), { acronym: "CL" }]);
            modsFilter = ` AND (mods = ? OR mods LIKE ?)`;
            modsParams.push(modsJson, `%${modsWithCl}%`);
          }
        } else {
          for (const mod of availableMods) {
            modsFilter += ` AND mods NOT LIKE ?`;
            modsParams.push(`%acronym":"${mod}"%`);
          }
        }

        const query = `
          WITH cohort AS (
            SELECT DISTINCT user_id
            FROM user_beatmap_playcount
            WHERE beatmap_id = ?
              AND pp BETWEEN ? AND ?
              ${modsFilter}
          )
          SELECT
            ub.beatmap_id,
            b.beatmapset_id,
            b.title,
            b.artist,
            b.version,
            b.creator,
            b.difficulty_rating,
            b.bpm,
            b.total_length,
            COUNT(DISTINCT ub.user_id) as play_count,
            AVG(ub.pp) as avg_pp,
            AVG(ub.accuracy) as avg_accuracy,
            (SELECT COUNT(*) FROM cohort) as cohort_size
          FROM user_beatmap_playcount ub
          JOIN beatmaps b ON ub.beatmap_id = b.beatmap_id
          WHERE ub.user_id IN (SELECT user_id FROM cohort)
            AND ub.beatmap_id != ?
            AND ub.pp BETWEEN ? AND ?
          GROUP BY ub.beatmap_id, b.beatmapset_id, b.title, b.artist, b.version, b.creator,
                   b.difficulty_rating, b.bpm, b.total_length
          ORDER BY play_count DESC, avg_pp DESC
          LIMIT ?
        `;

        const params = [
          beatmap_id, pp_lower, pp_upper,
          ...modsParams,
          beatmap_id, pp_lower, pp_upper, limit
        ];

        const recommendRows = await connection.query(query, params);
        const cohortSize = recommendRows.length > 0 ? Number(recommendRows[0].cohort_size) : 0;

        return {
          beatmap_id,
          total: recommendRows.length,
          recommendations: recommendRows.map((row: any) => ({
            beatmap_id: row.beatmap_id,
            beatmapset_id: row.beatmapset_id,
            title: row.title,
            artist: row.artist,
            version: row.version,
            creator: row.creator,
            difficulty_rating: row.difficulty_rating,
            bpm: row.bpm,
            total_length: row.total_length,
            play_count: Number(row.play_count),
            avg_pp: Math.round((row.avg_pp as number) * 100) / 100,
            avg_accuracy: Math.round((row.avg_accuracy as number) * 100) / 100,
            similarity_score: cohortSize > 0 ? Math.round(Number(row.play_count) / cohortSize * 100) / 100 : 0
          }))
        };
      } finally {
        connection.close();
      }
    }),

  /**
   * Batch get beatmaps by IDs
   * Mirrors POST /api/beatmaps
   */
  getBeatmaps: publicProcedure
    .input(GetBeatmapsInput)
    .query(async ({ input }) => {
      const { beatmap_ids } = input;

      const connection = await db.getConnection();

      try {
        const placeholders = beatmap_ids.map(() => '?').join(',');
        const query = `
          SELECT 
            beatmap_id,
            title,
            artist,
            version,
            creator,
            difficulty_rating as difficulty,
            bpm,
            total_length,
            mode,
            status
          FROM beatmaps
          WHERE beatmap_id IN (${placeholders})
        `;

        const rows = await connection.query(query, beatmap_ids);

        const beatmapMap = new Map();
        rows.forEach((row: any) => {
          beatmapMap.set(row.beatmap_id, {
            beatmap_id: row.beatmap_id,
            title: row.title,
            artist: row.artist,
            version: row.version,
            creator: row.creator,
            difficulty: row.difficulty,
            bpm: row.bpm,
            total_length: row.total_length,
            mode: row.mode,
            status: row.status
          });
        });

        const results = beatmap_ids.map((id: number) => beatmapMap.get(id) || null);

        return { beatmaps: results };
      } finally {
        connection.close();
      }
    }),

  /**
   * Get user by ID
   * Mirrors GET /api/user/:id
   */
  getUser: publicProcedure
    .input(GetUserInput)
    .query(async ({ input }) => {
      const { user_id } = input;

      const connection = await db.getConnection();

      try {
        const userRows = await connection.query(
          `
            SELECT 
              user_id,
              username,
              pp_raw,
              accuracy,
              playcount,
              level
            FROM users
            WHERE user_id = ?
          `,
          [user_id]
        );

        if (userRows.length === 0) {
          throw new Error('User not found');
        }

        const user = userRows[0];

        const playsRows = await connection.query(
          `
            SELECT 
              ub.beatmap_id,
              b.title,
              b.artist,
              b.version,
              ub.pp,
              ub.mods,
              ub.accuracy,
              ub.score,
              ub.playcount
            FROM user_beatmap_playcount ub
            JOIN beatmaps b ON ub.beatmap_id = b.beatmap_id
            WHERE ub.user_id = ?
            ORDER BY ub.pp DESC
            LIMIT 50
          `,
          [user_id]
        );

        const totalPlays = playsRows.length;
        const avgPp = totalPlays > 0 
          ? playsRows.reduce((sum: number, row: any) => sum + (row.pp as number), 0) / totalPlays 
          : 0;

        return {
          user_id: Number(user.user_id),
          stats: {
            total_plays: totalPlays,
            average_pp: Math.round(avgPp * 100) / 100,
            peak_rank: null
          },
          top_plays: playsRows.map((row: any) => ({
            beatmap_id: Number(row.beatmap_id),
            title: row.title,
            artist: row.artist,
            version: row.version,
            pp: row.pp,
            mods: row.mods,
            accuracy: row.accuracy,
            score: Number(row.score),
            playcount: Number(row.playcount)
          }))
        };
      } finally {
        connection.close();
      }
    }),
});

// Export type for client usage
export type RecommenderRouter = typeof recommenderRouter;

import express, { Express } from 'express';
import cors from 'cors';
import * as path from 'path';
import { Database, getGlobalDatabase } from './db';

(BigInt.prototype as any).toJSON = function() {
  return Number(this);
};

const app: Express = express();
const PORT = process.env.PORT || 3000;

app.use(cors());
app.use(express.json());

const DB_PATH = process.env.DUCKDB_PATH || path.resolve(__dirname, '../../../data/warehouse/2026-02/osu.duckdb');

// Initialize global database instance
const db = getGlobalDatabase(DB_PATH);

function calculateMedian(sortedValues: number[]): number {
  if (sortedValues.length === 0) return 0;
  const mid = Math.floor(sortedValues.length / 2);
  if (sortedValues.length % 2 === 0) {
    return (sortedValues[mid - 1] + sortedValues[mid]) / 2;
  }
  return sortedValues[mid];
}

app.get('/health', async (req, res) => {
  try {
    await db.connect();
    res.json({ status: 'ok', database: 'connected' });
  } catch (error) {
    res.json({ status: 'ok', database: 'disconnected' });
  }
});

app.post('/api/cohort', async (req, res) => {
  try {
    const { beatmap_id, pp_lower = 0, pp_upper = 10000, mods, top_k = 200 } = req.body;

    if (!beatmap_id) {
      return res.status(400).json({ error: 'Missing required field: beatmap_id' });
    }

    if (typeof beatmap_id !== 'number') {
      return res.status(400).json({ error: 'Invalid beatmap_id' });
    }

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
          max_combo,
          json_extract(data, '$.statistics.great') as count300,
          json_extract(data, '$.statistics.ok') as count100,
          json_extract(data, '$.statistics.meh') as count50,
          json_extract(data, '$.statistics.miss') as countmiss,
          json_extract(data, '$.statistics.large_tick_miss') as count_slider_breaks
        FROM mart_user_topk
        WHERE beatmap_id = ?
          AND pp BETWEEN ? AND ?
      `;

      const params: (number | string)[] = [beatmap_id, pp_lower, pp_upper];
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
        return res.status(404).json({ error: 'Beatmap not found' });
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
        return acc * 100; // Convert from [0,1] to [0,100]
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
        username: String(row.user_id),
        pp: Number(row.pp),
        accuracy: Math.round((Number(row.accuracy) || 0.95) * 100 * 100) / 100
      }));

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

      const uniqueUserIds = new Set(rows.map((row: any) => row.user_id)).size;

      res.json({
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
      });
    } finally {
      connection.close();
    }
  } catch (error) {
    console.error('Error in /api/cohort:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.post('/api/beatmap-plays', async (req, res) => {
  try {
    const { beatmap_id, mods, top_k = 200 } = req.body;

    if (!beatmap_id) {
      return res.status(400).json({ error: 'Missing required field: beatmap_id' });
    }

    if (typeof beatmap_id !== 'number') {
      return res.status(400).json({ error: 'Invalid beatmap_id' });
    }

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
          max_combo,
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
        return res.status(404).json({ error: 'Beatmap not found' });
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

      res.json({
        beatmap_id,
        total_plays: rows.length,
        all_plays: allPlays
      });
    } finally {
      connection.close();
    }
  } catch (error) {
    console.error('Error in /api/beatmap-plays:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.post('/api/recommend', async (req, res) => {
  try {
    const { beatmap_id, pp_lower = 0, pp_upper = 10000, mods, limit = 10 } = req.body;

    if (!beatmap_id) {
      return res.status(400).json({ error: 'Missing required field: beatmap_id' });
    }

    if (typeof beatmap_id !== 'number') {
      return res.status(400).json({ error: 'Invalid beatmap_id' });
    }

    const connection = await db.getConnection();

    try {
      const beatmapCheckRows = await connection.query(
        'SELECT beatmap_id FROM beatmaps WHERE beatmap_id = ?',
        [beatmap_id]
      );

      if (beatmapCheckRows.length === 0) {
        return res.status(404).json({ error: 'Beatmap not found' });
      }

      let cohortQuery = `
        SELECT DISTINCT user_id
        FROM user_beatmap_playcount
        WHERE beatmap_id = ?
          AND pp BETWEEN ? AND ?
      `;

      const cohortParams: (number | string)[] = [beatmap_id, pp_lower, pp_upper];
      const modsArray = mods !== undefined && mods !== null ? mods : [];
      const availableMods = ['HD', 'HR', 'DT', 'FL', 'EZ', 'HT', 'NC'];
      
      if (modsArray.length > 0) {
        const filteredMods = modsArray.filter((m: string) => m !== 'CL').sort();
        if (filteredMods.length === 1) {
          const mod = filteredMods[0];
          cohortQuery += ` AND (mods = ? OR mods LIKE ? OR mods LIKE ?)`;
          cohortParams.push(
            JSON.stringify([{ acronym: mod }]),
            `%${JSON.stringify([{ acronym: mod }])}%`,
            `%${JSON.stringify([{ acronym: mod }, { acronym: "CL" }])}%`
          );
        } else {
          const modsJson = JSON.stringify(filteredMods.map((m: string) => ({ acronym: m })));
          const modsWithCl = JSON.stringify([...filteredMods.map((m: string) => ({ acronym: m })), { acronym: "CL" }]);
          cohortQuery += ` AND (mods = ? OR mods LIKE ?)`;
          cohortParams.push(modsJson, `%${modsWithCl}%`);
        }
      } else {
        for (const mod of availableMods) {
          cohortQuery += ` AND mods NOT LIKE ?`;
          cohortParams.push(`%acronym":"${mod}"%`);
        }
      }

      const cohortRows = await connection.query(cohortQuery, cohortParams);

      if (cohortRows.length === 0) {
        return res.json({
          beatmap_id,
          total: 0,
          recommendations: []
        });
      }

      const userIds = cohortRows.map((row: any) => row.user_id as number);

      const placeholders = userIds.map(() => '?').join(',');
      let recommendQuery = `
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
          AVG(ub.accuracy) as avg_accuracy
        FROM user_beatmap_playcount ub
        JOIN beatmaps b ON ub.beatmap_id = b.beatmap_id
        WHERE ub.user_id IN (${placeholders})
          AND ub.beatmap_id != ?
          AND ub.pp BETWEEN ? AND ?
        GROUP BY ub.beatmap_id, b.beatmapset_id, b.title, b.artist, b.version, b.creator,
                 b.difficulty_rating, b.bpm, b.total_length
        ORDER BY play_count DESC, avg_pp DESC
        LIMIT ?
      `;

      const recommendParams = [
        ...userIds,
        beatmap_id,
        pp_lower,
        pp_upper,
        limit
      ];

      const recommendRows = await connection.query(recommendQuery, recommendParams);

      res.json({
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
          similarity_score: Math.round(Number(row.play_count) / userIds.length * 100) / 100
        }))
      });
    } finally {
      connection.close();
    }
  } catch (error) {
    console.error('Error in /api/recommend:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.post('/api/beatmaps', async (req, res) => {
  try {
    const { beatmap_ids } = req.body;

    if (!Array.isArray(beatmap_ids)) {
      return res.status(400).json({ error: 'beatmap_ids must be an array' });
    }

    if (beatmap_ids.length === 0) {
      return res.status(400).json({ error: 'beatmap_ids cannot be empty' });
    }

    if (beatmap_ids.length > 100) {
      return res.status(413).json({ error: 'Batch size exceeds maximum of 100' });
    }

    if (!beatmap_ids.every((id: any) => typeof id === 'number')) {
      return res.status(400).json({ error: 'Invalid beatmap_id in array' });
    }

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

      res.json({ beatmaps: results });
    } finally {
      connection.close();
    }
  } catch (error) {
    console.error('Error in /api/beatmaps:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.get('/api/user/:id', async (req, res) => {
  try {
    const userId = parseInt(req.params.id);

    if (isNaN(userId)) {
      return res.status(400).json({ error: 'Invalid user ID' });
    }

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
        [userId]
      );

      if (userRows.length === 0) {
        return res.status(404).json({ error: 'User not found' });
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
        [userId]
      );

      const totalPlays = playsRows.length;
      const avgPp = totalPlays > 0 
        ? playsRows.reduce((sum: number, row: any) => sum + (row.pp as number), 0) / totalPlays 
        : 0;

      res.json({
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
      });
    } finally {
      connection.close();
    }
  } catch (error) {
    console.error('Error in /api/user/:id:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.use((err: any, req: express.Request, res: express.Response, next: express.NextFunction) => {
  if (err instanceof SyntaxError && 'body' in err) {
    return res.status(400).json({ error: 'Invalid JSON' });
  }
  console.error('Unhandled error:', err);
  res.status(500).json({ error: 'Internal server error' });
});

app.use((req, res) => {
  res.status(404).json({ error: 'Not found' });
});

if (require.main === module) {
  app.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
  });
}

export default app;
export { Database, db };

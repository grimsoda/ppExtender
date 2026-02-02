import express from 'express';
import cors from 'cors';
import * as path from 'path';
import * as fs from 'fs';

const app = express();
const PORT = process.env.PORT || 3000;

app.use(cors());
app.use(express.json());

const DB_PATH = process.env.DUCKDB_PATH || 'data/warehouse/2026-02/osu.duckdb';
let dbInstance: any = null;

async function getDb() {
  if (!dbInstance) {
    const absolutePath = path.resolve(DB_PATH);
    if (!fs.existsSync(absolutePath)) {
      throw new Error(`Database not found at ${absolutePath}`);
    }
    let duckdb;
    if (typeof jest !== 'undefined' && (jest as any).requireActual) {
      duckdb = (jest as any).requireActual('@duckdb/node-api');
    } else {
      duckdb = await import('@duckdb/node-api');
    }
    dbInstance = await duckdb.DuckDBInstance.create(absolutePath);
  }
  return dbInstance;
}

async function getConnection() {
  const instance = await getDb();
  return await instance.connect();
}

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
    await getDb();
    res.json({ status: 'ok', database: 'connected' });
  } catch (error) {
    res.json({ status: 'ok', database: 'disconnected' });
  }
});

app.post('/api/cohort', async (req, res) => {
  try {
    const { beatmap_id, pp_lower = 0, pp_upper = 10000, mods } = req.body;

    if (!beatmap_id) {
      return res.status(400).json({ error: 'Missing required field: beatmap_id' });
    }

    if (typeof beatmap_id !== 'number') {
      return res.status(400).json({ error: 'Invalid beatmap_id' });
    }

    const connection = await getConnection();

    try {
      let query = `
        SELECT 
          user_id,
          pp,
          mods,
          accuracy,
          score
        FROM user_beatmap_playcount
        WHERE beatmap_id = ?
          AND pp BETWEEN ? AND ?
      `;

      const params: (number | string)[] = [beatmap_id, pp_lower, pp_upper];

      if (mods !== undefined && mods !== null && Array.isArray(mods) && mods.length > 0) {
        query += ' AND mods = ?';
        params.push(mods.join(','));
      }

      query += ' ORDER BY pp DESC';

      const result = await connection.run(query, params);
      const rows = await result.getRowObjects();

      if (rows.length === 0) {
        return res.status(404).json({ error: 'Beatmap not found' });
      }

      const ppValues = rows.map((row: any) => row.pp as number).sort((a: number, b: number) => a - b);
      const minPp = ppValues[0] || 0;
      const maxPp = ppValues[ppValues.length - 1] || 0;
      const meanPp = ppValues.length > 0 
        ? ppValues.reduce((a: number, b: number) => a + b, 0) / ppValues.length 
        : 0;
      const medianPp = calculateMedian(ppValues);

      res.json({
        beatmap_id,
        cohort_size: rows.length,
        pp_distribution: {
          min: Math.round(minPp * 100) / 100,
          max: Math.round(maxPp * 100) / 100,
          mean: Math.round(meanPp * 100) / 100,
          median: Math.round(medianPp * 100) / 100
        }
      });
    } finally {
      await connection.closeSync();
    }
  } catch (error) {
    console.error('Error in /api/cohort:', error);
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

    const connection = await getConnection();

    try {
      const beatmapCheckResult = await connection.run(
        'SELECT beatmap_id FROM beatmaps WHERE beatmap_id = ?',
        [beatmap_id]
      );
      const beatmapCheckRows = await beatmapCheckResult.getRowObjects();

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

      if (mods !== undefined && mods !== null && Array.isArray(mods) && mods.length > 0) {
        cohortQuery += ' AND mods = ?';
        cohortParams.push(mods.join(','));
      }

      const cohortResult = await connection.run(cohortQuery, cohortParams);
      const cohortRows = await cohortResult.getRowObjects();

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
        GROUP BY ub.beatmap_id, b.title, b.artist, b.version, b.creator, 
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

      const recommendResult = await connection.run(recommendQuery, recommendParams);
      const recommendRows = await recommendResult.getRowObjects();

      res.json({
        beatmap_id,
        total: recommendRows.length,
        recommendations: recommendRows.map((row: any) => ({
          beatmap_id: row.beatmap_id,
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
      await connection.closeSync();
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

    const connection = await getConnection();

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

      const result = await connection.run(query, beatmap_ids);
      const rows = await result.getRowObjects();

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
      await connection.closeSync();
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

    const connection = await getConnection();

    try {
      const userQuery = `
        SELECT 
          user_id,
          username,
          pp_raw,
          accuracy,
          playcount,
          level
        FROM users
        WHERE user_id = ?
      `;

      const userResult = await connection.run(userQuery, [userId]);
      const userRows = await userResult.getRowObjects();

      if (userRows.length === 0) {
        return res.status(404).json({ error: 'User not found' });
      }

      const user = userRows[0];

      const playsQuery = `
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
      `;

      const playsResult = await connection.run(playsQuery, [userId]);
      const playsRows = await playsResult.getRowObjects();

      const totalPlays = playsRows.length;
      const avgPp = totalPlays > 0 
        ? playsRows.reduce((sum: number, row: any) => sum + (row.pp as number), 0) / totalPlays 
        : 0;

      res.json({
        user_id: user.user_id,
        stats: {
          total_plays: totalPlays,
          average_pp: Math.round(avgPp * 100) / 100,
          peak_rank: null
        },
        top_plays: playsRows.map((row: any) => ({
          beatmap_id: row.beatmap_id,
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
      await connection.closeSync();
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

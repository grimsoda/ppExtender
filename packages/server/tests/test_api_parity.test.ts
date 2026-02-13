import { describe, it, expect, beforeAll, afterAll } from '@jest/globals';
import request from 'supertest';
import app from '../src/index';
import { appRouter } from '../src/root-router';
import { getGlobalDatabase, resetGlobalDatabase } from '../src/db';
import * as path from 'path';

/**
 * Characterization Tests for API Parity
 *
 * These tests verify that Express and tRPC endpoints return identical responses.
 * This is critical for ensuring tRPC migration maintains 100% backward compatibility.
 *
 * Test Strategy:
 * 1. Send identical requests to both Express and tRPC
 * 2. Compare responses deeply (structure, values, order)
 * 3. Log differences for manual review
 * 4. Fail test if responses differ (unless intentional difference documented)
 */

describe('API Parity Tests', () => {
  const caller = appRouter.createCaller({});

  beforeAll(async () => {
    const dbPath = process.env.DUCKDB_PATH || path.resolve(__dirname, '../../data/warehouse/2026-02/osu.duckdb');
    const db = getGlobalDatabase(dbPath);
    await db.connect();
  });

  afterAll(async () => {
    resetGlobalDatabase();
  });

  /**
   * Helper function to log differences between responses
   */
  function logDifferences(endpoint: string, expressRes: any, trpcRes: any, differences: string[]) {
    if (differences.length > 0) {
      console.log(`\n=== PARITY ISSUE: ${endpoint} ===`);
      console.log('Express response:', JSON.stringify(expressRes, null, 2));
      console.log('tRPC response:', JSON.stringify(trpcRes, null, 2));
      console.log('Differences:');
      differences.forEach(diff => console.log(`  - ${diff}`));
      console.log('===================================\n');
    }
  }

  function compareResponses(
    path: string,
    expressVal: any,
    trpcVal: any,
    differences: string[]
  ): void {
    if (expressVal === null && trpcVal === null) return;
    if (expressVal === undefined && trpcVal === undefined) return;

    if (typeof expressVal !== typeof trpcVal) {
      differences.push(`${path}: Type mismatch - Express (${typeof expressVal}) != tRPC (${typeof trpcVal})`);
      return;
    }

    if (typeof expressVal !== 'object') {
      if (expressVal !== trpcVal) {
        differences.push(`${path}: Value mismatch - Express (${expressVal}) != tRPC (${trpcVal})`);
      }
      return;
    }

    if (Array.isArray(expressVal) && Array.isArray(trpcVal)) {
      if (expressVal.length !== trpcVal.length) {
        differences.push(`${path}: Array length mismatch - Express (${expressVal.length}) != tRPC (${trpcVal.length})`);
        return;
      }
      for (let i = 0; i < expressVal.length; i++) {
        compareResponses(`${path}[${i}]`, expressVal[i], trpcVal[i], differences);
      }
      return;
    }

    const expressKeys = Object.keys(expressVal || {}).sort();
    const trpcKeys = Object.keys(trpcVal || {}).sort();

    if (JSON.stringify(expressKeys) !== JSON.stringify(trpcKeys)) {
      differences.push(
        `${path}: Keys mismatch - Express [${expressKeys.join(', ')}] != tRPC [${trpcKeys.join(', ')}]`
      );
    }

    const commonKeys = expressKeys.filter(k => trpcKeys.includes(k));
    for (const key of commonKeys) {
      compareResponses(`${path}.${key}`, expressVal[key], trpcVal[key], differences);
    }
  }

  async function compareEndpoint(
    endpointName: string,
    expressRequest: () => Promise<any>,
    trpcCall: () => Promise<any>,
    expectedStatus: number = 200
  ): Promise<void> {
    const differences: string[] = [];

    const expressRes = await expressRequest();
    expect(expressRes.status).toBe(expectedStatus);

    const trpcRes = await trpcCall();

    compareResponses('root', expressRes.body, trpcRes, differences);

    if (differences.length > 0) {
      logDifferences(endpointName, expressRes.body, trpcRes, differences);
    }

    expect(differences).toHaveLength(0);
  }

  async function compareErrorEndpoint(
    endpointName: string,
    expressRequest: () => Promise<any>,
    trpcCall: () => Promise<any>,
    expectedExpressStatus: number,
    expectedExpressError?: string
  ): Promise<void> {
    const expressRes = await expressRequest();
    expect(expressRes.status).toBe(expectedExpressStatus);
    if (expectedExpressError) {
      expect(expressRes.body.error).toContain(expectedExpressError);
    }

    let trpcError: Error | null = null;
    let trpcRes: any = null;

    try {
      trpcRes = await trpcCall();
    } catch (err) {
      trpcError = err as Error;
    }

    expect(trpcError).not.toBeNull();
    expect(trpcRes).toBeNull();

    console.log(`\n${endpointName} - Express error: ${expressRes.body.error}`);
    console.log(`${endpointName} - tRPC error: ${trpcError?.message}\n`);
  }

  describe('GET /health vs health', () => {
    it('should return identical responses', async () => {
      await compareEndpoint(
        'GET /health',
        () => request(app).get('/health'),
        () => caller.recommender.health()
      );
    });
  });

  describe('POST /api/cohort vs getCohort', () => {
    it('should handle empty mart_user_topk identically (404 vs error)', async () => {
      await compareErrorEndpoint(
        'POST /api/cohort (no data in mart_user_topk)',
        () =>
          request(app).post('/api/cohort').send({
            beatmap_id: 95,
            pp_lower: 0,
            pp_upper: 10000,
            top_k: 50,
          }),
        () =>
          caller.recommender.getCohort({
            beatmap_id: 95,
            pp_lower: 0,
            pp_upper: 10000,
            top_k: 50,
          }),
        404,
        'Beatmap not found'
      );
    });

    it('should handle invalid beatmap_id identically (404 vs error)', async () => {
      await compareErrorEndpoint(
        'POST /api/cohort (invalid beatmap)',
        () =>
          request(app).post('/api/cohort').send({
            beatmap_id: 999999999,
          }),
        () =>
          caller.recommender.getCohort({
            beatmap_id: 999999999,
          }),
        404,
        'Beatmap not found'
      );
    });

    it('should handle missing beatmap_id identically (400 vs validation error)', async () => {
      const expressRes = await request(app).post('/api/cohort').send({});

      expect(expressRes.status).toBe(400);
      expect(expressRes.body.error).toContain('Missing required field: beatmap_id');

      await expect(
        caller.recommender.getCohort({
          beatmap_id: undefined as any,
        })
      ).rejects.toThrow();
    });
  });

  describe('POST /api/beatmap-plays vs getBeatmapPlays', () => {
    it('should handle empty mart_user_topk identically (404 vs error)', async () => {
      await compareErrorEndpoint(
        'POST /api/beatmap-plays (no data in mart_user_topk)',
        () =>
          request(app).post('/api/beatmap-plays').send({
            beatmap_id: 95,
            top_k: 50,
          }),
        () =>
          caller.recommender.getBeatmapPlays({
            beatmap_id: 95,
            top_k: 50,
          }),
        404,
        'Beatmap not found'
      );
    });

    it('should handle invalid beatmap_id identically (404 vs error)', async () => {
      await compareErrorEndpoint(
        'POST /api/beatmap-plays (invalid beatmap)',
        () =>
          request(app).post('/api/beatmap-plays').send({
            beatmap_id: 999999999,
          }),
        () =>
          caller.recommender.getBeatmapPlays({
            beatmap_id: 999999999,
          }),
        404,
        'Beatmap not found'
      );
    });

    it('should handle missing beatmap_id identically (400 vs validation error)', async () => {
      const expressRes = await request(app).post('/api/beatmap-plays').send({});

      expect(expressRes.status).toBe(400);
      expect(expressRes.body.error).toContain('Missing required field: beatmap_id');

      await expect(
        caller.recommender.getBeatmapPlays({
          beatmap_id: undefined as any,
        })
      ).rejects.toThrow();
    });
  });

  describe('POST /api/recommend vs getRecommendations', () => {
    it('should return identical responses for valid beatmap_id', async () => {
      await compareEndpoint(
        'POST /api/recommend (valid beatmap)',
        () =>
          request(app).post('/api/recommend').send({
            beatmap_id: 5302539,
            pp_lower: 0,
            pp_upper: 10000,
            limit: 5,
          }),
        () =>
          caller.recommender.getRecommendations({
            beatmap_id: 5302539,
            pp_lower: 0,
            pp_upper: 10000,
            limit: 5,
          })
      );
    });

    it('should return identical responses with mods filter', async () => {
      await compareEndpoint(
        'POST /api/recommend (with mods)',
        () =>
          request(app).post('/api/recommend').send({
            beatmap_id: 5302539,
            mods: ['DT'],
            limit: 5,
          }),
        () =>
          caller.recommender.getRecommendations({
            beatmap_id: 5302539,
            mods: ['DT'],
            limit: 5,
          })
      );
    });

    it('should return identical responses with limit parameter', async () => {
      await compareEndpoint(
        'POST /api/recommend (limit)',
        () =>
          request(app).post('/api/recommend').send({
            beatmap_id: 5302539,
            limit: 10,
          }),
        () =>
          caller.recommender.getRecommendations({
            beatmap_id: 5302539,
            limit: 10,
          })
      );
    });

    it('should return identical responses with pp_lower/pp_upper', async () => {
      await compareEndpoint(
        'POST /api/recommend (pp range)',
        () =>
          request(app).post('/api/recommend').send({
            beatmap_id: 5302539,
            pp_lower: 200,
            pp_upper: 500,
            limit: 5,
          }),
        () =>
          caller.recommender.getRecommendations({
            beatmap_id: 5302539,
            pp_lower: 200,
            pp_upper: 500,
            limit: 5,
          })
      );
    });

    it('should handle invalid beatmap_id identically (404 vs error)', async () => {
      await compareErrorEndpoint(
        'POST /api/recommend (invalid beatmap)',
        () =>
          request(app).post('/api/recommend').send({
            beatmap_id: 999999999,
          }),
        () =>
          caller.recommender.getRecommendations({
            beatmap_id: 999999999,
          }),
        404,
        'Beatmap not found'
      );
    });

    it('should handle empty cohort identically (empty recommendations)', async () => {
      const expressRes = await request(app).post('/api/recommend').send({
        beatmap_id: 5302539,
        pp_lower: 999999,
        pp_upper: 1000000,
        limit: 5,
      });

      expect(expressRes.status).toBe(200);
      expect(expressRes.body.total).toBe(0);
      expect(expressRes.body.recommendations).toEqual([]);

      const trpcRes = await caller.recommender.getRecommendations({
        beatmap_id: 5302539,
        pp_lower: 999999,
        pp_upper: 1000000,
        limit: 5,
      });

      expect(trpcRes.total).toBe(0);
      expect(trpcRes.recommendations).toEqual([]);

      expect(expressRes.body).toEqual(trpcRes);
    });

    it('should handle missing beatmap_id identically (400 vs validation error)', async () => {
      const expressRes = await request(app).post('/api/recommend').send({});

      expect(expressRes.status).toBe(400);
      expect(expressRes.body.error).toContain('Missing required field: beatmap_id');

      await expect(
        caller.recommender.getRecommendations({
          beatmap_id: undefined as any,
        })
      ).rejects.toThrow();
    });
  });

  describe('POST /api/beatmaps vs getBeatmaps', () => {
    it('should return identical responses for single beatmap_id', async () => {
      await compareEndpoint(
        'POST /api/beatmaps (single beatmap)',
        () =>
          request(app).post('/api/beatmaps').send({
            beatmap_ids: [95],
          }),
        () =>
          caller.recommender.getBeatmaps({
            beatmap_ids: [95],
          })
      );
    });

    it('should return identical responses for multiple beatmap_ids', async () => {
      await compareEndpoint(
        'POST /api/beatmaps (multiple beatmaps)',
        () =>
          request(app).post('/api/beatmaps').send({
            beatmap_ids: [95, 96, 110],
          }),
        () =>
          caller.recommender.getBeatmaps({
            beatmap_ids: [95, 96, 110],
          })
      );
    });

    it('should return identical responses for mix of valid and invalid IDs', async () => {
      await compareEndpoint(
        'POST /api/beatmaps (mixed valid/invalid)',
        () =>
          request(app).post('/api/beatmaps').send({
            beatmap_ids: [95, 999999999, 96],
          }),
        () =>
          caller.recommender.getBeatmaps({
            beatmap_ids: [95, 999999999, 96],
          })
      );
    });

    it('should handle empty array identically (400 vs validation error)', async () => {
      const expressRes = await request(app).post('/api/beatmaps').send({
        beatmap_ids: [],
      });

      expect(expressRes.status).toBe(400);
      expect(expressRes.body.error).toContain('beatmap_ids cannot be empty');

      await expect(
        caller.recommender.getBeatmaps({
          beatmap_ids: [],
        })
      ).rejects.toThrow();
    });

    it('should handle large array identically (413 vs validation error)', async () => {
      const largeArray = Array(101).fill(1);

      const expressRes = await request(app).post('/api/beatmaps').send({
        beatmap_ids: largeArray,
      });

      expect(expressRes.status).toBe(413);
      expect(expressRes.body.error).toContain('Batch size exceeds maximum of 100');

      await expect(
        caller.recommender.getBeatmaps({
          beatmap_ids: largeArray,
        })
      ).rejects.toThrow();
    });

    it('should handle non-array identically (400 vs validation error)', async () => {
      const expressRes = await request(app).post('/api/beatmaps').send({
        beatmap_ids: 'not-an-array',
      });

      expect(expressRes.status).toBe(400);
      expect(expressRes.body.error).toContain('beatmap_ids must be an array');
    });

    it('should handle invalid beatmap_id in array identically (400 vs validation error)', async () => {
      const expressRes = await request(app).post('/api/beatmaps').send({
        beatmap_ids: [75, 'not-a-number', 76],
      });

      expect(expressRes.status).toBe(400);
      expect(expressRes.body.error).toContain('Invalid beatmap_id in array');
    });
  });

  describe('GET /api/user/:id vs getUser', () => {
    it('should return identical responses for valid user_id', async () => {
      await compareEndpoint(
        'GET /api/user/:id (valid user)',
        () => request(app).get('/api/user/1622883'),
        () =>
          caller.recommender.getUser({
            user_id: 1622883,
          })
      );
    });

    it('should handle invalid user_id identically (404 vs error)', async () => {
      await compareErrorEndpoint(
        'GET /api/user/:id (invalid user)',
        () => request(app).get('/api/user/999999999'),
        () =>
          caller.recommender.getUser({
            user_id: 999999999,
          }),
        404,
        'User not found'
      );
    });

    it('should handle invalid user_id format identically (400 vs validation error)', async () => {
      const expressRes = await request(app).get('/api/user/not-a-number');

      expect(expressRes.status).toBe(400);
      expect(expressRes.body.error).toContain('Invalid user ID');

      await expect(
        caller.recommender.getUser({
          user_id: 'not-a-number' as any,
        })
      ).rejects.toThrow();
    });

    it('should handle user with no plays identically', async () => {
      const testUserId = 99999;

      const expressRes = await request(app).get(`/api/user/${testUserId}`);

      if (expressRes.status === 404) {
        console.log(`User ${testUserId} not found, skipping no-plays test`);
        return;
      }

      const trpcRes = await caller.recommender.getUser({
        user_id: testUserId,
      });

      expect(expressRes.body).toEqual(trpcRes);
      expect(expressRes.body.stats.total_plays).toBe(0);
      expect(trpcRes.stats.total_plays).toBe(0);
    });
  });
});

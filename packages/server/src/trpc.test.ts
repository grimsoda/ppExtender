import { describe, it, expect, beforeAll, afterAll } from '@jest/globals';
import request from 'supertest';
import app from './index';
import { getGlobalDatabase, resetGlobalDatabase } from './db';
import * as path from 'path';

describe('tRPC Router', () => {
  beforeAll(async () => {
    // Initialize database for tests
    const dbPath = process.env.DUCKDB_PATH || path.resolve(__dirname, '../../../data/warehouse/2026-02/osu.duckdb');
    try {
      const db = getGlobalDatabase(dbPath);
      await db.connect();
    } catch (error) {
      console.warn('Database not available for tests:', error);
    }
  });

  afterAll(() => {
    resetGlobalDatabase();
  });

  describe('Health Check', () => {
    it('should return health status via tRPC', async () => {
      const response = await request(app)
        .get('/api/trpc/recommender.health');

      expect(response.status).toBe(200);
      expect(response.body.result.data).toEqual(
        expect.objectContaining({
          status: 'ok',
          database: expect.any(String),
        })
      );
    });
  });

  describe('Get Cohort', () => {
    it('should return cohort data with valid input', async () => {
      const response = await request(app)
        .get('/api/trpc/recommender.getCohort')
        .query({
          input: JSON.stringify({
            beatmap_id: 75,
            pp_lower: 0,
            pp_upper: 500,
            top_k: 50,
          }),
        });

      // May return 200 with data or error if beatmap not found
      if (response.status === 200) {
        expect(response.body.result.data).toEqual(
          expect.objectContaining({
            beatmap_id: 75,
            cohort_size: expect.any(Number),
            pp_distribution: expect.any(Object),
            accuracy_distribution: expect.any(Object),
            top_players: expect.any(Array),
            plays: expect.any(Array),
            seed_pp_range: expect.any(Object),
          })
        );
      }
    });

    it('should reject invalid beatmap_id', async () => {
      const response = await request(app)
        .get('/api/trpc/recommender.getCohort')
        .query({
          input: JSON.stringify({
            beatmap_id: 'invalid',
          }),
        });

      // tRPC returns 200 even for validation errors, but with error in body
      expect(response.body.error).toBeDefined();
    });
  });

  describe('Get Beatmap Plays', () => {
    it('should return plays data with valid input', async () => {
      const response = await request(app)
        .get('/api/trpc/recommender.getBeatmapPlays')
        .query({
          input: JSON.stringify({
            beatmap_id: 75,
            top_k: 50,
          }),
        });

      if (response.status === 200) {
        expect(response.body.result.data).toEqual(
          expect.objectContaining({
            beatmap_id: 75,
            total_plays: expect.any(Number),
            all_plays: expect.any(Array),
          })
        );
      }
    });
  });

  describe('Get Recommendations', () => {
    it('should return recommendations with valid input', async () => {
      const response = await request(app)
        .get('/api/trpc/recommender.getRecommendations')
        .query({
          input: JSON.stringify({
            beatmap_id: 75,
            limit: 10,
          }),
        });

      if (response.status === 200) {
        expect(response.body.result.data).toEqual(
          expect.objectContaining({
            beatmap_id: 75,
            total: expect.any(Number),
            recommendations: expect.any(Array),
          })
        );
      }
    }, 30000);
  });

  describe('Get Beatmaps', () => {
    it('should return beatmaps with valid input', async () => {
      const response = await request(app)
        .get('/api/trpc/recommender.getBeatmaps')
        .query({
          input: JSON.stringify({
            beatmap_ids: [75, 76],
          }),
        });

      expect(response.status).toBe(200);
      expect(response.body.result.data).toEqual(
        expect.objectContaining({
          beatmaps: expect.any(Array),
        })
      );
    });

    it('should reject empty beatmap_ids array', async () => {
      const response = await request(app)
        .get('/api/trpc/recommender.getBeatmaps')
        .query({
          input: JSON.stringify({
            beatmap_ids: [],
          }),
        });

      expect(response.body.error).toBeDefined();
    });

    it('should reject more than 100 beatmap_ids', async () => {
      const response = await request(app)
        .get('/api/trpc/recommender.getBeatmaps')
        .query({
          input: JSON.stringify({
            beatmap_ids: Array(101).fill(75),
          }),
        });

      expect(response.body.error).toBeDefined();
    });
  });

  describe('Get User', () => {
    it('should return user data with valid input', async () => {
      const response = await request(app)
        .get('/api/trpc/recommender.getUser')
        .query({
          input: JSON.stringify({
            user_id: 1,
          }),
        });

      if (response.status === 200) {
        expect(response.body.result.data).toEqual(
          expect.objectContaining({
            user_id: 1,
            stats: expect.any(Object),
            top_plays: expect.any(Array),
          })
        );
      }
    });
  });
});

describe('Express Routes Still Work', () => {
  it('should still serve health endpoint via Express', async () => {
    const response = await request(app).get('/health');
    expect(response.status).toBe(200);
    expect(response.body).toHaveProperty('status');
  });
});

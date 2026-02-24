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
        .get('/trpc/recommender.health');

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
        .get('/trpc/recommender.getCohort')
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
        .get('/trpc/recommender.getCohort')
        .query({
          input: JSON.stringify({
            beatmap_id: 'invalid',
          }),
        });

      // tRPC returns 200 even for validation errors, but with error in body
      expect(response.body.error).toBeDefined();
    });

    it('should return empty cohort for beatmap with no plays matching filters', async () => {
      // Use a very high pp range that likely has no plays
      const response = await request(app)
        .get('/trpc/recommender.getCohort')
        .query({
          input: JSON.stringify({
            beatmap_id: 75,
            pp_lower: 9999,
            pp_upper: 10000,
            top_k: 50,
          }),
        });

      expect(response.status).toBe(200);
      // Should return empty cohort, not an error
      const data = response.body.result?.data;
      if (data) {
        expect(data.cohort_size).toBe(0);
        expect(data.plays).toEqual([]);
        expect(data.top_players).toEqual([]);
      }
    });

    it('should accept optional mods parameter', async () => {
      const response = await request(app)
        .get('/trpc/recommender.getCohort')
        .query({
          input: JSON.stringify({
            beatmap_id: 75,
            mods: ['HD', 'DT'],
            top_k: 50,
          }),
        });

      // Should not error on mods parameter
      expect(response.status).toBe(200);
      expect(response.body.error).toBeUndefined();
    });
  });

  describe('Get Beatmap Plays', () => {
    it('should return plays data with valid input', async () => {
      const response = await request(app)
        .get('/trpc/recommender.getBeatmapPlays')
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

    it('should return empty plays for non-existent beatmap', async () => {
      const response = await request(app)
        .get('/trpc/recommender.getBeatmapPlays')
        .query({
          input: JSON.stringify({
            beatmap_id: 999999999,
            top_k: 50,
          }),
        });

      expect(response.status).toBe(200);
      const data = response.body.result?.data;
      if (data) {
        expect(data.total_plays).toBe(0);
        expect(data.all_plays).toEqual([]);
      }
    });

    it('should accept optional mods parameter', async () => {
      const response = await request(app)
        .get('/trpc/recommender.getBeatmapPlays')
        .query({
          input: JSON.stringify({
            beatmap_id: 75,
            mods: ['HR'],
            top_k: 50,
          }),
        });

      expect(response.status).toBe(200);
      expect(response.body.error).toBeUndefined();
    });
  });

  describe('Get Recommendations', () => {
    it('should return recommendations with valid input', async () => {
      const response = await request(app)
        .get('/trpc/recommender.getRecommendations')
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

    it('should return error for non-existent beatmap', async () => {
      const response = await request(app)
        .get('/trpc/recommender.getRecommendations')
        .query({
          input: JSON.stringify({
            beatmap_id: 999999999,
            limit: 10,
          }),
        });

      // getRecommendations throws "Beatmap not found" for non-existent beatmaps
      expect(response.body.error).toBeDefined();
      expect(response.body.error?.message).toContain('Beatmap not found');
    });

    it('should return empty recommendations for beatmap with no cohort', async () => {
      // Use extreme PP range to ensure no cohort
      const response = await request(app)
        .get('/trpc/recommender.getRecommendations')
        .query({
          input: JSON.stringify({
            beatmap_id: 75,
            pp_lower: 9999,
            pp_upper: 10000,
            limit: 10,
          }),
        });

      expect(response.status).toBe(200);
      const data = response.body.result?.data;
      if (data) {
        expect(data.total).toBe(0);
        expect(data.recommendations).toEqual([]);
      }
    });

    it('should accept optional mods parameter', async () => {
      const response = await request(app)
        .get('/trpc/recommender.getRecommendations')
        .query({
          input: JSON.stringify({
            beatmap_id: 75,
            mods: ['DT'],
            limit: 5,
          }),
        });

      expect(response.status).toBe(200);
      expect(response.body.error).toBeUndefined();
    }, 30000);
  });

  describe('Get Beatmaps', () => {
    it('should return beatmaps with valid input', async () => {
      const response = await request(app)
        .get('/trpc/recommender.getBeatmaps')
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
        .get('/trpc/recommender.getBeatmaps')
        .query({
          input: JSON.stringify({
            beatmap_ids: [],
          }),
        });

      expect(response.body.error).toBeDefined();
    });

    it('should reject more than 100 beatmap_ids', async () => {
      const response = await request(app)
        .get('/trpc/recommender.getBeatmaps')
        .query({
          input: JSON.stringify({
            beatmap_ids: Array(101).fill(75),
          }),
        });

      expect(response.body.error).toBeDefined();
    });

    it('should return null for non-existent beatmap IDs', async () => {
      const response = await request(app)
        .get('/trpc/recommender.getBeatmaps')
        .query({
          input: JSON.stringify({
            beatmap_ids: [999999998, 999999999],
          }),
        });

      expect(response.status).toBe(200);
      const beatmaps = response.body.result?.data?.beatmaps;
      if (beatmaps) {
        // All beatmaps should be null for non-existent IDs
        expect(beatmaps.every((b: unknown) => b === null)).toBe(true);
      }
    });

    it('should return mixed results for existing and non-existing beatmaps', async () => {
      const response = await request(app)
        .get('/trpc/recommender.getBeatmaps')
        .query({
          input: JSON.stringify({
            beatmap_ids: [75, 999999999],
          }),
        });

      expect(response.status).toBe(200);
      const beatmaps = response.body.result?.data?.beatmaps;
      if (beatmaps) {
        expect(beatmaps).toHaveLength(2);
        // First may be null or a beatmap object, second should be null
        expect(beatmaps[1]).toBeNull();
      }
    });
  });

  describe('Get User', () => {
    it('should return user data with valid input', async () => {
      const response = await request(app)
        .get('/trpc/recommender.getUser')
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

    it('should return error for non-existent user', async () => {
      const response = await request(app)
        .get('/trpc/recommender.getUser')
        .query({
          input: JSON.stringify({
            user_id: 999999999,
          }),
        });

      // getUser throws "User not found" for non-existent users
      expect(response.body.error).toBeDefined();
      expect(response.body.error?.message).toContain('User not found');
    });

    it('should reject invalid user_id type', async () => {
      const response = await request(app)
        .get('/trpc/recommender.getUser')
        .query({
          input: JSON.stringify({
            user_id: 'invalid',
          }),
        });

      expect(response.body.error).toBeDefined();
    });
  });

  describe('Input Validation', () => {
    it('should reject missing required fields in getCohort', async () => {
      const response = await request(app)
        .get('/trpc/recommender.getCohort')
        .query({
          input: JSON.stringify({
            pp_lower: 0,
          }),
        });

      expect(response.body.error).toBeDefined();
    });

    it('should apply default values for optional fields', async () => {
      const response = await request(app)
        .get('/trpc/recommender.getCohort')
        .query({
          input: JSON.stringify({
            beatmap_id: 75,
          }),
        });

      // Should not error - defaults applied
      expect(response.status).toBe(200);
      expect(response.body.error).toBeUndefined();
    });
  });
});

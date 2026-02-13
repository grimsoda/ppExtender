import { describe, it, expect, beforeAll, afterAll } from '@jest/globals';
import request from 'supertest';
import app from './index';

describe('tRPC Router', () => {
  describe('Health Check', () => {
    it('should return health status via tRPC', async () => {
      const response = await request(app)
        .get('/api/trpc/recommender.health');

      expect(response.status).toBe(200);
      expect(response.body.result.data).toEqual(
        expect.objectContaining({
          status: 'ok',
          trpc: true,
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
            beatmap_id: 12345,
            pp_lower: 0,
            pp_upper: 500,
          }),
        });

      expect(response.status).toBe(200);
      expect(response.body.result.data).toEqual(
        expect.objectContaining({
          beatmap_id: 12345,
          cohort_size: 0,
          pp_distribution: expect.any(Object),
          _trpc: true,
        })
      );
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
            beatmap_id: 12345,
          }),
        });

      expect(response.status).toBe(200);
      expect(response.body.result.data).toEqual(
        expect.objectContaining({
          beatmap_id: 12345,
          total_plays: 0,
          _trpc: true,
        })
      );
    });
  });

  describe('Get Recommendations', () => {
    it('should return recommendations with valid input', async () => {
      const response = await request(app)
        .get('/api/trpc/recommender.getRecommendations')
        .query({
          input: JSON.stringify({
            beatmap_id: 12345,
            limit: 10,
          }),
        });

      expect(response.status).toBe(200);
      expect(response.body.result.data).toEqual(
        expect.objectContaining({
          beatmap_id: 12345,
          total: 0,
          recommendations: [],
          _trpc: true,
        })
      );
    });
  });

  describe('Get Beatmaps', () => {
    it('should return beatmaps with valid input', async () => {
      const response = await request(app)
        .get('/api/trpc/recommender.getBeatmaps')
        .query({
          input: JSON.stringify({
            beatmap_ids: [12345, 67890],
          }),
        });

      expect(response.status).toBe(200);
      expect(response.body.result.data).toEqual(
        expect.objectContaining({
          beatmaps: expect.any(Array),
          _trpc: true,
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
            beatmap_ids: Array(101).fill(12345),
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
            user_id: 12345,
          }),
        });

      expect(response.status).toBe(200);
      expect(response.body.result.data).toEqual(
        expect.objectContaining({
          user_id: 12345,
          stats: expect.any(Object),
          _trpc: true,
        })
      );
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

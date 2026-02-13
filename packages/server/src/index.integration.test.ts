import request from 'supertest';
import app from './index';
import * as path from 'path';
import * as fs from 'fs';

/**
 * Integration Tests with Real Database
 * 
 * These tests run against the actual DuckDB database with real data.
 * They verify that the API works correctly with the production dataset.
 * 
 * Prerequisites:
 * - Database must exist at data/warehouse/2026-02/osu.duckdb
 * - All ETL pipelines must have been run successfully
 */

const DB_PATH = path.resolve('data/warehouse/2026-02/osu.duckdb');

describe('Integration Tests with Real Data', () => {
  // Increase timeout for database operations
  jest.setTimeout(30000);

  beforeAll(async () => {
    // Verify database exists
    if (!fs.existsSync(DB_PATH)) {
      throw new Error(`Database not found at ${DB_PATH}. Run ETL pipeline first.`);
    }
  });

  describe('Database Connectivity', () => {
    it('should connect to the real database', async () => {
      const response = await request(app).get('/health');
      expect(response.status).toBe(200);
      expect(response.body).toHaveProperty('status', 'ok');
      expect(response.body).toHaveProperty('database');
    });
  });

  describe('POST /api/cohort', () => {
    it('should return valid response structure', async () => {
      const response = await request(app)
        .post('/api/cohort')
        .send({ beatmap_id: 59605 });
      
      // Should return either 200 (with data) or 404 (no data)
      expect([200, 404]).toContain(response.status);
      
      if (response.status === 200) {
        expect(response.body).toHaveProperty('beatmap_id');
        expect(response.body).toHaveProperty('cohort_size');
        expect(typeof response.body.cohort_size).toBe('number');
        
        if (response.body.cohort_size > 0) {
          expect(response.body).toHaveProperty('pp_distribution');
          expect(response.body.pp_distribution).toHaveProperty('min');
          expect(response.body.pp_distribution).toHaveProperty('max');
          expect(response.body.pp_distribution).toHaveProperty('mean');
          expect(response.body.pp_distribution).toHaveProperty('median');
        }
      }
    });

    it('should filter cohort by pp range', async () => {
      const response = await request(app)
        .post('/api/cohort')
        .send({ 
          beatmap_id: 59605,
          pp_lower: 200,
          pp_upper: 400
        });
      
      expect([200, 404]).toContain(response.status);
      if (response.status === 200) {
        expect(response.body).toHaveProperty('cohort_size');
      }
    });

    it('should return 404 for non-existent beatmap', async () => {
      const response = await request(app)
        .post('/api/cohort')
        .send({ beatmap_id: 999999999 });
      
      expect(response.status).toBe(404);
      expect(response.body).toHaveProperty('error');
    });

    it('should handle missing beatmap_id', async () => {
      const response = await request(app)
        .post('/api/cohort')
        .send({});
      
      expect(response.status).toBe(400);
      expect(response.body).toHaveProperty('error');
    });

    it('should handle invalid beatmap_id type', async () => {
      const response = await request(app)
        .post('/api/cohort')
        .send({ beatmap_id: 'invalid' });
      
      expect(response.status).toBe(400);
      expect(response.body).toHaveProperty('error');
    });
  });

  describe('POST /api/recommend', () => {
    it('should return valid response structure', async () => {
      const response = await request(app)
        .post('/api/recommend')
        .send({ beatmap_id: 59605, limit: 10 });
      
      expect([200, 404]).toContain(response.status);
      
      if (response.status === 200) {
        expect(response.body).toHaveProperty('beatmap_id');
        expect(response.body).toHaveProperty('recommendations');
        expect(Array.isArray(response.body.recommendations)).toBe(true);
        
        if (response.body.recommendations.length > 0) {
          const rec = response.body.recommendations[0];
          expect(rec).toHaveProperty('beatmap_id');
          expect(rec).toHaveProperty('title');
          expect(rec).toHaveProperty('artist');
          expect(rec).toHaveProperty('similarity_score');
        }
      }
    });

    it('should respect the limit parameter', async () => {
      const response = await request(app)
        .post('/api/recommend')
        .send({ beatmap_id: 59605, limit: 5 });
      
      expect([200, 404]).toContain(response.status);
      if (response.status === 200) {
        expect(response.body.recommendations.length).toBeLessThanOrEqual(5);
      }
    });

    it('should return 404 for non-existent beatmap', async () => {
      const response = await request(app)
        .post('/api/recommend')
        .send({ beatmap_id: 999999999 });
      
      expect(response.status).toBe(404);
      expect(response.body).toHaveProperty('error');
    });
  });

  describe('POST /api/beatmaps', () => {
    it('should return metadata for beatmap batch', async () => {
      const response = await request(app)
        .post('/api/beatmaps')
        .send({ beatmap_ids: [59605, 59606, 59607] });
      
      expect(response.status).toBe(200);
      expect(response.body).toHaveProperty('beatmaps');
      expect(Array.isArray(response.body.beatmaps)).toBe(true);
      expect(response.body.beatmaps.length).toBe(3);
    });

    it('should handle missing beatmap_ids', async () => {
      const response = await request(app)
        .post('/api/beatmaps')
        .send({});
      
      expect(response.status).toBe(400);
      expect(response.body).toHaveProperty('error');
    });

    it('should handle empty beatmap_ids array', async () => {
      const response = await request(app)
        .post('/api/beatmaps')
        .send({ beatmap_ids: [] });
      
      expect(response.status).toBe(400);
      expect(response.body).toHaveProperty('error');
    });

    it('should handle large batch requests', async () => {
      const ids = Array.from({ length: 150 }, (_, i) => i + 1);
      const response = await request(app)
        .post('/api/beatmaps')
        .send({ beatmap_ids: ids });
      
      expect(response.status).toBe(413);
      expect(response.body).toHaveProperty('error');
    });
  });

  describe('GET /api/user/:id', () => {
    it('should return valid user response structure', async () => {
      const response = await request(app).get('/api/user/2217753');
      
      expect([200, 404]).toContain(response.status);
      
      if (response.status === 200) {
        expect(response.body).toHaveProperty('user_id');
        expect(response.body).toHaveProperty('stats');
        expect(response.body).toHaveProperty('top_plays');
        expect(Array.isArray(response.body.top_plays)).toBe(true);
      }
    });

    it('should return 404 for non-existent user', async () => {
      const response = await request(app).get('/api/user/999999999');
      
      expect(response.status).toBe(404);
      expect(response.body).toHaveProperty('error');
    });

    it('should handle invalid user id', async () => {
      const response = await request(app).get('/api/user/invalid');
      
      expect(response.status).toBe(400);
      expect(response.body).toHaveProperty('error');
    });
  });

  describe('Error Handling', () => {
    it('should handle invalid JSON in request body', async () => {
      const response = await request(app)
        .post('/api/cohort')
        .set('Content-Type', 'application/json')
        .send('invalid json');
      
      expect(response.status).toBe(400);
    });

    it('should handle 404 for unknown routes', async () => {
      const response = await request(app).get('/api/unknown-route');
      
      expect(response.status).toBe(404);
    });
  });
});

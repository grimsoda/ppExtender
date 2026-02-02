import request from 'supertest';
import app from './index';

// Mock DuckDB
jest.mock('@duckdb/node-api', () => ({
  Database: {
    create: jest.fn()
  },
  DuckDBConnection: jest.fn(),
  DuckDBPreparedStatement: jest.fn()
}));

describe('Health Check', () => {
  it('should return ok status', async () => {
    const response = await request(app).get('/health');
    expect(response.status).toBe(200);
    expect(response.body).toHaveProperty('status', 'ok');
  });

  it('should return database connection status', async () => {
    const response = await request(app).get('/health');
    expect(response.status).toBe(200);
    expect(response.body).toHaveProperty('database');
  });
});

describe('POST /api/cohort', () => {
  it('should return cohort stats for valid beatmap', async () => {
    const response = await request(app)
      .post('/api/cohort')
      .send({ beatmap_id: 12345 });
    
    expect(response.status).toBe(200);
    expect(response.body).toHaveProperty('cohort_size');
    expect(response.body).toHaveProperty('pp_distribution');
    expect(response.body.pp_distribution).toHaveProperty('min');
    expect(response.body.pp_distribution).toHaveProperty('max');
    expect(response.body.pp_distribution).toHaveProperty('mean');
    expect(response.body.pp_distribution).toHaveProperty('median');
  });

  it('should handle missing beatmap_id', async () => {
    const response = await request(app)
      .post('/api/cohort')
      .send({});
    
    expect(response.status).toBe(400);
    expect(response.body).toHaveProperty('error');
    expect(response.body.error).toContain('beatmap_id');
  });

  it('should handle invalid beatmap_id type', async () => {
    const response = await request(app)
      .post('/api/cohort')
      .send({ beatmap_id: 'invalid' });
    
    expect(response.status).toBe(400);
    expect(response.body).toHaveProperty('error');
  });

  it('should handle pp range filters', async () => {
    const response = await request(app)
      .post('/api/cohort')
      .send({ 
        beatmap_id: 12345,
        pp_lower: 100,
        pp_upper: 300
      });
    
    expect(response.status).toBe(200);
    expect(response.body).toHaveProperty('cohort_size');
  });

  it('should handle mods filter', async () => {
    const response = await request(app)
      .post('/api/cohort')
      .send({ 
        beatmap_id: 12345,
        mods: ['HD', 'HR']
      });
    
    expect(response.status).toBe(200);
    expect(response.body).toHaveProperty('cohort_size');
  });

  it('should return 404 for non-existent beatmap', async () => {
    const response = await request(app)
      .post('/api/cohort')
      .send({ beatmap_id: 999999999 });
    
    expect(response.status).toBe(404);
    expect(response.body).toHaveProperty('error');
  });
});

describe('POST /api/recommend', () => {
  it('should return recommendations for valid beatmap', async () => {
    const response = await request(app)
      .post('/api/recommend')
      .send({ beatmap_id: 12345 });
    
    expect(response.status).toBe(200);
    expect(response.body).toHaveProperty('recommendations');
    expect(response.body).toHaveProperty('total');
    expect(Array.isArray(response.body.recommendations)).toBe(true);
  });

  it('should return ranked beatmap list', async () => {
    const response = await request(app)
      .post('/api/recommend')
      .send({ beatmap_id: 12345 });
    
    expect(response.status).toBe(200);
    if (response.body.recommendations.length > 0) {
      const first = response.body.recommendations[0];
      expect(first).toHaveProperty('beatmap_id');
      expect(first).toHaveProperty('similarity_score');
    }
  });

  it('should handle missing beatmap_id', async () => {
    const response = await request(app)
      .post('/api/recommend')
      .send({});
    
    expect(response.status).toBe(400);
    expect(response.body).toHaveProperty('error');
  });

  it('should filter by pp range', async () => {
    const response = await request(app)
      .post('/api/recommend')
      .send({ 
        beatmap_id: 12345,
        pp_lower: 150,
        pp_upper: 400
      });
    
    expect(response.status).toBe(200);
    expect(response.body).toHaveProperty('recommendations');
  });

  it('should filter by mods', async () => {
    const response = await request(app)
      .post('/api/recommend')
      .send({ 
        beatmap_id: 12345,
        mods: ['DT']
      });
    
    expect(response.status).toBe(200);
    expect(response.body).toHaveProperty('recommendations');
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
  it('should return batch metadata for valid beatmap_ids', async () => {
    const response = await request(app)
      .post('/api/beatmaps')
      .send({ beatmap_ids: [12345, 67890] });
    
    expect(response.status).toBe(200);
    expect(response.body).toHaveProperty('beatmaps');
    expect(Array.isArray(response.body.beatmaps)).toBe(true);
  });

  it('should return metadata for each beatmap', async () => {
    const response = await request(app)
      .post('/api/beatmaps')
      .send({ beatmap_ids: [12345] });
    
    expect(response.status).toBe(200);
    if (response.body.beatmaps.length > 0) {
      const beatmap = response.body.beatmaps[0];
      expect(beatmap).toHaveProperty('beatmap_id');
      expect(beatmap).toHaveProperty('title');
      expect(beatmap).toHaveProperty('artist');
      expect(beatmap).toHaveProperty('difficulty');
    }
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

  it('should handle invalid beatmap_id in array', async () => {
    const response = await request(app)
      .post('/api/beatmaps')
      .send({ beatmap_ids: [12345, 'invalid', 67890] });
    
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
  it('should return user stats for valid user id', async () => {
    const response = await request(app).get('/api/user/12345');
    
    expect(response.status).toBe(200);
    expect(response.body).toHaveProperty('user_id');
    expect(response.body).toHaveProperty('stats');
    expect(response.body).toHaveProperty('top_plays');
  });

  it('should return user stats object', async () => {
    const response = await request(app).get('/api/user/12345');
    
    expect(response.status).toBe(200);
    expect(response.body.stats).toHaveProperty('total_plays');
    expect(response.body.stats).toHaveProperty('average_pp');
    expect(response.body.stats).toHaveProperty('peak_rank');
  });

  it('should return top plays array', async () => {
    const response = await request(app).get('/api/user/12345');
    
    expect(response.status).toBe(200);
    expect(Array.isArray(response.body.top_plays)).toBe(true);
  });

  it('should handle invalid user id', async () => {
    const response = await request(app).get('/api/user/invalid');
    
    expect(response.status).toBe(400);
    expect(response.body).toHaveProperty('error');
  });

  it('should return 404 for non-existent user', async () => {
    const response = await request(app).get('/api/user/999999999');
    
    expect(response.status).toBe(404);
    expect(response.body).toHaveProperty('error');
  });

  it('should handle top plays with beatmap metadata', async () => {
    const response = await request(app).get('/api/user/12345');
    
    expect(response.status).toBe(200);
    if (response.body.top_plays.length > 0) {
      const play = response.body.top_plays[0];
      expect(play).toHaveProperty('beatmap_id');
      expect(play).toHaveProperty('pp');
      expect(play).toHaveProperty('accuracy');
      expect(play).toHaveProperty('mods');
    }
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

  it('should handle database connection errors gracefully', async () => {
    // This test verifies graceful error handling when DB is unavailable
    const response = await request(app).get('/health');
    expect(response.status).toBe(200);
  });
});

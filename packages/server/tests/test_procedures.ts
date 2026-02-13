import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { appRouter } from '../src/root-router';
import { getGlobalDatabase, resetGlobalDatabase } from '../src/db';
import * as path from 'path';

describe('tRPC Recommender Procedures', () => {
  const caller = appRouter.createCaller({});
  
  beforeAll(async () => {
    // Initialize database connection for integration tests
    const dbPath = process.env.DUCKDB_PATH || path.resolve(__dirname, '../../../../data/warehouse/2026-02/osu.duckdb');
    const db = getGlobalDatabase(dbPath);
    await db.connect();
  });

  afterAll(async () => {
    resetGlobalDatabase();
  });

  describe('health', () => {
    it('should return ok status with database info', async () => {
      const result = await caller.recommender.health();
      
      expect(result).toHaveProperty('status');
      expect(result.status).toBe('ok');
      expect(result).toHaveProperty('database');
      expect(['connected', 'disconnected']).toContain(result.database);
    });
  });

  describe('getCohort', () => {
    it('should return cohort data for valid beatmap_id', async () => {
      const result = await caller.recommender.getCohort({
        beatmap_id: 75, // osu! tutorial beatmap
        pp_lower: 0,
        pp_upper: 10000,
        top_k: 50,
      });

      expect(result).toHaveProperty('beatmap_id');
      expect(result).toHaveProperty('cohort_size');
      expect(result).toHaveProperty('pp_distribution');
      expect(result.pp_distribution).toHaveProperty('min');
      expect(result.pp_distribution).toHaveProperty('max');
      expect(result.pp_distribution).toHaveProperty('mean');
      expect(result.pp_distribution).toHaveProperty('median');
      expect(result).toHaveProperty('accuracy_distribution');
      expect(result).toHaveProperty('top_players');
      expect(Array.isArray(result.top_players)).toBe(true);
      expect(result).toHaveProperty('plays');
      expect(Array.isArray(result.plays)).toBe(true);
      expect(result).toHaveProperty('seed_pp_range');
    });

    it('should handle mods filter', async () => {
      const result = await caller.recommender.getCohort({
        beatmap_id: 75,
        mods: ['DT', 'HR'],
        top_k: 50,
      });

      expect(result).toHaveProperty('beatmap_id');
      expect(result.beatmap_id).toBe(75);
    });

    it('should validate beatmap_id is required', async () => {
      await expect(
        caller.recommender.getCohort({
          beatmap_id: undefined as any,
        })
      ).rejects.toThrow();
    });
  });

  describe('getBeatmapPlays', () => {
    it('should return plays for valid beatmap_id', async () => {
      const result = await caller.recommender.getBeatmapPlays({
        beatmap_id: 75,
        top_k: 50,
      });

      expect(result).toHaveProperty('beatmap_id');
      expect(result).toHaveProperty('total_plays');
      expect(result).toHaveProperty('all_plays');
      expect(Array.isArray(result.all_plays)).toBe(true);
      
      if (result.all_plays.length > 0) {
        const play = result.all_plays[0];
        expect(play).toHaveProperty('scoreId');
        expect(play).toHaveProperty('userId');
        expect(play).toHaveProperty('pp');
        expect(play).toHaveProperty('accuracy');
        expect(play).toHaveProperty('mods');
        expect(play).toHaveProperty('rank');
        expect(play).toHaveProperty('maxCombo');
      }
    });

    it('should handle mods filter', async () => {
      const result = await caller.recommender.getBeatmapPlays({
        beatmap_id: 75,
        mods: ['HD'],
        top_k: 50,
      });

      expect(result).toHaveProperty('beatmap_id');
      expect(result.beatmap_id).toBe(75);
    });
  });

  describe('getRecommendations', () => {
    it('should return recommendations for valid beatmap_id', async () => {
      const result = await caller.recommender.getRecommendations({
        beatmap_id: 75,
        pp_lower: 0,
        pp_upper: 10000,
        limit: 5,
      });

      expect(result).toHaveProperty('beatmap_id');
      expect(result).toHaveProperty('total');
      expect(result).toHaveProperty('recommendations');
      expect(Array.isArray(result.recommendations)).toBe(true);
      
      if (result.recommendations.length > 0) {
        const rec = result.recommendations[0];
        expect(rec).toHaveProperty('beatmap_id');
        expect(rec).toHaveProperty('beatmapset_id');
        expect(rec).toHaveProperty('title');
        expect(rec).toHaveProperty('artist');
        expect(rec).toHaveProperty('version');
        expect(rec).toHaveProperty('creator');
        expect(rec).toHaveProperty('difficulty_rating');
        expect(rec).toHaveProperty('bpm');
        expect(rec).toHaveProperty('total_length');
        expect(rec).toHaveProperty('play_count');
        expect(rec).toHaveProperty('avg_pp');
        expect(rec).toHaveProperty('avg_accuracy');
        expect(rec).toHaveProperty('similarity_score');
      }
    });

    it('should respect limit parameter', async () => {
      const result = await caller.recommender.getRecommendations({
        beatmap_id: 75,
        limit: 3,
      });

      expect(result.total).toBeLessThanOrEqual(3);
      expect(result.recommendations.length).toBeLessThanOrEqual(3);
    });
  });

  describe('getUser', () => {
    it('should return user data for valid user_id', async () => {
      // Test with a well-known user ID (osu! tutorial creator or similar)
      const result = await caller.recommender.getUser({
        user_id: 1,
      });

      expect(result).toHaveProperty('user_id');
      expect(result).toHaveProperty('stats');
      expect(result.stats).toHaveProperty('total_plays');
      expect(result.stats).toHaveProperty('average_pp');
      expect(result.stats).toHaveProperty('peak_rank');
      expect(result).toHaveProperty('top_plays');
      expect(Array.isArray(result.top_plays)).toBe(true);
      
      if (result.top_plays.length > 0) {
        const play = result.top_plays[0];
        expect(play).toHaveProperty('beatmap_id');
        expect(play).toHaveProperty('title');
        expect(play).toHaveProperty('artist');
        expect(play).toHaveProperty('version');
        expect(play).toHaveProperty('pp');
        expect(play).toHaveProperty('mods');
        expect(play).toHaveProperty('accuracy');
      }
    });

    it('should validate user_id is required', async () => {
      await expect(
        caller.recommender.getUser({
          user_id: undefined as any,
        })
      ).rejects.toThrow();
    });
  });

  describe('getBeatmaps', () => {
    it('should return beatmaps for valid IDs', async () => {
      const result = await caller.recommender.getBeatmaps({
        beatmap_ids: [75, 76, 77],
      });

      expect(result).toHaveProperty('beatmaps');
      expect(Array.isArray(result.beatmaps)).toBe(true);
      expect(result.beatmaps.length).toBe(3);
    });

    it('should return null for non-existent beatmap IDs', async () => {
      const result = await caller.recommender.getBeatmaps({
        beatmap_ids: [999999999],
      });

      expect(result.beatmaps[0]).toBeNull();
    });

    it('should validate array length constraints', async () => {
      await expect(
        caller.recommender.getBeatmaps({
          beatmap_ids: [],
        })
      ).rejects.toThrow();

      await expect(
        caller.recommender.getBeatmaps({
          beatmap_ids: Array(101).fill(1),
        })
      ).rejects.toThrow();
    });
  });
});

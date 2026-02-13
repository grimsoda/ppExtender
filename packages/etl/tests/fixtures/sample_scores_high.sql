-- Sample osu_scores_high data for ID resolution integration tests
-- This fixture contains legacy scores that should be matched against new scores

-- Exact matches: These will match exactly on (user_id, beatmap_id, mods_key, pp)
INSERT INTO `osu_scores_high` (`score_id`, `user_id`, `beatmap_id`, `score`, `pp`, `accuracy`, `max_combo`, `mods`, `passed`, `perfect`, `statistics`, `created_at`, `updated_at`, `build_id`, `legacy_score_id`, `legacy_total_score`, `started_at`, `ended_at`, `rank`, `type`, `is_legacy_score`) VALUES
-- Exact match pair 1: Will match new_scores.sql score_id 2001
(1001, 101, 201, 1000000, 500.0, 99.5, 500, 64, 1, 1, '{"perfect": 100, "great": 200, "good": 10, "meh": 0, "miss": 0}', '2024-01-01 10:00:00', '2024-01-01 10:05:00', 12345, NULL, 1000000, '2024-01-01 10:00:00', '2024-01-01 10:05:00', 'SS', 'solo_score', 0),
-- Exact match pair 2: Will match new_scores.sql score_id 2002
(1002, 101, 202, 950000, 480.0, 98.0, 450, 16, 1, 0, '{"perfect": 90, "great": 210, "good": 15, "meh": 2, "miss": 1}', '2024-01-02 11:00:00', '2024-01-02 11:04:00', 12346, NULL, 950000, '2024-01-02 11:00:00', '2024-01-02 11:04:00', 'S', 'solo_score', 0),
-- Exact match pair 3: Will match new_scores.sql score_id 2003 (no mods)
(1003, 102, 203, 980000, 450.0, 97.5, 400, 0, 1, 0, '{"perfect": 85, "great": 215, "good": 20, "meh": 3, "miss": 2}', '2024-01-03 09:00:00', '2024-01-03 09:03:00', 12347, NULL, 980000, '2024-01-03 09:00:00', '2024-01-03 09:03:00', 'A', 'solo_score', 0),
-- Exact match pair 4: Will match new_scores.sql score_id 2004 (DT+HR mods = 72)
(1004, 102, 204, 990000, 600.0, 99.0, 550, 72, 1, 1, '{"perfect": 95, "great": 205, "good": 8, "meh": 1, "miss": 0}', '2024-01-04 14:00:00', '2024-01-04 14:06:00', 12348, NULL, 990000, '2024-01-04 14:00:00', '2024-01-04 14:06:00', 'SS', 'solo_score', 0),
-- Exact match pair 5: Will match new_scores.sql score_id 2005
(1005, 103, 205, 970000, 550.0, 98.5, 480, 8, 1, 0, '{"perfect": 88, "great": 212, "good": 12, "meh": 1, "miss": 1}', '2024-01-05 16:00:00', '2024-01-05 16:05:00', 12349, NULL, 970000, '2024-01-05 16:00:00', '2024-01-05 16:05:00', 'S', 'solo_score', 0);

-- Fuzzy matches: Same user/beatmap/mods but slightly different pp
INSERT INTO `osu_scores_high` (`score_id`, `user_id`, `beatmap_id`, `score`, `pp`, `accuracy`, `max_combo`, `mods`, `passed`, `perfect`, `statistics`, `created_at`, `updated_at`, `build_id`, `legacy_score_id`, `legacy_total_score`, `started_at`, `ended_at`, `rank`, `type`, `is_legacy_score`) VALUES
-- Fuzzy match: Will match new_scores.sql score_id 2006 with close pp (350.0 vs 352.5)
(1006, 104, 206, 850000, 350.0, 96.5, 380, 64, 1, 0, '{"perfect": 80, "great": 220, "good": 25, "meh": 4, "miss": 3}', '2024-01-06 13:00:00', '2024-01-06 13:04:00', 12350, NULL, 850000, '2024-01-06 13:00:00', '2024-01-06 13:04:00', 'A', 'solo_score', 0),
-- Fuzzy match: Will match new_scores.sql score_id 2007 with close pp (420.0 vs 422.0)
(1007, 105, 207, 920000, 420.0, 98.0, 460, 16, 1, 0, '{"perfect": 92, "great": 208, "good": 14, "meh": 2, "miss": 1}', '2024-01-07 15:00:00', '2024-01-07 15:05:00', 12351, NULL, 920000, '2024-01-07 15:00:00', '2024-01-07 15:05:00', 'S', 'solo_score', 0);

-- Unmatched: These have no corresponding new score (for testing unmatched handling)
INSERT INTO `osu_scores_high` (`score_id`, `user_id`, `beatmap_id`, `score`, `pp`, `accuracy`, `max_combo`, `mods`, `passed`, `perfect`, `statistics`, `created_at`, `updated_at`, `build_id`, `legacy_score_id`, `legacy_total_score`, `started_at`, `ended_at`, `rank`, `type`, `is_legacy_score`) VALUES
-- Unmatched: User 999 doesn't exist in new scores
(1008, 999, 999, 800000, 300.0, 95.0, 350, 0, 1, 0, '{"perfect": 75, "great": 225, "good": 30, "meh": 5, "miss": 4}', '2024-01-08 10:00:00', '2024-01-08 10:04:00', 12352, NULL, 800000, '2024-01-08 10:00:00', '2024-01-08 10:04:00', 'B', 'solo_score', 0);

-- Duplicate legacy ID test: Same score_id appearing twice (edge case)
-- Note: In real data this shouldn't happen, but we need to test handling
INSERT INTO `osu_scores_high` (`score_id`, `user_id`, `beatmap_id`, `score`, `pp`, `accuracy`, `max_combo`, `mods`, `passed`, `perfect`, `statistics`, `created_at`, `updated_at`, `build_id`, `legacy_score_id`, `legacy_total_score`, `started_at`, `ended_at`, `rank`, `type`, `is_legacy_score`) VALUES
(1009, 106, 208, 880000, 380.0, 97.0, 420, 0, 1, 0, '{"perfect": 82, "great": 218, "good": 18, "meh": 3, "miss": 2}', '2024-01-09 11:00:00', '2024-01-09 11:04:00', 12353, NULL, 880000, '2024-01-09 11:00:00', '2024-01-09 11:04:00', 'A', 'solo_score', 0);

-- Multiple scores for same user/beatmap with different mods (edge case)
INSERT INTO `osu_scores_high` (`score_id`, `user_id`, `beatmap_id`, `score`, `pp`, `accuracy`, `max_combo`, `mods`, `passed`, `perfect`, `statistics`, `created_at`, `updated_at`, `build_id`, `legacy_score_id`, `legacy_total_score`, `started_at`, `ended_at`, `rank`, `type`, `is_legacy_score`) VALUES
-- User 107 on beatmap 209 with different mods combinations
(1010, 107, 209, 900000, 400.0, 97.5, 440, 0, 1, 0, '{"perfect": 85, "great": 215, "good": 15, "meh": 2, "miss": 1}', '2024-01-10 12:00:00', '2024-01-10 12:04:00', 12354, NULL, 900000, '2024-01-10 12:00:00', '2024-01-10 12:04:00', 'A', 'solo_score', 0),
(1011, 107, 209, 950000, 450.0, 98.5, 470, 64, 1, 0, '{"perfect": 90, "great": 210, "good": 10, "meh": 1, "miss": 0}', '2024-01-10 13:00:00', '2024-01-10 13:05:00', 12355, NULL, 950000, '2024-01-10 13:00:00', '2024-01-10 13:05:00', 'S', 'solo_score', 0),
(1012, 107, 209, 980000, 480.0, 99.0, 490, 16, 1, 1, '{"perfect": 95, "great": 205, "good": 5, "meh": 0, "miss": 0}', '2024-01-10 14:00:00', '2024-01-10 14:06:00', 12356, NULL, 980000, '2024-01-10 14:00:00', '2024-01-10 14:06:00', 'SS', 'solo_score', 0);

-- Null values test: Score with NULL in some fields (edge case)
INSERT INTO `osu_scores_high` (`score_id`, `user_id`, `beatmap_id`, `score`, `pp`, `accuracy`, `max_combo`, `mods`, `passed`, `perfect`, `statistics`, `created_at`, `updated_at`, `build_id`, `legacy_score_id`, `legacy_total_score`, `started_at`, `ended_at`, `rank`, `type`, `is_legacy_score`) VALUES
(1013, 108, 210, 750000, NULL, 94.0, 320, 0, 1, 0, '{"perfect": 70, "great": 230, "good": 35, "meh": 6, "miss": 5}', '2024-01-11 10:00:00', '2024-01-11 10:03:00', 12357, NULL, 750000, '2024-01-11 10:00:00', '2024-01-11 10:03:00', 'C', 'solo_score', 0);

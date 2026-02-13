-- Sample scores data for ID resolution integration tests
-- This fixture contains new scores that match against legacy scores from sample_scores_high.sql

-- Exact matches: These correspond to osu_scores_high entries 1001-1005
INSERT INTO `scores` (`id`, `user_id`, `beatmap_id`, `ruleset_id`, `total_score`, `accuracy`, `max_combo`, `statistics`, `passed`, `perfect`, `pp`, `legacy_score_id`, `legacy_total_score`, `started_at`, `ended_at`, `rank`, `type`, `build_id`, `ended_at_unix`, `is_legacy_score`, `has_replay`, `preserve`, `client_version`, `data`) VALUES
-- Exact match pair 1: Matches legacy score_id 1001 (DT mod = 64, pp=500.0)
(2001, 101, 201, 0, 1000000, 99.5, 500, '{"perfect": 100, "great": 200, "good": 10, "meh": 0, "miss": 0}', 1, 1, 500.0, 1001, 1000000, '2024-01-01 10:00:00', '2024-01-01 10:05:00', 'SS', 'solo_score', 12345, 1704123900, 0, 1, 1, '2024.101.0', '{"mods": [{"acronym": "DT"}]}'),
-- Exact match pair 2: Matches legacy score_id 1002 (HR mod = 16, pp=480.0)
(2002, 101, 202, 0, 950000, 98.0, 450, '{"perfect": 90, "great": 210, "good": 15, "meh": 2, "miss": 1}', 1, 0, 480.0, 1002, 950000, '2024-01-02 11:00:00', '2024-01-02 11:04:00', 'S', 'solo_score', 12346, 1704211440, 0, 1, 1, '2024.101.0', '{"mods": [{"acronym": "HR"}]}'),
-- Exact match pair 3: Matches legacy score_id 1003 (no mods = 0, pp=450.0)
(2003, 102, 203, 0, 980000, 97.5, 400, '{"perfect": 85, "great": 215, "good": 20, "meh": 3, "miss": 2}', 1, 0, 450.0, 1003, 980000, '2024-01-03 09:00:00', '2024-01-03 09:03:00', 'A', 'solo_score', 12347, 1704285780, 0, 1, 1, '2024.101.0', '{"mods": []}'),
-- Exact match pair 4: Matches legacy score_id 1004 (DT+HR mods = 72, pp=600.0)
(2004, 102, 204, 0, 990000, 99.0, 550, '{"perfect": 95, "great": 205, "good": 8, "meh": 1, "miss": 0}', 1, 1, 600.0, 1004, 990000, '2024-01-04 14:00:00', '2024-01-04 14:06:00', 'SS', 'solo_score', 12348, 1704386760, 0, 1, 1, '2024.101.0', '{"mods": [{"acronym": "DT"}, {"acronym": "HR"}]}'),
-- Exact match pair 5: Matches legacy score_id 1005 (HD mod = 8, pp=550.0)
(2005, 103, 205, 0, 970000, 98.5, 480, '{"perfect": 88, "great": 212, "good": 12, "meh": 1, "miss": 1}', 1, 0, 550.0, 1005, 970000, '2024-01-05 16:00:00', '2024-01-05 16:05:00', 'S', 'solo_score', 12349, 1704482700, 0, 1, 1, '2024.101.0', '{"mods": [{"acronym": "HD"}]}');

-- Fuzzy matches: Same user/beatmap/mods but slightly different pp values
INSERT INTO `scores` (`id`, `user_id`, `beatmap_id`, `ruleset_id`, `total_score`, `accuracy`, `max_combo`, `statistics`, `passed`, `perfect`, `pp`, `legacy_score_id`, `legacy_total_score`, `started_at`, `ended_at`, `rank`, `type`, `build_id`, `ended_at_unix`, `is_legacy_score`, `has_replay`, `preserve`, `client_version`, `data`) VALUES
-- Fuzzy match: Matches legacy score_id 1006 with close pp (352.5 vs 350.0)
(2006, 104, 206, 0, 855000, 96.8, 385, '{"perfect": 81, "great": 219, "good": 24, "meh": 4, "miss": 2}', 1, 0, 352.5, 1006, 850000, '2024-01-06 13:00:00', '2024-01-06 13:04:00', 'A', 'solo_score', 12350, 1704552240, 0, 1, 1, '2024.101.0', '{"mods": [{"acronym": "DT"}]}'),
-- Fuzzy match: Matches legacy score_id 1007 with close pp (422.0 vs 420.0)
(2007, 105, 207, 0, 928000, 98.2, 465, '{"perfect": 93, "great": 207, "good": 13, "meh": 2, "miss": 1}', 1, 0, 422.0, 1007, 920000, '2024-01-07 15:00:00', '2024-01-07 15:05:00', 'S', 'solo_score', 12351, 1704643500, 0, 1, 1, '2024.101.0', '{"mods": [{"acronym": "HR"}]}');

-- Multiple scores for same user/beatmap with different mods (edge case testing)
INSERT INTO `scores` (`id`, `user_id`, `beatmap_id`, `ruleset_id`, `total_score`, `accuracy`, `max_combo`, `statistics`, `passed`, `perfect`, `pp`, `legacy_score_id`, `legacy_total_score`, `started_at`, `ended_at`, `rank`, `type`, `build_id`, `ended_at_unix`, `is_legacy_score`, `has_replay`, `preserve`, `client_version`, `data`) VALUES
-- User 107 on beatmap 209: NM (no mods) version - matches legacy score_id 1010
(2008, 107, 209, 0, 905000, 97.8, 445, '{"perfect": 86, "great": 214, "good": 14, "meh": 2, "miss": 1}', 1, 0, 405.0, 1010, 900000, '2024-01-10 12:00:00', '2024-01-10 12:04:00', 'A', 'solo_score', 12354, 1704895440, 0, 1, 1, '2024.101.0', '{"mods": []}'),
-- User 107 on beatmap 209: DT version - matches legacy score_id 1011
(2009, 107, 209, 0, 955000, 98.6, 475, '{"perfect": 91, "great": 209, "good": 9, "meh": 1, "miss": 0}', 1, 0, 455.0, 1011, 950000, '2024-01-10 13:00:00', '2024-01-10 13:05:00', 'S', 'solo_score', 12355, 1704899100, 0, 1, 1, '2024.101.0', '{"mods": [{"acronym": "DT"}]}'),
-- User 107 on beatmap 209: HR version - matches legacy score_id 1012
(2010, 107, 209, 0, 985000, 99.2, 495, '{"perfect": 96, "great": 204, "good": 4, "meh": 0, "miss": 0}', 1, 1, 485.0, 1012, 980000, '2024-01-10 14:00:00', '2024-01-10 14:06:00', 'SS', 'solo_score', 12356, 1704902760, 0, 1, 1, '2024.101.0', '{"mods": [{"acronym": "HR"}]}');

-- Null values test: Score with NULL pp for matching against legacy score 1013
INSERT INTO `scores` (`id`, `user_id`, `beatmap_id`, `ruleset_id`, `total_score`, `accuracy`, `max_combo`, `statistics`, `passed`, `perfect`, `pp`, `legacy_score_id`, `legacy_total_score`, `started_at`, `ended_at`, `rank`, `type`, `build_id`, `ended_at_unix`, `is_legacy_score`, `has_replay`, `preserve`, `client_version`, `data`) VALUES
-- Null pp match: Matches legacy score_id 1013 which also has NULL pp
(2011, 108, 210, 0, 760000, 94.5, 325, '{"perfect": 71, "great": 229, "good": 34, "meh": 5, "miss": 4}', 1, 0, NULL, 1013, 750000, '2024-01-11 10:00:00', '2024-01-11 10:03:00', 'C', 'solo_score', 12357, 1704982980, 0, 1, 1, '2024.101.0', '{"mods": []}');

-- Additional new scores without matching legacy scores (for ensuring we don't match incorrectly)
INSERT INTO `scores` (`id`, `user_id`, `beatmap_id`, `ruleset_id`, `total_score`, `accuracy`, `max_combo`, `statistics`, `passed`, `perfect`, `pp`, `legacy_score_id`, `legacy_total_score`, `started_at`, `ended_at`, `rank`, `type`, `build_id`, `ended_at_unix`, `is_legacy_score`, `has_replay`, `preserve`, `client_version`, `data`) VALUES
-- Orphan score: No matching legacy score (user 200 doesn't exist in high scores)
(2012, 200, 300, 0, 1100000, 99.9, 600, '{"perfect": 100, "great": 200, "good": 0, "meh": 0, "miss": 0}', 1, 1, 700.0, 2000, 1100000, '2024-01-12 10:00:00', '2024-01-12 10:06:00', 'SS', 'solo_score', 12358, 1705069380, 0, 1, 1, '2024.101.0', '{"mods": [{"acronym": "DT"}, {"acronym": "HR"}, {"acronym": "HD"}]}'),
-- Another orphan: Different beatmap entirely
(2013, 201, 301, 0, 1050000, 99.0, 580, '{"perfect": 98, "great": 202, "good": 2, "meh": 0, "miss": 0}', 1, 1, 650.0, 2001, 1050000, '2024-01-13 11:00:00', '2024-01-13 11:05:00', 'SS', 'solo_score', 12359, 1705159500, 0, 1, 1, '2024.101.0', '{"mods": [{"acronym": "DT"}]}');

# Data Directory

This directory contains SQL dump files for the osu! top 10000 performance dataset.

## Structure

```
data/
└── ingest/
    └── 2026-02/
        └── sql/           # SQL dump files
```

## Files

The `sql/` directory contains 13 SQL dump files:

### Large Data Files
- `scores.sql` (~13GB) - Main scores data
- `osu_scores_high.sql` (~6GB) - High scores data
- `osu_beatmap_difficulty_attribs.sql` (~4GB) - Beatmap difficulty attributes
- `osu_user_beatmap_playcount.sql` (~1.6GB) - User beatmap playcounts
- `osu_beatmap_difficulty.sql` (~470MB) - Beatmap difficulty data
- `osu_beatmap_failtimes.sql` (~134MB) - Beatmap fail times
- `osu_beatmapsets.sql` (~42MB) - Beatmap sets metadata
- `osu_beatmaps.sql` (~52MB) - Beatmaps metadata
- `osu_user_stats.sql` (~2MB) - User statistics

### Small Metadata Files
- `sample_users.sql` (~250KB) - Sample user data
- `osu_counts.sql` (~3KB) - Counts metadata
- `osu_difficulty_attribs.sql` (~3KB) - Difficulty attributes metadata
- `osu_beatmap_performance_blacklist.sql` (~2KB) - Performance blacklist

## Source

These files are copied from:
`/run/media/work/OS/ppExtender/datasets/2026_02_01_performance_osu_top_10000/`

## Note

The SQL files in `data/ingest/2026-02/sql/` are gitignored due to their large size (24GB total).
They must be copied manually from the source location.

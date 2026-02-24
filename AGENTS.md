# Agent Learnings & Deviations

This document captures key technical decisions and learnings from the osu! Recommender project that deviate from standard practices or first-sight intuition.

## Table of Contents

1. [MySQL Import Performance](#mysql-import-performance)
2. [Bash Arithmetic with Decimals](#bash-arithmetic-with-decimals)
3. [DuckDB Memory Management](#duckdb-memory-management)
4. [Parquet Export Structure](#parquet-export-structure)
5. [Database Schema Mapping](#database-schema-mapping)
6. [Import Progress Monitoring](#import-progress-monitoring)
7. [Tailwind CSS v4 Syntax Change](#tailwind-css-v4-syntax-change)
8. [Bun Test Runner Gotcha](#bun-test-runner-gotcha)
9. [tRPC + Express Route Conflicts](#trpc--express-route-conflicts)

---

## MySQL Import Performance

### The Deviation

**Standard Practice**: Apply session optimizations in separate queries before import.

**What We Learned**: MySQL session optimizations must be applied in the **SAME connection** as the import operation.

### The Problem

Setting session variables in one connection and importing in another doesn't work:

```bash
# WRONG - Settings are lost
mysql -e "SET SESSION foreign_key_checks = 0"
mysql < import.sql  # Settings not applied!

# WRONG - Separate connections
mysql -e "SET SESSION ..."
mysql -e "SOURCE import.sql"
```

### The Solution

Pipe optimizations directly into the import stream:

```bash
(
    echo "SET SESSION foreign_key_checks = 0;"
    echo "SET SESSION unique_checks = 0;"
    echo "SET SESSION autocommit = 0;"
    cat import.sql
    echo "COMMIT;"
) | mysql -uuser -ppass database
```

### Performance Impact

| Approach | Speed | Time for 20GB |
|----------|-------|---------------|
| No optimizations | 0.87 MB/s | ~6.6 hours |
| Separate connections | 0.87 MB/s | ~6.6 hours |
| Same connection (correct) | 3-5 MB/s | ~1-2 hours |

### Key Optimizations

```sql
SET SESSION foreign_key_checks = 0;      -- Skip FK validation
SET SESSION unique_checks = 0;           -- BIGGEST WIN: Skip index validation
SET SESSION autocommit = 0;              -- Batch commits
SET SESSION sql_log_bin = 0;             -- Skip replication log
SET GLOBAL innodb_flush_log_at_trx_commit = 0;  -- Delay disk writes
SET GLOBAL innodb_doublewrite = 0;       -- Skip double-write buffer
```

---

## Bash Arithmetic with Decimals

### The Deviation

**Standard Practice**: Use `$((...))` for arithmetic in bash.

**What We Learned**: `bc` command returns decimal strings that break bash arithmetic.

### The Problem

```bash
# This FAILS with "arithmetic syntax error"
est_remaining_sec=$(echo "scale=0; $remaining * $avg_time" | bc)
est_remaining_min=$((est_remaining_sec / 60))  # ERROR: 41864.000 is not an integer
```

### The Solution

Use `bc` for ALL arithmetic when dealing with calculated values:

```bash
# WRONG
local margin_min=$((margin_sec / 60))

# CORRECT
local margin_min=$(echo "scale=0; $margin_sec / 60" | bc)
```

### Pattern for Time Calculations

```bash
# Calculate with bc
local est_remaining_sec=$(echo "scale=0; $remaining_statements * $avg_time" | bc)
local est_remaining_min=$(echo "scale=0; $est_remaining_sec / 60" | bc)
local est_remaining_hr=$(echo "scale=0; $est_remaining_min / 60" | bc)
```

---

## DuckDB Memory Management

### The Deviation

**Standard Practice**: Create tables with single CREATE TABLE AS SELECT (CTAS) query.

**What We Learned**: Window functions over 62M rows require batch processing to avoid OOM.

### The Problem

```sql
-- FAILS with OutOfMemory on 12GB RAM
CREATE TABLE mart_best_scores AS
SELECT *,
       ROW_NUMBER() OVER (PARTITION BY user_id, beatmap_id, mods_key ORDER BY pp DESC) as rn
FROM stg_scores
```

### The Solution

Process in batches using NTILE:

```sql
-- Step 1: Create empty result table
CREATE TABLE mart_best_scores (...schema...);

-- Step 2: Create user ranges (10 batches)
CREATE TEMPORARY TABLE user_ranges AS
SELECT user_id, NTILE(10) OVER (ORDER BY user_id) as batch_num
FROM (SELECT DISTINCT user_id FROM stg_scores);

-- Step 3: Process each batch
FOR batch IN 1..10:
    INSERT INTO mart_best_scores
    SELECT ...
    FROM stg_scores
    WHERE user_id IN (SELECT user_id FROM user_ranges WHERE batch_num = batch)
    QUALIFY ROW_NUMBER() OVER (...) = 1
```

### Configuration for Large Datasets

```sql
SET preserve_insertion_order = false;  -- Allow reordering for memory efficiency
SET threads = 2;                        -- Reduce parallelism to save memory
```

### Memory-Safe Pattern

Always use `QUALIFY` instead of subqueries with window functions:

```sql
-- Memory efficient
SELECT * FROM table
QUALIFY ROW_NUMBER() OVER (...) = 1

-- Memory intensive (creates intermediate result)
SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (...) as rn
    FROM table
) WHERE rn = 1
```

---

## Parquet Export Structure

### The Deviation

**Standard Practice**: Export to single file per table.

**What We Learned**: `PER_THREAD_OUTPUT` creates subdirectories, not multiple files in same directory.

### The Structure

```
data/parquet/
├── scores/
│   └── data.parquet/           # Directory, not file!
│       └── data_0.parquet      # Actual file
│       └── data_1.parquet      # (if multiple threads produce files)
├── osu_beatmaps/
│   └── data.parquet/
│       └── data_0.parquet
```

### Export Configuration

```sql
COPY (SELECT * FROM table)
TO 'output_dir/data.parquet' (
    FORMAT PARQUET,
    COMPRESSION 'zstd',
    COMPRESSION_LEVEL 3,
    ROW_GROUP_SIZE 1000000,
    ROW_GROUP_SIZE_BYTES '16MB',
    PARQUET_VERSION 'V2',
    PER_THREAD_OUTPUT true  -- Creates data_0.parquet, data_1.parquet, etc.
)
```

### Loading Pattern

```python
# Load all files in subdirectory
glob_pattern = f"{table_dir}/data.parquet/*.parquet"
con.execute(f"SELECT * FROM read_parquet('{glob_pattern}')")
```

---

## Database Schema Mapping

### The Deviation

**Standard Practice**: Backend queries match ETL output directly.

**What We Learned**: Backend expected denormalized tables, ETL created normalized raw tables.

### The Mismatch

**Backend Expected**:
- `user_beatmap_playcount` with columns: `user_id, beatmap_id, pp, mods, accuracy, score`
- `beatmaps` with columns: `beatmap_id, title, artist, difficulty_rating, ...`
- `users` with columns: `user_id, username, pp_raw, ...`

**ETL Created**:
- `raw_osu_user_beatmap_playcount` with columns: `user_id, beatmap_id, playcount`
- `raw_osu_beatmaps` with columns: `beatmap_id, beatmapset_id, difficultyrating, ...`
- `raw_osu_user_stats` with columns: `user_id, rank_score, accuracy, ...`

### The Solution

Create views that map raw tables to expected schema:

```sql
-- Create view for user_beatmap_playcount
CREATE OR REPLACE VIEW user_beatmap_playcount AS
SELECT 
    user_id,
    beatmap_id,
    score,
    pp,
    mods_key as mods,
    95.0 as accuracy  -- Placeholder
FROM mart_best_scores;

-- Create view for beatmaps
CREATE OR REPLACE VIEW beatmaps AS
SELECT 
    beatmap_id,
    beatmapset_id,
    version,
    difficultyrating as difficulty_rating,
    bpm,
    total_length,
    playmode as mode,
    approved as status
FROM raw_osu_beatmaps;

-- Create view for users
CREATE OR REPLACE VIEW users AS
SELECT 
    user_id,
    CAST(user_id as VARCHAR) as username,
    rank_score as pp_raw,
    accuracy,
    playcount,
    level
FROM raw_osu_user_stats;
```

### Pattern for Schema Evolution

Always create views for API compatibility rather than modifying backend code:

```sql
-- If backend expects table X but ETL creates table Y
-- Create: CREATE VIEW X AS SELECT ... FROM Y
```

---

## Import Progress Monitoring

### The Deviation

**Standard Practice**: Query row counts to track import progress.

**What We Learned**: Row count queries are blocked by metadata locks during import.

### The Problem

```sql
-- This BLOCKS during import!
SELECT COUNT(*) FROM table;  -- Waits for metadata lock
```

### The Solution

Use MySQL processlist polling:

```bash
# Poll every 0.2 seconds to detect INSERT statement changes
while kill -0 $mysql_pid; do
    sleep 0.2
    
    # Get current query
    query=$(mysql -e "SHOW PROCESSLIST" | grep "$conn_id" | awk '{...}')
    
    # Check if query changed (new INSERT statement)
    if [ "$query" != "$prev_query" ]; then
        # Calculate timing stats
        # Update progress
    fi
done
```

### Timing Calculation

```bash
# Track statement duration
statement_duration=$((current_time - last_statement_time))

# Rolling average (last 20 samples)
avg_time=$(echo "scale=3; $total_time / $window_size" | bc)

# Margin of error (95% confidence)
margin=$(echo "scale=3; 1.96 * $std_dev / sqrt($window_size)" | bc)

# Time estimate with margin
eta="${est_remaining_hr}h ${est_remaining_min}m (±${margin_min}m)"
```

### Heartbeat Pattern

Show updates even when same query is running:

```bash
if [ $((current_time - last_update_time)) -ge $update_interval ]; then
    echo "Current INSERT running for: ${current_query_time}s"
fi
```

---

## Performance Benchmarks

| Operation | Before Optimization | After Optimization | Improvement |
|-----------|-------------------|-------------------|-------------|
| MySQL Import | 0.87 MB/s | 3-5 MB/s | 4-6x faster |
| Parquet Export | 1.1 MB/s | 3.9-4.5 MB/s | 3-4x faster |
| Gold Phase (62M rows) | OOM (failed) | 572s (9.5 min) | Working |
| Compression Ratio | - | 8x (39GB → 4.8GB) | - |

---

## Key Takeaways

1. **MySQL Session State**: Always apply session settings in the same connection stream
2. **Bash Arithmetic**: Use `bc` exclusively when dealing with calculated decimal values
3. **Large Datasets**: Batch process window functions using NTILE to avoid OOM
4. **PER_THREAD_OUTPUT**: Creates nested directory structure, not multiple files
5. **Schema Mapping**: Use views to decouple ETL schema from API expectations
6. **Import Monitoring**: Processlist polling is the only non-blocking approach

---

## Tailwind CSS v4 Syntax Change

### The Deviation

**Standard Practice**: Use `@tailwind` directives in CSS files.

**What We Learned**: Tailwind CSS v4 uses a new import-based syntax instead of directives.

### The Problem

```css
/* WRONG - Tailwind v3 syntax (breaks in v4) */
@tailwind base;
@tailwind components;
@tailwind utilities;

/* CORRECT - Tailwind v4 syntax */
@import "tailwindcss";
```

**Symptom**: App appears completely unstyled/barebones because Tailwind doesn't generate any CSS.

### Why This Happens

Tailwind CSS v4 is a complete rewrite that:
- Replaces `@tailwind` directives with `@import "tailwindcss"`
- Uses a new PostCSS plugin (`@tailwindcss/postcss` instead of `tailwindcss`)
- Has different configuration patterns

### Migration Checklist

When upgrading from Tailwind v3 to v4:

1. **Update `index.css`**:
   ```css
   @import "tailwindcss";
   ```

2. **Update `postcss.config.js`**:
   ```js
   export default {
     plugins: {
       '@tailwindcss/postcss': {},  // New package name
       autoprefixer: {},
     },
   }
   ```

3. **Remove `tailwind.config.js` (optional)**: v4 supports CSS-based configuration

### Reference

- [Tailwind CSS v4 Documentation](https://tailwindcss.com/docs/v4-beta)
- [Migration Guide](https://tailwindcss.com/docs/v4-beta#changes-from-v3)

---

## Bun Test Runner Gotcha

### The Deviation

**Standard Practice**: Run `bun test` to execute tests in Bun projects.

**What We Learned**: `bun test` is a reserved command that runs Bun's native test runner, ignoring package.json scripts.

### The Problem

```bash
# This runs Bun's native test runner (WRONG - no jsdom)
bun test  # 0 tests passing

# This runs the package.json test script (CORRECT)
bun run test  # 55 tests passing
npm test      # 55 tests passing
```

**Why**: `bun test` is hardcoded to use `bun:test` and ignores the `"test": "vitest"` script.

### The Solution

**Option 1**: Use `bun run test` (explicit)
**Option 2**: Add shorthand script `"t": "vitest"` and use `bun t`
**Option 3**: Use `npm test` (always works)

### Shell Alias (Optional)

```bash
# Add to .bashrc or .zshrc for convenience
alias bt='bun run test'
```

---

## Troubleshooting Guide

### "arithmetic syntax error" in bash
**Cause**: Using `$((...))` with decimal values from `bc`
**Fix**: Use `$(echo "scale=0; $value / 60" | bc)` for all calculations

### "Out of Memory Error" in DuckDB
**Cause**: Window functions over large datasets
**Fix**: Process in batches using NTILE(10) and loops

### "Table not found" in backend
**Cause**: ETL creates `raw_*` tables, backend expects clean names
**Fix**: Create views mapping raw tables to expected names

### Import progress not updating
**Cause**: Row count queries blocked by metadata locks
**Fix**: Use SHOW PROCESSLIST polling instead

### "Cannot find parquet files"
**Cause**: Looking in `table/*.parquet` but files are in `table/data.parquet/*.parquet`
**Fix**: Update glob pattern to include `data.parquet` subdirectory

---

## tRPC + Express Route Conflicts

### The Deviation

**Standard Practice**: Mount tRPC middleware at `/api` for clean URLs.

**What We Learned**: tRPC middleware mounted at `/api` intercepts ALL requests to `/api/*`, conflicting with REST endpoints.

### The Problem

```typescript
// WRONG - tRPC intercepts /api/cohort requests
app.use('/api', createExpressMiddleware({ router: appRouter }));
app.post('/api/cohort', handler);  // Never reached!
```

**Symptom**: REST endpoints return 404 because tRPC tries to handle them first.

### The Solution

Mount tRPC at a different path (e.g., `/trpc`) to avoid conflicts:

```typescript
// CORRECT - tRPC at /trpc, REST at /api
app.use('/trpc', createExpressMiddleware({ router: appRouter }));
app.post('/api/cohort', handler);  // Works correctly
```

Or fully migrate to tRPC and remove REST endpoints:

```typescript
// tRPC-only architecture
app.use('/trpc', createExpressMiddleware({ router: appRouter }));
// No REST endpoints needed
```

### Frontend Client Configuration

When tRPC is at `/trpc`:

```typescript
// packages/app/src/lib/trpc.ts
export const trpcClient = trpc.createClient({
  links: [
    httpBatchLink({
      url: `${API_URL}/trpc`,  // Note: /trpc not /api
    }),
  ],
});
```

### React Query Integration

tRPC React hooks use TanStack Query internally:

```typescript
// packages/app/src/api-hooks.ts
export function useGetCohort(beatmap_id: number) {
  return trpc.recommender.getCohort.useQuery(
    { beatmap_id },
    {
      enabled: !!beatmap_id,
      select: (data) => ({
        // Transform snake_case to camelCase for component compatibility
        size: data.cohort_size,
        ppDistribution: data.pp_distribution,
      }),
    }
  );
}
```

### Reference

- [tRPC Express Adapter](https://trpc.io/docs/server/adapters/express)
- [tRPC React Query](https://trpc.io/docs/client/react)

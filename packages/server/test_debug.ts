import * as path from 'path';
import * as fs from 'fs';

const DB_PATH = path.resolve('data/warehouse/2026-02/osu.duckdb');
console.log('DB_PATH:', DB_PATH);
console.log('DB exists:', fs.existsSync(DB_PATH));

async function test() {
  const duckdb = await import('@duckdb/node-api');
  const instance = await duckdb.DuckDBInstance.create(DB_PATH);
  const connection = await instance.connect();
  
  const result = await connection.run(
    'SELECT COUNT(*) as count FROM user_beatmap_playcount WHERE beatmap_id = ?',
    [59605]
  );
  const rows = await result.getRowObjects();
  console.log('Query result:', rows);
  
  await connection.closeSync();
}

test().catch(console.error);

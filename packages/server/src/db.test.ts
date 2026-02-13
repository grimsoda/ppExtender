import { Database, Connection, getGlobalDatabase, resetGlobalDatabase } from '../src/db';
import * as path from 'path';
import * as fs from 'fs';

/**
 * Database class unit tests
 * 
 * Tests the Database wrapper for DuckDB Neo client
 */

describe('Database', () => {
  const testDbPath = path.resolve(__dirname, 'test.db');
  let db: Database;

  beforeEach(() => {
    // Clean up any existing test database
    if (fs.existsSync(testDbPath)) {
      fs.unlinkSync(testDbPath);
    }
    resetGlobalDatabase();
  });

  afterEach(async () => {
    await db?.close();
    // Clean up test database
    if (fs.existsSync(testDbPath)) {
      fs.unlinkSync(testDbPath);
    }
  });

  describe('constructor', () => {
    it('should create a Database instance with the given path', () => {
      db = new Database(testDbPath);
      expect(db).toBeDefined();
    });
  });

  describe('exists', () => {
    it('should return false if database file does not exist', () => {
      db = new Database('/nonexistent/path/test.db');
      expect(db.exists()).toBe(false);
    });

    it('should return true if database file exists', () => {
      // Create a dummy file
      fs.writeFileSync(testDbPath, '');
      db = new Database(testDbPath);
      expect(db.exists()).toBe(true);
    });
  });

  describe('connect', () => {
    it('should throw error if database file does not exist', async () => {
      db = new Database('/nonexistent/path/test.db');
      await expect(db.connect()).rejects.toThrow('Database not found');
    });

    it('should connect successfully if database file exists', async () => {
      // Create a test database first
      const { DuckDBInstance } = await import('@duckdb/node-api');
      const instance = await DuckDBInstance.create(testDbPath);
      await instance.connect();
      
      db = new Database(testDbPath);
      await expect(db.connect()).resolves.not.toThrow();
    });
  });

  describe('query', () => {
    beforeEach(async () => {
      // Create a test database with sample data
      const { DuckDBInstance } = await import('@duckdb/node-api');
      const instance = await DuckDBInstance.create(testDbPath);
      const conn = await instance.connect();
      
      await conn.runAndReadAll(`
        CREATE TABLE test_table (
          id INTEGER,
          name VARCHAR,
          value DOUBLE
        )
      `);
      
      await conn.runAndReadAll(`
        INSERT INTO test_table VALUES 
          (1, 'Alice', 10.5),
          (2, 'Bob', 20.3),
          (3, 'Charlie', 30.7)
      `);
      
      conn.closeSync();

      db = new Database(testDbPath);
      await db.connect();
    });

    it('should execute a simple query and return rows as objects', async () => {
      const rows = await db.query('SELECT * FROM test_table ORDER BY id');
      
      expect(rows).toHaveLength(3);
      expect(rows[0]).toHaveProperty('id', 1);
      expect(rows[0]).toHaveProperty('name', 'Alice');
      expect(rows[0]).toHaveProperty('value', 10.5);
    });

    it('should execute a query with parameters', async () => {
      const rows = await db.query('SELECT * FROM test_table WHERE id = ?', [2]);
      
      expect(rows).toHaveLength(1);
      expect(rows[0]).toHaveProperty('id', 2);
      expect(rows[0]).toHaveProperty('name', 'Bob');
    });

    it('should return empty array for query with no results', async () => {
      const rows = await db.query('SELECT * FROM test_table WHERE id = ?', [999]);
      
      expect(rows).toHaveLength(0);
      expect(rows).toEqual([]);
    });

    it('should throw error if not connected', async () => {
      const newDb = new Database(testDbPath);
      await expect(newDb.query('SELECT 1')).rejects.toThrow('Database not connected');
    });
  });

  describe('queryRaw', () => {
    beforeEach(async () => {
      // Create a test database with sample data
      const { DuckDBInstance } = await import('@duckdb/node-api');
      const instance = await DuckDBInstance.create(testDbPath);
      const conn = await instance.connect();
      
      await conn.runAndReadAll(`
        CREATE TABLE raw_test (id INTEGER, name VARCHAR)
      `);
      await conn.runAndReadAll(`
        INSERT INTO raw_test VALUES (1, 'Test')
      `);

      conn.closeSync();

      db = new Database(testDbPath);
      await db.connect();
    });

    it('should return raw rows as arrays', async () => {
      const rows = await db.queryRaw('SELECT * FROM raw_test');

      expect(rows).toHaveLength(1);
      expect(Array.isArray(rows[0])).toBe(true);
      expect(rows[0][0]).toBe(1);
      expect(rows[0][1]).toBe('Test');
    });
  });

  describe('getConnection', () => {
    beforeEach(async () => {
      const { DuckDBInstance } = await import('@duckdb/node-api');
      const instance = await DuckDBInstance.create(testDbPath);
      await instance.connect();
      
      db = new Database(testDbPath);
      await db.connect();
    });

    it('should return a Connection instance', async () => {
      const conn = await db.getConnection();
      expect(conn).toBeDefined();
      expect(conn).toBeInstanceOf(Connection);
      conn.close();
    });

    it('should auto-connect if not connected', async () => {
      const newDb = new Database(testDbPath);
      const conn = await newDb.getConnection();
      expect(conn).toBeDefined();
      conn.close();
    });
  });

  describe('close', () => {
    it('should close without error', async () => {
      const { DuckDBInstance } = await import('@duckdb/node-api');
      const instance = await DuckDBInstance.create(testDbPath);
      await instance.connect();
      
      db = new Database(testDbPath);
      await db.connect();
      
      await expect(db.close()).resolves.not.toThrow();
    });
  });
});

describe('Connection', () => {
  const testDbPath = path.resolve(__dirname, 'conn_test.db');
  let db: Database;
  let conn: Connection;

  beforeEach(async () => {
    // Clean up
    if (fs.existsSync(testDbPath)) {
      fs.unlinkSync(testDbPath);
    }
    
    // Create test database
    const { DuckDBInstance } = await import('@duckdb/node-api');
    const instance = await DuckDBInstance.create(testDbPath);
    const duckConn = await instance.connect();
    
    await duckConn.runAndReadAll(`
      CREATE TABLE conn_test (id INTEGER, data VARCHAR)
    `);
    await duckConn.runAndReadAll(`
      INSERT INTO conn_test VALUES (1, 'A'), (2, 'B'), (3, 'C')
    `);

    duckConn.closeSync();

    db = new Database(testDbPath);
    await db.connect();
    conn = await db.getConnection();
  });

  afterEach(async () => {
    conn?.close();
    await db?.close();
    if (fs.existsSync(testDbPath)) {
      fs.unlinkSync(testDbPath);
    }
  });

  describe('query', () => {
    it('should execute query and return objects', async () => {
      const rows = await conn.query('SELECT * FROM conn_test ORDER BY id');
      
      expect(rows).toHaveLength(3);
      expect(rows[0]).toEqual({ id: 1, data: 'A' });
      expect(rows[1]).toEqual({ id: 2, data: 'B' });
      expect(rows[2]).toEqual({ id: 3, data: 'C' });
    });

    it('should execute query with parameters', async () => {
      const rows = await conn.query('SELECT * FROM conn_test WHERE id = ?', [2]);
      
      expect(rows).toHaveLength(1);
      expect(rows[0]).toEqual({ id: 2, data: 'B' });
    });
  });

  describe('queryRaw', () => {
    it('should return raw arrays', async () => {
      const rows = await conn.queryRaw('SELECT * FROM conn_test ORDER BY id');
      
      expect(rows).toHaveLength(3);
      expect(rows[0]).toEqual([1, 'A']);
      expect(rows[1]).toEqual([2, 'B']);
      expect(rows[2]).toEqual([3, 'C']);
    });
  });

  describe('close', () => {
    it('should close the connection', () => {
      expect(() => conn.close()).not.toThrow();
    });
  });
});

describe('Global Database', () => {
  const testDbPath = path.resolve(__dirname, 'global_test.db');

  beforeEach(() => {
    resetGlobalDatabase();
    if (fs.existsSync(testDbPath)) {
      fs.unlinkSync(testDbPath);
    }
  });

  afterEach(() => {
    resetGlobalDatabase();
    if (fs.existsSync(testDbPath)) {
      fs.unlinkSync(testDbPath);
    }
  });

  describe('getGlobalDatabase', () => {
    it('should create new instance with path', () => {
      const db = getGlobalDatabase(testDbPath);
      expect(db).toBeDefined();
      expect(db).toBeInstanceOf(Database);
    });

    it('should return same instance on subsequent calls', () => {
      const db1 = getGlobalDatabase(testDbPath);
      const db2 = getGlobalDatabase();
      expect(db1).toBe(db2);
    });

    it('should throw error if not initialized and no path provided', () => {
      expect(() => getGlobalDatabase()).toThrow('Database not initialized');
    });
  });

  describe('resetGlobalDatabase', () => {
    it('should reset the global instance', () => {
      const db1 = getGlobalDatabase(testDbPath);
      resetGlobalDatabase();
      const db2 = getGlobalDatabase(testDbPath);
      expect(db1).not.toBe(db2);
    });
  });
});

/**
 * Integration tests with real database
 * These tests require the actual DuckDB database to exist
 */
describe('Database Integration', () => {
  const realDbPath = path.resolve(__dirname, '../../../data/warehouse/2026-02/osu.duckdb');
  let db: Database;

  beforeAll(async () => {
    // Only run if database exists
    if (!fs.existsSync(realDbPath)) {
      console.warn(`Skipping integration tests - database not found at ${realDbPath}`);
      return;
    }
    
    db = new Database(realDbPath);
    await db.connect();
  });

  afterAll(async () => {
    await db?.close();
  });

  it('should query mart_user_topk table', async () => {
    if (!fs.existsSync(realDbPath)) {
      return;
    }
    
    const rows = await db.query('SELECT COUNT(*) as count FROM mart_user_topk LIMIT 1');
    expect(rows).toHaveLength(1);
    expect(rows[0]).toHaveProperty('count');
  });

  it('should query beatmaps table', async () => {
    if (!fs.existsSync(realDbPath)) {
      return;
    }
    
    const rows = await db.query('SELECT beatmap_id, title FROM beatmaps LIMIT 1');
    expect(rows.length).toBeGreaterThanOrEqual(0);
    if (rows.length > 0) {
      expect(rows[0]).toHaveProperty('beatmap_id');
      expect(rows[0]).toHaveProperty('title');
    }
  });

  it('should query with parameters', async () => {
    if (!fs.existsSync(realDbPath)) {
      return;
    }
    
    const rows = await db.query(
      'SELECT beatmap_id FROM beatmaps WHERE beatmap_id = ?',
      [59605]
    );
    // May or may not find the beatmap, but query should execute without error
    expect(Array.isArray(rows)).toBe(true);
  });
});

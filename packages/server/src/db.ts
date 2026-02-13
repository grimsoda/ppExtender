import { DuckDBInstance } from '@duckdb/node-api';
import * as path from 'path';
import * as fs from 'fs';

/**
 * Database class for DuckDB Neo client
 * 
 * Provides a simple interface for connecting to DuckDB and executing queries.
 * Uses the new Neo API with runAndReadAll() and getRows() methods.
 */
export class Database {
  private instance: DuckDBInstance | null = null;
  private dbPath: string;

  constructor(dbPath: string) {
    this.dbPath = path.resolve(dbPath);
  }

  /**
   * Check if the database file exists
   */
  exists(): boolean {
    return fs.existsSync(this.dbPath);
  }

  /**
   * Connect to the database
   * @throws Error if database file doesn't exist
   */
  async connect(): Promise<void> {
    if (!this.exists()) {
      throw new Error(`Database not found at ${this.dbPath}`);
    }

    this.instance = await DuckDBInstance.create(this.dbPath);
  }

  /**
   * Execute a query and return all rows
   * 
   * @param sql - SQL query string
   * @param params - Optional query parameters
   * @returns Array of row objects
   */
  async query(sql: string, params?: (number | string)[]): Promise<any[]> {
    if (!this.instance) {
      throw new Error('Database not connected. Call connect() first.');
    }

    const connection = await this.instance.connect();
    
    try {
      const result = await connection.runAndReadAll(sql, params);
      const rows = result.getRows();
      
      // Convert rows to objects with column names as keys
      const columnNames = result.columnNames();
      return rows.map((row: any[]) => {
        const obj: Record<string, any> = {};
        columnNames.forEach((name: string, index: number) => {
          obj[name] = row[index];
        });
        return obj;
      });
    } finally {
      connection.closeSync();
    }
  }

  /**
   * Execute a query and return raw rows (array of arrays)
   * 
   * @param sql - SQL query string
   * @param params - Optional query parameters
   * @returns Array of row arrays
   */
  async queryRaw(sql: string, params?: (number | string)[]): Promise<any[][]> {
    if (!this.instance) {
      throw new Error('Database not connected. Call connect() first.');
    }

    const connection = await this.instance.connect();
    
    try {
      const result = await connection.runAndReadAll(sql, params);
      return result.getRows();
    } finally {
      connection.closeSync();
    }
  }

  /**
   * Get a single connection for multiple queries
   * Useful for transactions or multiple queries in sequence
   * Auto-connects if not already connected
   */
  async getConnection(): Promise<Connection> {
    if (!this.instance) {
      await this.connect();
    }

    const conn = await this.instance!.connect();
    return new Connection(conn);
  }

  /**
   * Close the database instance
   */
  async close(): Promise<void> {
    if (this.instance) {
      // DuckDBInstance doesn't have a close method in the Neo API
      // Connections are closed after each query
      this.instance = null;
    }
  }
}

/**
 * Connection wrapper for executing multiple queries
 */
export class Connection {
  private connection: any;

  constructor(connection: any) {
    this.connection = connection;
  }

  /**
   * Execute a query and return all rows as objects
   */
  async query(sql: string, params?: (number | string)[]): Promise<any[]> {
    const result = await this.connection.runAndReadAll(sql, params);
    const rows = result.getRows();
    
    const columnNames = result.columnNames();
    return rows.map((row: any[]) => {
      const obj: Record<string, any> = {};
      columnNames.forEach((name: string, index: number) => {
        obj[name] = row[index];
      });
      return obj;
    });
  }

  /**
   * Execute a query and return raw rows
   */
  async queryRaw(sql: string, params?: (number | string)[]): Promise<any[][]> {
    const result = await this.connection.runAndReadAll(sql, params);
    return result.getRows();
  }

  /**
   * Close the connection
   */
  close(): void {
    this.connection.closeSync();
  }
}

/**
 * Singleton database instance for the application
 */
let globalDb: Database | null = null;

/**
 * Get or create the global database instance
 */
export function getGlobalDatabase(dbPath?: string): Database {
  if (!globalDb && dbPath) {
    globalDb = new Database(dbPath);
  }
  
  if (!globalDb) {
    throw new Error('Database not initialized. Provide dbPath on first call.');
  }
  
  return globalDb;
}

/**
 * Reset the global database instance (useful for testing)
 */
export function resetGlobalDatabase(): void {
  globalDb = null;
}

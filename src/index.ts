import mysql from 'mysql2/promise';

const connectionName = process.env.SQL_CONNECTION_NAME;
const host = process.env.SQL_HOST;
const dbUser = process.env.SQL_USER;
const dbPassword = process.env.SQL_PASSWORD;
const dbName = process.env.SQL_NAME;
const connectionLimit = (process.env.SQL_CONNECTION_LIMIT && Number.parseInt(process.env.SQL_CONNECTION_LIMIT)) || 100;

const mysqlConfig: mysql.PoolOptions = {
  user: dbUser,
  password: dbPassword,
  database: dbName,
  charset: 'utf8mb4_unicode_ci',
  connectionLimit: connectionLimit,
};
if (process.env.NODE_ENV === 'production') {
  mysqlConfig.socketPath = `/cloudsql/${connectionName}`;
} else {
  mysqlConfig.host = host;
}

export class DbPool {
  pool: mysql.Pool;

  private static instance: DbPool;
  static getInstance(): DbPool {
    if (!DbPool.instance) {
      DbPool.instance = new DbPool();
    }
    return DbPool.instance;
  }

  constructor() {
    this.pool = mysql.createPool(mysqlConfig);
    const logTimeout = setInterval(() => {
      const _allConnections = (this.pool.pool as any)._allConnections.length;
      const _freeConnections = (this.pool.pool as any)._freeConnections.length;
      const _connectionQueue = (this.pool.pool as any)._connectionQueue.length;
      console.log(
        `[MYSQL CONNECTION STATUS] All Connections ${_allConnections} - Free Connections ${_freeConnections} - Queue Connections ${_connectionQueue}`
      );
      if (_allConnections === mysqlConfig.connectionLimit && _freeConnections < (mysqlConfig.connectionLimit as number) / 2) {
        console.error(new Error('Mysql pool connection limit alert'));
      }
    }, 5 * 60000);
    process.on('SIGTERM', () => {
      clearInterval(logTimeout);
    });
  }
}

export class Db {
  private _pool: mysql.Pool;
  private _transaction: mysql.PoolConnection | null = null;

  constructor() {
    this._pool = DbPool.getInstance().pool;
  }

  getPool(): mysql.Pool {
    return this._pool;
  }

  async query(query: string, values?: any): Promise<any> {
    const db = this._transaction || this._pool;
    const [rows] = await db.query(query, values);
    return rows;
  }

  async beginTransaction(): Promise<void> {
    const conn = await this._pool.getConnection();
    conn.beginTransaction();
    this._transaction = conn;
  }

  async commit(): Promise<void> {
    if (!this._transaction) {
      throw Error('No transaction currently started');
    }
    await this._transaction.commit();
  }

  async rollback(): Promise<void> {
    if (!this._transaction) {
      throw Error('No transaction currently started');
    }
    await this._transaction.rollback();
  }
}

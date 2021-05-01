import mysql from 'mysql';

const connectionName = process.env.SQL_CONNECTION_NAME;
const host = process.env.SQL_HOST;
const dbUser = process.env.SQL_USER;
const dbPassword = process.env.SQL_PASSWORD;
const dbName = process.env.SQL_NAME;

const mysqlConfig: mysql.PoolConfig = {
  user: dbUser,
  password: dbPassword,
  database: dbName,
  charset: 'utf8mb4_unicode_ci',
  connectionLimit: 100
};
if (process.env.NODE_ENV === 'production') {
  mysqlConfig.socketPath = `/cloudsql/${connectionName}`;
}
else {
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
    setInterval(() => {
      const _allConnections = (this.pool as any)._allConnections.length;
      const _acquiringConnections = (this.pool as any)._acquiringConnections.length;
      const _freeConnections = (this.pool as any)._freeConnections.length;
      const _connectionQueue = (this.pool as any)._connectionQueue.length;
      console.log(`********** MYSQL CONNECTION STATUS ***********`);
      console.log(`All Connections ${_allConnections}`);
      console.log(`Acquiring Connections ${_acquiringConnections}`);
      console.log(`Free Connections ${_freeConnections}`);
      console.log(`Queue Connections ${_connectionQueue}`);
      console.log(`**********************************************`);
      if (_allConnections === mysqlConfig.connectionLimit && _freeConnections < (mysqlConfig.connectionLimit as number / 2)) {
        console.error(new Error('Mysql pool connection limit alert'));
      }

    }, 60000);

  }

}

export class Db {

  private _pool: mysql.Pool;
  private _transaction: mysql.PoolConnection | null = null;

  constructor() {
    this._pool = DbPool.getInstance().pool;
  }

  getPool(): mysql.Pool { return this._pool; }

  async query(query: string, values?: any): Promise<any> {
    return new Promise((resolve, reject) => {
      const db = this._transaction || this._pool;
      return db.query(query, values, function(err: mysql.MysqlError | null, results?: any, fields?: mysql.FieldInfo[]): void {
        if (err) { reject(err); }
        else { resolve(results); }
      });
    });
  }

  async beginTransaction(): Promise<void> {
    return new Promise((resolve, reject) => {
      return this._pool.getConnection((err: mysql.MysqlError, conn: mysql.PoolConnection) => {
        if (err) { reject(err); }
        else {
          return conn.beginTransaction((err2: mysql.MysqlError) => {
            if (err2) { reject(err2); }
            else {
              this._transaction = conn;
              resolve();
            }
          });
        }
      });
    });
  }

  async commit(): Promise<void> {
    return new Promise((resolve, reject) => {
      if (!this._transaction) { throw Error('No transaction currently started'); }
      return this._transaction.commit((err: mysql.MysqlError) => {
        (this._transaction as mysql.PoolConnection).release();
        this._transaction = null;
        if (err) { reject(err); }
        else { resolve(); }
      });
    });
  }

  async rollback(): Promise<void> {
    return new Promise((resolve, reject) => {
      if (!this._transaction) { throw Error('No transaction currently started'); }
      return this._transaction.rollback((err: mysql.MysqlError) => {
        (this._transaction as mysql.PoolConnection).release();
        this._transaction = null;
        if (err) { reject(err); }
        else { resolve(); }
      });
    });
  }

}

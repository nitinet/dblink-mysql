import { Handler, model, sql } from 'dblink-core';
import mysql from 'mysql';
import { Readable } from 'stream';

// let typeCast: mysql.TypeCast = function (field: mysql.UntypedFieldInfo & {
// 	type: string;
// 	string(): null | string;
// 	buffer(): null | Buffer;
// 	geometry(): null | mysql.GeometryType;
// }, next: () => void) {
// 	if (field.type === 'TINY' && field.length === 1) {
// 		return (field.string() === '1');
// 	} else if (field.type === 'JSON') {
// 		let data = field.string();
// 		return null != data ? JSON.parse(data) : null;
// 	} else {
// 		return next();
// 	}
// }

/**
 * Mysql Handler
 *
 * @export
 * @class Mysql
 * @typedef {Mysql}
 * @extends {Handler}
 */
export default class Mysql extends Handler {
  /**
   * Connection Pool
   *
   * @type {mysql.Pool}
   */
  connectionPool: mysql.Pool;

  /**
   * Creates an instance of Mysql.
   *
   * @constructor
   * @param {model.IConnectionConfig} config
   */
  constructor(config: model.IConnectionConfig) {
    super(config);

    this.connectionPool = mysql.createPool({
      connectionLimit: this.config.connectionLimit,
      host: this.config.host,
      port: this.config.port,
      user: this.config.username,
      password: this.config.password,
      database: this.config.database
    });
  }

  /**
   * Handler initialisation
   *
   * @async
   * @returns {Promise<void>}
   */
  async init(): Promise<void> {
    // document why this async method 'init' is empty
  }

  /**
   * Get a new Connection
   *
   * @returns {Promise<mysql.PoolConnection>}
   */
  getConnection(): Promise<mysql.PoolConnection> {
    return new Promise<mysql.PoolConnection>((res, rej) => {
      this.connectionPool.getConnection((err: mysql.MysqlError, c: mysql.PoolConnection) => {
        if (err) rej(err);
        else res(c);
      });
    });
  }

  /**
   * Initialize a Transaction
   *
   * @param {mysql.PoolConnection} conn
   * @returns {Promise<void>}
   */
  initTransaction(conn: mysql.PoolConnection): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      conn.beginTransaction((err: mysql.MysqlError) => {
        if (err) reject(err);
        else resolve();
      });
    });
  }

  /**
   * Commit Transaction
   *
   * @param {mysql.PoolConnection} conn
   * @returns {Promise<void>}
   */
  commit(conn: mysql.PoolConnection): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      conn.commit((err: mysql.MysqlError) => {
        if (err) reject(err);
        else resolve();
      });
    });
  }

  /**
   * Rollback Transaction
   *
   * @param {mysql.PoolConnection} conn
   * @returns {Promise<void>}
   */
  rollback(conn: mysql.PoolConnection): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      conn.rollback((err: mysql.MysqlError) => {
        if (err) reject(err);
        else resolve();
      });
    });
  }

  /**
   * Close Connection
   *
   * @async
   * @param {mysql.PoolConnection} conn
   * @returns {Promise<void>}
   */
  async close(conn: mysql.PoolConnection): Promise<void> {
    conn.release();
  }

  /**
   * Run string query
   *
   * @async
   * @param {string} query
   * @param {?any[]} [dataArgs]
   * @param {?mysql.Connection} [connection]
   * @returns {Promise<model.ResultSet>}
   */
  async run(query: string, dataArgs?: unknown[], connection?: mysql.Connection): Promise<model.ResultSet> {
    const conn = connection ?? this.connectionPool;

    const data: { insertId: number; changedRows: number } | Record<string, unknown>[] = await new Promise((resolve, reject) => {
      conn.query(query, dataArgs, function (err: Error | null, r: { insertId: number; changedRows: number } | Record<string, unknown>[]) {
        if (err) {
          reject(err);
        } else {
          resolve(r);
        }
      });
    });

    const result = new model.ResultSet();
    if (Array.isArray(data)) {
      result.rows = data;
      result.rowCount = data.length;
    } else {
      if (data.insertId) result.id = data.insertId;
      if (data.changedRows) {
        result.rowCount = data.changedRows;
      }
    }
    return result;
  }

  /**
   * Run statements
   *
   * @param {(sql.Statement | sql.Statement[])} queryStmt
   * @param {?mysql.Connection} [connection]
   * @returns {Promise<model.ResultSet>}
   */
  runStatement(queryStmt: sql.Statement | sql.Statement[], connection?: mysql.Connection): Promise<model.ResultSet> {
    const { query, dataArgs } = this.prepareQuery(queryStmt);
    return this.run(query, dataArgs, connection);
  }

  /**
   * Run quries and stream output
   *
   * @async
   * @param {string} query
   * @param {?any[]} [dataArgs]
   * @param {?mysql.Connection} [connection]
   * @returns {Promise<Readable>}
   */
  async stream(query: string, dataArgs?: unknown[], connection?: mysql.Connection): Promise<Readable> {
    const conn = connection ?? this.connectionPool;

    const stream = conn.query(query, dataArgs).stream();
    return stream;
  }

  /**
   * Run statements and stream output
   *
   * @param {(sql.Statement | sql.Statement[])} queryStmt
   * @param {?mysql.Connection} [connection]
   * @returns {Promise<Readable>}
   */
  streamStatement(queryStmt: sql.Statement | sql.Statement[], connection?: mysql.Connection): Promise<Readable> {
    const { query, dataArgs } = this.prepareQuery(queryStmt);
    return this.stream(query, dataArgs, connection);
  }
}

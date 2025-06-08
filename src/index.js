import { Handler, model } from 'dblink-core';
import mysql from 'mysql';
export default class Mysql extends Handler {
  connectionPool;
  constructor(config) {
    super(config);
    this.connectionPool = mysql.createPool(config);
  }
  getConnection() {
    return new Promise((res, rej) => {
      this.connectionPool.getConnection((err, c) => {
        if (err) rej(err);
        else res(c);
      });
    });
  }
  initTransaction(conn) {
    return new Promise((resolve, reject) => {
      conn.beginTransaction(err => {
        if (err) reject(err);
        else resolve();
      });
    });
  }
  commit(conn) {
    return new Promise((resolve, reject) => {
      conn.commit(err => {
        if (err) reject(err);
        else resolve();
      });
    });
  }
  rollback(conn) {
    return new Promise((resolve, reject) => {
      conn.rollback(err => {
        if (err) reject(err);
        else resolve();
      });
    });
  }
  async close(conn) {
    conn.release();
  }
  async run(query, dataArgs, connection) {
    const conn = connection ?? this.connectionPool;
    const data = await new Promise((resolve, reject) => {
      conn.query(query, dataArgs, function (err, r) {
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
    } else if (data.insertId) {
      result.rows.push({ id: data.insertId });
    }
    return result;
  }
  runStatement(queryStmt, connection) {
    const { query, dataArgs } = this.prepareQuery(queryStmt);
    return this.run(query, dataArgs, connection);
  }
  async stream(query, dataArgs, connection) {
    const conn = connection ?? this.connectionPool;
    const stream = conn.query(query, dataArgs).stream();
    return stream;
  }
  streamStatement(queryStmt, connection) {
    const { query, dataArgs } = this.prepareQuery(queryStmt);
    return this.stream(query, dataArgs, connection);
  }
  serializeValue(val, dataType) {
    if (dataType == Array) {
      return JSON.stringify(val);
    } else return val;
  }
  deSerializeValue(val, dataType) {
    if (dataType == Array) {
      return JSON.parse(val);
    } else return val;
  }
}
//# sourceMappingURL=index.js.map

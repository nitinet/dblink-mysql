import { Handler, model } from 'dblink-core';
import mysql from 'mysql';
export default class Mysql extends Handler {
    connectionPool;
    constructor(config) {
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
    async init() {
    }
    getConnection() {
        return new Promise((res, rej) => {
            this.connectionPool.getConnection((err, c) => {
                if (err)
                    rej(err);
                else
                    res(c);
            });
        });
    }
    initTransaction(conn) {
        return new Promise((resolve, reject) => {
            conn.beginTransaction((err) => {
                if (err)
                    reject(err);
                else
                    resolve();
            });
        });
    }
    commit(conn) {
        return new Promise((resolve, reject) => {
            conn.commit((err) => {
                if (err)
                    reject(err);
                else
                    resolve();
            });
        });
    }
    rollback(conn) {
        return new Promise((resolve, reject) => {
            conn.rollback((err) => {
                if (err)
                    reject(err);
                else
                    resolve();
            });
        });
    }
    async close(conn) {
        conn.release();
    }
    async run(query, dataArgs, connection) {
        let conn = connection ?? this.connectionPool;
        let data = await new Promise((resolve, reject) => {
            conn.query(query, dataArgs, function (err, r) {
                if (err) {
                    reject(err);
                }
                else {
                    resolve(r);
                }
            });
        });
        let result = new model.ResultSet();
        if (data.insertId)
            result.id = data.insertId;
        if (data.changedRows) {
            result.rowCount = data.changedRows;
        }
        else if (Array.isArray(data)) {
            result.rows = data;
            result.rowCount = data.length;
        }
        return result;
    }
    runStatement(queryStmt, connection) {
        let { query, dataArgs } = this.prepareQuery(queryStmt);
        return this.run(query, dataArgs, connection);
    }
    async stream(query, dataArgs, connection) {
        let conn = connection ?? this.connectionPool;
        let stream = conn.query(query, dataArgs).stream();
        return stream;
    }
    streamStatement(queryStmt, connection) {
        let { query, dataArgs } = this.prepareQuery(queryStmt);
        return this.stream(query, dataArgs, connection);
    }
}
//# sourceMappingURL=index.js.map
"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const sequelize_1 = require("sequelize");
class Postgres {
    /*
     * @param connectionUrl postgres connection url
     *  e.g.'postgres://user:pass@host.docker.internal.com:5432/dbname'
     */
    constructor(sequelize) {
        this.sequelize = sequelize;
    }
    getDynamicModel(schema, tableName, indexes, schemaName) {
        const initOptions = {
            sequelize: this.sequelize,
            underscored: true,
            timestamps: false,
            indexes: indexes,
            schema: schemaName,
        };
        return this.sequelize.define(tableName, schema, initOptions);
    }
    static getSequelizeObject(connectionUrl) {
        if (Postgres.sequelizeObjects.has(connectionUrl)) {
            return Postgres.sequelizeObjects.get(connectionUrl);
        }
        const sequelize = new sequelize_1.Sequelize(connectionUrl, {
            logging: false,
            benchmark: true,
            typeValidation: true,
            pool: {
                max: 3,
                min: 0,
                acquire: 30000,
                idle: 10000,
            },
        });
        Postgres.sequelizeObjects.set(connectionUrl, sequelize);
        return sequelize;
    }
    static getClient(connectionUrl) {
        const sequelize = Postgres.getSequelizeObject(connectionUrl);
        return new Postgres(sequelize);
    }
}
exports.default = Postgres;
Postgres.sequelizeObjects = new Map();

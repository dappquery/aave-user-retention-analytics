import { InitOptions, Sequelize, ModelIndexesOptions } from "sequelize";

export default class Postgres {
  public static sequelizeObjects: Map<string, Sequelize> = new Map();
  public readonly sequelize: Sequelize;

  /*
   * @param connectionUrl postgres connection url
   *  e.g.'postgres://user:pass@host.docker.internal.com:5432/dbname'
   */
  private constructor(sequelize: Sequelize) {
    this.sequelize = sequelize;
  }

  public getDynamicModel(
    schema: Record<string, any>,
    tableName: string,
    indexes: ModelIndexesOptions[],
    schemaName: string,
  ): any {
    const initOptions: InitOptions = {
      sequelize: this.sequelize,
      underscored: true,
      timestamps: false,
      indexes: indexes,
      schema: schemaName,
    };

    return this.sequelize.define(tableName, schema, initOptions);
  }

  public static getSequelizeObject(connectionUrl: string): Sequelize {
    if(Postgres.sequelizeObjects.has(connectionUrl)) {
      return Postgres.sequelizeObjects.get(connectionUrl)!;
    }

    const sequelize = new Sequelize(connectionUrl, {
      logging: false, //console.log,
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

  public static getClient(connectionUrl: string): Postgres {
    const sequelize = Postgres.getSequelizeObject(connectionUrl);

    return new Postgres(sequelize);
  }
}

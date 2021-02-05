import Postgres from './Postgres';
import { DataTypes, ModelIndexesOptions, Sequelize } from 'sequelize';
import moment from "moment";

const DBConnectionUrl = 'postgres://postgres:root@127.0.0.1:5432/dapp_development';
const schemaName = 'aave_v2';
const startTimestamp = 1606694400; // Launch of Aave V2 protocol
const tabelNameCohort = 'cohort';
const tableNameCohortBaseValue = 'cohortBaseValue';
const tabelNameCohortMetricValue = 'cohortMetricValue';

// Cohort Analytics of Aave V2 protocol
export default class CohortAnalytics {

  private cohortModel: any;

  private cohortBaseValueModel: any;

  private cohortMetricValueModel: any;

  private dbConnection: Sequelize;

  private newSignupsInCohorts: any;

  private cohortData: any;

  public async populate() {
    const oThis = this;

    this.newSignupsInCohorts = {};

    await oThis.getModelAndCreateTableIfNotExist();

    await oThis.populateCohorts();

    await oThis.populateCohortBaseValue();

    await oThis.populateCohortMetricValue();

  }

  private async getModelAndCreateTableIfNotExist() {

    this.cohortModel = await Postgres.getClient(DBConnectionUrl).getDynamicModel(this.getSchemaForCohortTable(), tabelNameCohort, this.getIndexesForCohortTable(), schemaName);

    this.cohortBaseValueModel = await Postgres.getClient(DBConnectionUrl).getDynamicModel(this.getSchemaForCohortBaseValue(), tableNameCohortBaseValue, this.getIndexesForCohortBaseValue(), schemaName);

    this.cohortMetricValueModel = await Postgres.getClient(DBConnectionUrl).getDynamicModel(this.getSchemaForCohortMetricTable(), tabelNameCohortMetricValue, this.getIndexesForCohortMetrixTable(), schemaName);

    this.dbConnection = await Postgres.getClient(DBConnectionUrl).sequelize;
    try {
      const options: any = {
        schema: schemaName,
      }
      options.force = false;
      await this.cohortModel.sync(options);

      await this.cohortBaseValueModel.sync(options);

      await this.cohortMetricValueModel.sync(options);
      console.log(`Cohort::getModelAndCreateTableIfNotExist::Tables created Successfully `);
    } catch (e) {
      console.log(`Cohort::getModelAndCreateTableIfNotExist::Error in creating table :: ${e}`);
    }
  }

  private async populateCohorts() {
    let startTimestamps = startTimestamp * 1000;
    let maximumCohort: any = await this.dbConnection.query('SELECT cohort_number, end_timestamp FROM "aave_v2"."cohorts" ORDER BY cohort_number DESC LIMIT 1');
    let cohortStart = 1;
    console.log(`Cohort::populateCohorts::Maximum existing cohort ::${JSON.stringify(maximumCohort[0])}`);
    if (maximumCohort[0].length > 0) {
      startTimestamps = new Date(maximumCohort[0][0]['end_timestamp']).getTime();
      cohortStart = maximumCohort[0][0]['cohort_number'];
    }
    let endTimestamp = parseInt(moment(startTimestamps).add('1', 'week').format('X')) * 1000;
    let recordsToInsert = [];
    while (startTimestamps < Date.now()) {
      let ObjectToInsert = {
        cohortNumber: cohortStart,
        startTimestamp: new Date(startTimestamps).toUTCString(),
        endTimestamp: new Date(endTimestamp).toUTCString(),
      }
      recordsToInsert.push(ObjectToInsert);
      startTimestamps = endTimestamp;
      endTimestamp = parseInt(moment(startTimestamps).add('1', 'week').format('X')) * 1000;
      cohortStart++;
    }
    try {
      if (recordsToInsert.length > 0) {
        await this.cohortModel.bulkCreate(recordsToInsert);
      } else {
        console.log(`Cohort::populateCohorts::Nothing To Insert In Cohort ${recordsToInsert}`);
      }
    } catch (e) {
      console.log(`Cohort::populateCohorts::Error In Inserting Data In cohort ${e}`);
    }
  }

  private async populateCohortBaseValue() {
    let data: any = await this.dbConnection.query('SELECT cohort_number as "cohortNumber", end_timestamp as "endTimestamp",start_timestamp as "startTimestamp" FROM "aave_v2"."cohorts" ORDER BY cohort_number ');
    this.cohortData = data[0]
    let alreadyExistUser: any = {};
    for (let record of this.cohortData) {
      console.log(`Cohort::populateCohorts::query For ${JSON.stringify(record)}`);
      let startDate = `'${moment(record.startTimestamp).format()}'`;
      let endDate = `'${moment(record.endTimestamp).format()}'`;
      let uniqueUsers: any[] = [];
      let userTransactionData: any = await this.dbConnection.query(`SELECT DISTINCT("user_transactions"."user") as "uniqueUser" FROM "aave_v2"."user_transactions"
        WHERE timestamp >= ${startDate} and timestamp< ${endDate}  `);
      userTransactionData[0].forEach((data: any) => {
        if (!(alreadyExistUser[data['uniqueUser']] === true)) {
          uniqueUsers.push(data['uniqueUser']);
          alreadyExistUser[data['uniqueUser']] = true;
        }
      });
      this.newSignupsInCohorts[record.cohortNumber] = uniqueUsers;
      let ObjectToInsert = {
        cohortNumber: record.cohortNumber,
        newSignups: uniqueUsers.length,
      }
      try {
        console.log(`Cohort::populateCohortBaseValue::upserting data in cohortBaseValue ${JSON.stringify(ObjectToInsert)}`)
        await this.cohortBaseValueModel.upsert(ObjectToInsert);
      } catch (e) {
        console.log(`Cohort::populateCohortBaseValue::Error In Inserting Data In cohort Base ${e}`);
      }
    }
  }

  private async populateCohortMetricValue() {
    for (let cohort of this.cohortData) {
      let weekNumber = 1;
      let newUserOfCohort = this.newSignupsInCohorts[cohort.cohortNumber]; // newUserOfCohort.length => totalTxs
      for (let week of this.cohortData) {
        if (week.cohortNumber > cohort.cohortNumber) {
          let startDate = `'${moment(week.startTimestamp).format()}'`;
          let endDate = `'${moment(week.endTimestamp).format()}'`;
          let startIteration = 0;
          let endIteration = 500;
          let totalUserTransaction = 0;
          let uniqueUserString = '';
          while (true) {
            uniqueUserString = "\'" + newUserOfCohort.slice(startIteration, endIteration).join("\',\'") + "\'";
            if (uniqueUserString === "''") {
              break;
            } else {
              let userTransactionData: any = await this.dbConnection.query(`SELECT count(DISTINCT("user_transactions"."user")) as "uniqueUser" FROM "aave_v2"."user_transactions"
              WHERE timestamp >= ${startDate} and timestamp< ${endDate} and "user_transactions"."user" IN(${uniqueUserString})`);
              totalUserTransaction = totalUserTransaction + parseInt(userTransactionData[0][0]['uniqueUser']);
            }
            startIteration = endIteration;
            endIteration += endIteration;
            uniqueUserString = '';
          }
          let objectToInsert = {
            cohortNumber: cohort.cohortNumber,
            activeUsersMakingTxs: totalUserTransaction,
            weekNumber: weekNumber,
          }
          try {
            console.log(`Cohort::populateCohortMetricValue::upserting Data In cohortMetricValues ${JSON.stringify(objectToInsert)}`)
            await this.cohortMetricValueModel.upsert(objectToInsert);
          } catch (e) {
            console.log(`Cohort::populateCohortMetricValue::Error In Inserting Data In cohort Base ${e}`);
          }
          weekNumber++;
        }
      }
    }
  }

  private getSchemaForCohortTable() {
    return {
      cohortNumber: {
        type: DataTypes.INTEGER,
        allowNull: false,
        primaryKey: true,
      },
      startTimestamp: {
        type: DataTypes.DATE,
        allowNull: true,
      },
      endTimestamp: {
        type: DataTypes.DATE,
        allowNull: true,
      },
    };
  }

  private getSchemaForCohortBaseValue() {
    return {
      cohortNumber: {
        type: DataTypes.INTEGER,
        allowNull: false,
        primaryKey: true,
      },
      newSignups: {
        type: DataTypes.INTEGER,
        allowNull: true,
      }
    }
  }

  private getSchemaForCohortMetricTable() {
    return {
      cohortNumber: {
        type: DataTypes.INTEGER,
        allowNull: false,
      },
      weekNumber: {
        type: DataTypes.INTEGER,
        allowNUll: false,
      },
      activeUsersMakingTxs: {
        type: DataTypes.INTEGER,
        allowNUll: true,
      },
    }
  }

  private getIndexesForCohortBaseValue(): ModelIndexesOptions[] {
    return [
      {
        unique: true,
        fields: ["cohort_number"],
      }
    ];
  }

  private getIndexesForCohortTable(): ModelIndexesOptions[] {
    return [
      {
        unique: true,
        fields: ["cohort_number"],
      }
    ];
  }

  private getIndexesForCohortMetrixTable(): ModelIndexesOptions[] {
    return [
      {
        concurrently: true,
        unique: true,
        fields: ["cohort_number", "week_number"],
      }
    ];
  }
};


console.log('Start populating cohort data');
new CohortAnalytics().populate().then(() => {
  console.log('Data population completed');
});

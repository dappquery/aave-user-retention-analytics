"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const Postgres_1 = __importDefault(require("./Postgres"));
const sequelize_1 = require("sequelize");
const moment_1 = __importDefault(require("moment"));
const DBConnectionUrl = 'postgres://postgres:root@127.0.0.1:5432/dapp_development';
const schemaName = 'aave_v2';
const startTimestamp = 1606694400; // Launch of Aave V2 protocol
const tabelNameCohort = 'cohort';
const tableNameCohortBaseValue = 'cohortBaseValue';
const tabelNameCohortMetricValue = 'cohortMetricValue';
// Cohort Analytics of Aave V2 protocol
class CohortAnalytics {
    populate() {
        return __awaiter(this, void 0, void 0, function* () {
            const oThis = this;
            this.newSignupsInCohorts = {};
            yield oThis.getModelAndCreateTableIfNotExist();
            yield oThis.populateCohorts();
            yield oThis.populateCohortBaseValue();
            yield oThis.populateCohortMetricValue();
        });
    }
    getModelAndCreateTableIfNotExist() {
        return __awaiter(this, void 0, void 0, function* () {
            this.cohortModel = yield Postgres_1.default.getClient(DBConnectionUrl).getDynamicModel(this.getSchemaForCohortTable(), tabelNameCohort, this.getIndexesForCohortTable(), schemaName);
            this.cohortBaseValueModel = yield Postgres_1.default.getClient(DBConnectionUrl).getDynamicModel(this.getSchemaForCohortBaseValue(), tableNameCohortBaseValue, this.getIndexesForCohortBaseValue(), schemaName);
            this.cohortMetricValueModel = yield Postgres_1.default.getClient(DBConnectionUrl).getDynamicModel(this.getSchemaForCohortMetricTable(), tabelNameCohortMetricValue, this.getIndexesForCohortMetrixTable(), schemaName);
            this.dbConnection = yield Postgres_1.default.getClient(DBConnectionUrl).sequelize;
            try {
                const options = {
                    schema: schemaName,
                };
                options.force = false;
                yield this.cohortModel.sync(options);
                yield this.cohortBaseValueModel.sync(options);
                yield this.cohortMetricValueModel.sync(options);
                console.log(`Cohort::getModelAndCreateTableIfNotExist::Tables created Successfully `);
            }
            catch (e) {
                console.log(`Cohort::getModelAndCreateTableIfNotExist::Error in creating table :: ${e}`);
            }
        });
    }
    populateCohorts() {
        return __awaiter(this, void 0, void 0, function* () {
            let startTimestamps = startTimestamp * 1000;
            let maximumCohort = yield this.dbConnection.query('SELECT cohort_number, end_timestamp FROM "aave_v2"."cohorts" ORDER BY cohort_number DESC LIMIT 1');
            let cohortStart = 1;
            console.log(`Cohort::populateCohorts::Maximum existing cohort ::${JSON.stringify(maximumCohort[0])}`);
            if (maximumCohort[0].length > 0) {
                startTimestamps = new Date(maximumCohort[0][0]['end_timestamp']).getTime();
                cohortStart = maximumCohort[0][0]['cohort_number'];
            }
            let endTimestamp = parseInt(moment_1.default(startTimestamps).add('1', 'week').format('X')) * 1000;
            let recordsToInsert = [];
            while (startTimestamps < Date.now()) {
                let ObjectToInsert = {
                    cohortNumber: cohortStart,
                    startTimestamp: new Date(startTimestamps).toUTCString(),
                    endTimestamp: new Date(endTimestamp).toUTCString(),
                };
                recordsToInsert.push(ObjectToInsert);
                startTimestamps = endTimestamp;
                endTimestamp = parseInt(moment_1.default(startTimestamps).add('1', 'week').format('X')) * 1000;
                cohortStart++;
            }
            try {
                if (recordsToInsert.length > 0) {
                    yield this.cohortModel.bulkCreate(recordsToInsert);
                }
                else {
                    console.log(`Cohort::populateCohorts::Nothing To Insert In Cohort ${recordsToInsert}`);
                }
            }
            catch (e) {
                console.log(`Cohort::populateCohorts::Error In Inserting Data In cohort ${e}`);
            }
        });
    }
    populateCohortBaseValue() {
        return __awaiter(this, void 0, void 0, function* () {
            let data = yield this.dbConnection.query('SELECT cohort_number as "cohortNumber", end_timestamp as "endTimestamp",start_timestamp as "startTimestamp" FROM "aave_v2"."cohorts" ORDER BY cohort_number ');
            this.cohortData = data[0];
            let alreadyExistUser = {};
            for (let record of this.cohortData) {
                console.log(`Cohort::populateCohorts::query For ${JSON.stringify(record)}`);
                let startDate = `'${moment_1.default(record.startTimestamp).format()}'`;
                let endDate = `'${moment_1.default(record.endTimestamp).format()}'`;
                let uniqueUsers = [];
                let userTransactionData = yield this.dbConnection.query(`SELECT DISTINCT("user_transactions"."user") as "uniqueUser" FROM "aave_v2"."user_transactions"
        WHERE timestamp >= ${startDate} and timestamp< ${endDate}  `);
                userTransactionData[0].forEach((data) => {
                    if (!(alreadyExistUser[data['uniqueUser']] === true)) {
                        uniqueUsers.push(data['uniqueUser']);
                        alreadyExistUser[data['uniqueUser']] = true;
                    }
                });
                this.newSignupsInCohorts[record.cohortNumber] = uniqueUsers;
                let ObjectToInsert = {
                    cohortNumber: record.cohortNumber,
                    newSignups: uniqueUsers.length,
                };
                try {
                    console.log(`Cohort::populateCohortBaseValue::upserting data in cohortBaseValue ${JSON.stringify(ObjectToInsert)}`);
                    yield this.cohortBaseValueModel.upsert(ObjectToInsert);
                }
                catch (e) {
                    console.log(`Cohort::populateCohortBaseValue::Error In Inserting Data In cohort Base ${e}`);
                }
            }
        });
    }
    populateCohortMetricValue() {
        return __awaiter(this, void 0, void 0, function* () {
            for (let cohort of this.cohortData) {
                let weekNumber = 1;
                let newUserOfCohort = this.newSignupsInCohorts[cohort.cohortNumber]; // newUserOfCohort.length => totalTxs
                for (let week of this.cohortData) {
                    if (week.cohortNumber > cohort.cohortNumber) {
                        let startDate = `'${moment_1.default(week.startTimestamp).format()}'`;
                        let endDate = `'${moment_1.default(week.endTimestamp).format()}'`;
                        let startIteration = 0;
                        let endIteration = 500;
                        let totalUserTransaction = 0;
                        let uniqueUserString = '';
                        while (true) {
                            uniqueUserString = "\'" + newUserOfCohort.slice(startIteration, endIteration).join("\',\'") + "\'";
                            if (uniqueUserString === "''") {
                                break;
                            }
                            else {
                                let userTransactionData = yield this.dbConnection.query(`SELECT count(DISTINCT("user_transactions"."user")) as "uniqueUser" FROM "aave_v2"."user_transactions"
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
                        };
                        try {
                            console.log(`Cohort::populateCohortMetricValue::upserting Data In cohortMetricValues ${JSON.stringify(objectToInsert)}`);
                            yield this.cohortMetricValueModel.upsert(objectToInsert);
                        }
                        catch (e) {
                            console.log(`Cohort::populateCohortMetricValue::Error In Inserting Data In cohort Base ${e}`);
                        }
                        weekNumber++;
                    }
                }
            }
        });
    }
    getSchemaForCohortTable() {
        return {
            cohortNumber: {
                type: sequelize_1.DataTypes.INTEGER,
                allowNull: false,
                primaryKey: true,
            },
            startTimestamp: {
                type: sequelize_1.DataTypes.DATE,
                allowNull: true,
            },
            endTimestamp: {
                type: sequelize_1.DataTypes.DATE,
                allowNull: true,
            },
        };
    }
    getSchemaForCohortBaseValue() {
        return {
            cohortNumber: {
                type: sequelize_1.DataTypes.INTEGER,
                allowNull: false,
                primaryKey: true,
            },
            newSignups: {
                type: sequelize_1.DataTypes.INTEGER,
                allowNull: true,
            }
        };
    }
    getSchemaForCohortMetricTable() {
        return {
            cohortNumber: {
                type: sequelize_1.DataTypes.INTEGER,
                allowNull: false,
            },
            weekNumber: {
                type: sequelize_1.DataTypes.INTEGER,
                allowNUll: false,
            },
            activeUsersMakingTxs: {
                type: sequelize_1.DataTypes.INTEGER,
                allowNUll: true,
            },
        };
    }
    getIndexesForCohortBaseValue() {
        return [
            {
                unique: true,
                fields: ["cohort_number"],
            }
        ];
    }
    getIndexesForCohortTable() {
        return [
            {
                unique: true,
                fields: ["cohort_number"],
            }
        ];
    }
    getIndexesForCohortMetrixTable() {
        return [
            {
                concurrently: true,
                unique: true,
                fields: ["cohort_number", "week_number"],
            }
        ];
    }
}
exports.default = CohortAnalytics;
;
console.log('Start populating cohort data');
new CohortAnalytics().populate().then(() => {
    console.log('Data population completed');
});

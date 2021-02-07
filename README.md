# Aave V2 Protocol User Retention Cohort Analysis

Implement User retention dashboard for Aave V2 protocol with below features:
* Cohort analysis of new users on week by week basis.
* Track users with repeat transactions.
* Funnel analysis of repeat users to track user conversions.

![Aave V2 User Retention Analysis](https://github.com/napolean0/aave-user-retention-analytics/blob/main/src/img/cohort_analysis.png)
![Aave V2 Funnel Analysis](https://github.com/napolean0/aave-user-retention-analytics/blob/main/src/img/funnel_analysis.png)

You can view dashboard from here: [Aave V2 dashboard](https://analytics.dappquery.com/public/dashboard/4bb1aed2-d1c4-4da2-9d9c-5cd2de020047).

## What is Cohort Analysis

A cohort analysis on a group of users who share a common characteristic over a certain period of time. Cohort analysis is the study of these common characteristics of these users over a specific period. To boost users retention it's critical to do cohort analysis.

## Why Cohort Analysis?

Cohort analysis is a very important for Defi protocols like Aave v2. It's important to keep track of repeat users and understand what makes them happy. This analysis can be used to identify the success of feature adoption rate and churn rates.

## Why Funnel Analysis?

A funnel report gives information on how Aave user progress through different stages and where they drop off. Funnel analysis help identify barriers that cause users to leave before reaching a conversion point.

## Aave V2 Subgraph

Data is pulled from [Aave V2 subgraph](https://api.thegraph.com/subgraphs/name/aave/protocol-v2).

Entities Fetched:
* UserTransaction
* Deposit

Subgraph data is tranformed to SQL tables for efficient query.

## Aave V2 User Retention Dashboard

* Data is pulled from subgraph and transformed to SQL tables. Below tables are populated:
    * cohorts
    * cohort_base_values
    * cohort_metrics
* A cohort is specified by week start and week end data.
* Cohort start date is taken when Aave V2 protocol was launched.
* User enters into a cohort based on his first transaction date. User can only be part of 1 cohort at a time.
* In a cohort, user is counted only once, even he makes multiple transactions.
* User retention is calculated based on his transaction activity week by week basis.

## Funnel Anlaysis
* Users are put in different categories like User making 2+ transactions, Users making 16+ transactions.
* Users made 32+ and 64+ transactions have been categorized to mini whales and whales.
* Query is run on user_transactions table to group users in different categories. Temporary table concept is used in SQL while constructing the query.
* Funnel chart is prepared using different category and the counts.

## Technology Stack

* Typescript
* Postgres database
* Sequelize Postgres client
* SQL
* moment for datetime parsing
* Dashboard and code is hosted on AWS

## How to set up

```
git clone git@github.com:napolean0/aave-user-retention-analytics.git
cd  aave-user-retention-analytics
npm install
npm run PopulateCohortData
```

## SQL Query

Refer src/user_retention.sql for query to use.

## Contributing

If you want to contribute to this project, feel free to fork the project and open a PR.

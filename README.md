# Real-Time Join & Forecasting with Confluent Cloud

**Use case:** picture an online trading platform. Stock trades are streaming in constantly, and the team wants two things live: who's trading and where they're from (a **join**), and which stocks are surging in activity so they can spot momentum as it builds (a **forecast**). In this lab you'll build exactly that — entirely inside Confluent Cloud, no Terraform, no local setup. Total time: ~45–50 minutes, including sign-up.

---

## 1. Sign Up

1. Go to `confluent.cloud/signup` and create an account with your email.
   ![Sign up screen](screenshots/01-signup.png)
2. Verify your email and log in to the Confluent Cloud console.
   ![Confluent Cloud homepage after login](screenshots/02-login.png)

---

## 2. Create a Cluster

1. On the **Environments** page, open the `default` environment that came with your account.
   ![Default environment](screenshots/02b-environment.png)
2. On the **Create cluster** page, keep the default configuration — **Standard** cluster, **AWS**, region **us-east-2** — and click **Continue**.
   
   ![Create cluster](screenshots/03-cluster-create.png)
3. On the **Enter payment information** screen, add your card details — you won't be charged until your free trial ends — then click **Submit**.
   
   <img src="screenshots/04-payment-info.png" width="360" alt="Enter payment information">

> [!TIP]
> **You won't be charged.** Every new Confluent Cloud signup includes **$400 in free credit**, which more than covers this workshop. A card is only required to activate your account.

4. Back on the **Create cluster** page, click **Launch cluster** — it shows **Running** once ready.
   ![Cluster running](screenshots/04c-cluster-running.png)

---

## 3. Generate Data Sources

1. From your cluster, open **Connectors** and click **Add Connector**.
   ![Connectors page](screenshots/05a-connectors-page.png)
2. Choose the **Sample Data** (Datagen Source) connector and click **Get started**.
   ![Sample Data connector](screenshots/05b-sample-data-plugin.png)
3. Select the **Users** template — it writes to the `sample_data_users` topic — then click **Launch**.
   <img src="screenshots/05-connector-users.png" width="420" alt="Launch Users sample data">
4. Add another connector the same way, select the **Stock trades** template — it writes to `sample_data_stock_trades` — then click **Launch**.
   <img src="screenshots/06-connector-stock-trades.png" width="420" alt="Launch Stock trades sample data">
5. Wait until both connectors show **Running**.
   ![Both connectors running](screenshots/06b-connectors-running.png)
6. From the left nav, open **Topics**.

   <img src="screenshots/06c-topics-nav.png" width="180" alt="Topics nav">
7. Click **sample_data_users**, then open the **Messages** tab to view live user records.
   ![Users topic messages](screenshots/07-topic-users.png)
8. Click **sample_data_stock_trades**, open the **Messages** tab, and note the shared `userid` field.
   ![Stock trades topic messages](screenshots/08-topic-stock-trades.png)

---

## Lab 2: Enrich & Forecast Live Trades with Flink

With trades and customer data now streaming, use Flink SQL to answer the two business questions from the use case: **who is trading and from where** (enrichment), and **which stocks are heating up** (forecast) — both computed continuously on the live streams.

### Step 1: Enrich Live Trades with Customer Context

1. [Open Flink](https://confluent.cloud/go/flink), select the **default** environment, and click **Continue**.

   <img src="screenshots/09-flink-navigate.png" width="420" alt="Navigate to Flink compute pools">
2. On the **Compute pools** tab, click **SQL Workspace** on the default pool (created for you automatically).
   <img src="screenshots/09b-compute-pool.png" width="600" alt="Flink compute pool">
3. In the workspace, set **Use catalog** to `default` and **Use database** to `cluster_0` so your topics resolve as tables.
   <img src="screenshots/09c-workspace-catalog.png" width="600" alt="Set catalog and database">
4. `sample_data_users` from Datagen is an append-only stream, so first key it into a lookup table that keeps the latest row per user using a [materialized table](https://docs.confluent.io/cloud/current/flink/reference/statements/create-materialized-table.html):

   ```sql
   CREATE MATERIALIZED TABLE users_keyed (
     userid STRING NOT NULL,
     regionid STRING,
     gender STRING,
     PRIMARY KEY (userid) NOT ENFORCED
   ) AS
   SELECT COALESCE(userid, '') AS userid, regionid, gender FROM sample_data_users;
   ```

> [!TIP]
> A **materialized table** bundles a table and its continuous query into one object — define it once, with no separate `INSERT INTO` to manage. Better yet, you can evolve its logic in place with `CREATE OR ALTER MATERIALIZED TABLE` (change the query, add columns) instead of tearing statements down and rebuilding them.

5. Enrich each trade with its user's region and gender using a temporal join, and store the result to a `trades_enriched` topic that the next lab will forecast on:

   ```sql
   CREATE MATERIALIZED TABLE trades_enriched AS
   SELECT
     t.userid,
     t.symbol,
     t.side,
     t.quantity,
     t.price,
     u.regionid,
     u.gender
   FROM sample_data_stock_trades t
   JOIN users_keyed FOR SYSTEM_TIME AS OF t.`$rowtime` AS u
     ON t.userid = u.userid;
   ```

6. Query the enriched stream to confirm each trade now carries its customer's region and gender:

   ```sql
   SELECT * FROM trades_enriched;
   ```

   <img src="screenshots/10b-trades-enriched-query.png" width="600" alt="Query trades_enriched">

---

### Step 2: Forecast Trades per Stock

`ML_FORECAST` needs a real time series (a numeric value per timestamp), so window `trades_enriched` into a per-stock trade count every 10 seconds and forecast each symbol on its own. The inner query tumbles trades into `trade_count` per `symbol`, and `ML_FORECAST` — partitioned by `symbol` — projects where each stock's activity is heading (`minTrainingSize` is set to 10, applied per symbol, so a forecast appears once a stock has ~10 windows of history).

1. Create the forecast as a materialized table so the projections persist to a topic:

   ```sql
   CREATE MATERIALIZED TABLE trades_forecast AS
   SELECT
     window_start,
     symbol,
     ML_FORECAST(
       CAST(trade_count AS DOUBLE),
       window_start,
       JSON_OBJECT('minTrainingSize' VALUE 10, 'horizon' VALUE 5)
     ) OVER (
       PARTITION BY symbol
       ORDER BY window_time
       RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
     ) AS forecast
   FROM (
     SELECT window_start, window_time, symbol, COUNT(*) AS trade_count
     FROM TABLE(
       TUMBLE(TABLE trades_enriched, DESCRIPTOR($rowtime), INTERVAL '10' SECONDS)
     )
     GROUP BY window_start, window_end, window_time, symbol
   );
   ```

2. Inspect the output to see the forecasted trade count for each stock:

   ```sql
   SELECT * FROM trades_forecast;
   ```

   <img src="screenshots/12-flink-forecast-result.png" width="600" alt="Forecast output">

# Real-Time Join & Forecasting with Confluent Cloud

**Use case:** picture an online trading platform. Stock trades are streaming in constantly, and the team wants two things live: who's trading and where they're from (a **join**), and a heads-up before trading volume spikes so they can scale capacity ahead of the surge (a **forecast**). In this lab you'll build exactly that — entirely inside Confluent Cloud, no Terraform, no local setup. Total time: ~45–50 minutes, including sign-up.

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
   ![Topics nav](screenshots/06c-topics-nav.png)
7. Click **sample_data_users**, then open the **Messages** tab to view live user records.
   ![Users topic messages](screenshots/07-topic-users.png)
8. Click **sample_data_stock_trades**, open the **Messages** tab, and note the shared `userid` field.
   ![Stock trades topic messages](screenshots/08-topic-stock-trades.png)

Both templates generate a `userid` in the same `User_1`–`User_9` range — that's what makes them joinable in the next lab.

---

## Lab 2 — Step 1: Flink Join

1. Open **Flink → SQL Workspace** (create a compute pool if prompted).
   ![Flink SQL workspace](screenshots/09-flink-workspace.png)
2. `sample_data_users` from Datagen is an append-only stream, so first key it into a lookup table that keeps the latest row per user:

   ```sql
   CREATE TABLE users_keyed (
     userid STRING,
     regionid STRING,
     gender STRING,
     PRIMARY KEY (userid) NOT ENFORCED
   );

   INSERT INTO users_keyed
   SELECT userid, regionid, gender FROM sample_data_users;
   ```

3. Enrich each trade with its user's region and gender using a temporal join, and store the result to a `trades_enriched` topic that the next lab will forecast on:

   ```sql
   CREATE TABLE trades_enriched (
     userid STRING,
     symbol STRING,
     side STRING,
     quantity INT,
     price INT,
     regionid STRING,
     gender STRING
   );

   INSERT INTO trades_enriched
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

   ![Enriched trades topic](screenshots/10-flink-join-result.png)

---

## Lab 2 — Step 2: Flink Built-in Forecasting Model

`ML_FORECAST` needs a real time series (a numeric value per timestamp), so first turn the `trades_enriched` stream from Step 1 into a windowed volume with a watermark it can order by:

1. Create a windowed-volume table and stream 10-second trading volumes (shares traded) into it:

   ```sql
   CREATE TABLE trades_windowed (
     window_start TIMESTAMP(3) NOT NULL,
     total_quantity BIGINT,
     WATERMARK FOR window_start AS window_start
   );

   INSERT INTO trades_windowed
   SELECT window_start, SUM(quantity) AS total_quantity
   FROM TABLE(
     TUMBLE(TABLE trades_enriched, DESCRIPTOR($rowtime), INTERVAL '10' SECONDS)
   )
   GROUP BY window_start, window_end;
   ```

   ![Windowed trade volume](screenshots/11-flink-windowed-counts.png)

2. Run `ML_FORECAST` over that time series to project the next 5 windows of trading volume (`minTrainingSize` is set to 10 so a forecast appears within ~2 minutes instead of the default 128 windows):

   ```sql
   SELECT
     window_start,
     ML_FORECAST(
       CAST(total_quantity AS DOUBLE),
       window_start,
       JSON_OBJECT('minTrainingSize' VALUE 10, 'horizon' VALUE 5)
     ) OVER (
       ORDER BY window_start
       RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
     ) AS forecast
   FROM trades_windowed;
   ```

   ![Forecast output](screenshots/12-flink-forecast-result.png)

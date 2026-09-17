# Real-Time Join & Forecasting with Confluent Cloud

**Use case:** picture a retail website during a flash sale. Pageviews are streaming in constantly, and the team watching it wants two things live: who's browsing and where they're from (a **join**), and a heads-up before traffic spikes so they can scale up before the site slows down (a **forecast**). In this lab you'll build exactly that — entirely inside Confluent Cloud, no Terraform, no local setup. Total time: ~45–50 minutes, including sign-up.

---

## 1. Sign Up

1. Go to `confluent.cloud/signup` and create an account with your email.
   ![Sign up screen](screenshots/01-signup.png)
2. Verify your email and log in to the Confluent Cloud console.
   ![Confluent Cloud homepage after login](screenshots/02-login.png)

---

## 2. Create a Cluster

1. Click **Create cluster**, choose the **Basic** cluster type.
   ![Choose cluster type](screenshots/03-cluster-type.png)
2. Select cloud provider **AWS** and region **us-east-1**, then launch it.
   ![Select AWS us-east-1](screenshots/04-cluster-region.png)

---

## 3. Create the Datagen Connectors

1. In your cluster, go to **Connectors → Datagen Source**, and create one using the **Users** quickstart template.
   ![Datagen connector - Users template](screenshots/05-connector-users.png)
2. Create a second Datagen Source connector using the **Pageviews** quickstart template.
   ![Datagen connector - Pageviews template](screenshots/06-connector-pageviews.png)

Both templates generate a `userid` in the same `User_1`–`User_9` range — that's what makes them joinable in the next lab.

---

## 4. Explore the Data

1. Open **Topics → users** and view live messages.
   ![Users topic messages](screenshots/07-topic-users.png)
2. Open **Topics → pageviews** and view live messages — note the shared `userid` field.
   ![Pageviews topic messages](screenshots/08-topic-pageviews.png)

---

## Lab 2 — Step 1: Flink Join

1. Open **Flink → SQL Workspace** (create a compute pool if prompted).
   ![Flink SQL workspace](screenshots/09-flink-workspace.png)
2. Run this query to enrich each pageview with the user's region and gender:

   ```sql
   SELECT
     p.userid,
     p.pageid,
     p.viewtime,
     u.regionid,
     u.gender
   FROM pageviews p
   JOIN users u
     ON p.userid = u.userid;
   ```

   ![Join query results](screenshots/10-flink-join-result.png)

---

## Lab 2 — Step 2: Flink Built-in Forecasting Model

`ML_FORECAST` needs a real time series (a numeric value per timestamp), so first turn raw pageviews into a windowed count:

1. Create a 1-minute tumbling-window view of pageview volume:

   ```sql
   CREATE TABLE pageviews_per_minute AS
   SELECT
     window_start,
     window_end,
     COUNT(*) AS pageview_count
   FROM TABLE(
     TUMBLE(TABLE pageviews, DESCRIPTOR($rowtime), INTERVAL '1' MINUTE)
   )
   GROUP BY window_start, window_end;
   ```

   ![Windowed pageview counts](screenshots/11-flink-windowed-counts.png)

2. Run `ML_FORECAST` over that time series to project the next 5 minutes of pageview volume:

   ```sql
   SELECT
     window_start,
     ML_FORECAST(
       CAST(pageview_count AS DOUBLE),
       window_start,
       JSON_OBJECT('horizon' VALUE 5)
     ) OVER (
       ORDER BY window_start
       RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
     ) AS forecast
   FROM pageviews_per_minute;
   ```

   ![Forecast output](screenshots/12-flink-forecast-result.png)

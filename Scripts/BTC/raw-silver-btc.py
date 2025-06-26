import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col, to_date, regexp_replace, round
from pyspark.sql.window import Window

# Parametri
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session


# --- Path configurati---
RAW_BUCKET = "s3://crypto-raw-bucket"
SILVER_BUCKET = "s3://crypto-silver"

BTC_RAW_PATH = f"{RAW_BUCKET}/BTC_EUR_Historical_Data.csv"
GT_BTC_RAW_PATH = f"{RAW_BUCKET}/google_trend_bitcoin.csv"

BTC_SILVER_PATH = f"{SILVER_BUCKET}/btc"
GT_BTC_SILVER_PATH = f"{SILVER_BUCKET}/gt_bitcoin"


# --- Funzioni ETL ---
def load_csv(path: str):
    dynf = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={"paths": [path]},
        format="csv",
        format_options={"withHeader": True}
    )
    return dynf.toDF()


def clean_price_df(df, date_col="Date", price_col="Price"):
    df = df.withColumn(price_col, regexp_replace(col(price_col), ",", "").cast("float"))
    df = df.withColumn(date_col, to_date(col(date_col), "MM/dd/yyyy"))

    window = Window.orderBy(date_col)
    df = df.withColumn(
        price_col,
        F.when((col(price_col).isNull()) | (col(price_col) == -1),
               (F.lag(price_col).over(window) + F.lead(price_col).over(window)) / 2)
        .otherwise(col(price_col))
    )

    df = df.withColumn(price_col, round(col(price_col), 3))
    return df


def clean_trend_df(df, date_col="Settimana", value_col="interesse bitcoin"):
    return df.withColumn(date_col, col(date_col).cast("date")) \
             .withColumn(value_col, col(value_col).cast("float"))


def save_parquet(df, output_path):
    dynf = DynamicFrame.fromDF(df, glueContext, "parquet_out")
    glueContext.write_dynamic_frame.from_options(
        frame=dynf,
        connection_type="s3",
        connection_options={"path": output_path},
        format="parquet"
    )


# --- Esecuzione ETL ---
btc_raw_df = load_csv(BTC_RAW_PATH)
gt_btc_raw_df = load_csv(GT_BTC_RAW_PATH)

btc_clean_df = clean_price_df(btc_raw_df)
gt_btc_clean_df = clean_trend_df(gt_btc_raw_df)

save_parquet(btc_clean_df, BTC_SILVER_PATH)
save_parquet(gt_btc_clean_df, GT_BTC_SILVER_PATH)

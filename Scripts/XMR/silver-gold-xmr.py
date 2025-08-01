import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, avg
from pyspark.sql.window import Window

# Parametri
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session


# --- Percorsi Silver e Gold ---
SILVER_BUCKET = "s3://crypto-silver"
GOLD_BUCKET = "s3://crypto-golden"

XMR_SILVER_PATH = f"{SILVER_BUCKET}/xmr"
GT_XMR_SILVER_PATH = f"{SILVER_BUCKET}/gt_monero"
XMR_GOLD_PATH = f"{GOLD_BUCKET}/xmr_with_trend"


# --- Funzioni di supporto ---
def load_parquet(path):
    dynf = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={"paths": [path]},
        format="parquet"
    )
    return dynf.toDF()


def save_to_gold(df, path):
    dynf = DynamicFrame.fromDF(df, glueContext, "gold_output")
    glueContext.write_dynamic_frame.from_options(
        frame=dynf,
        connection_type="s3",
        connection_options={"path": path},
        format="parquet"
    )


# --- ETL: Caricamento dati Silver ---
xmr_df = load_parquet(XMR_SILVER_PATH)
gt_xmr_df = load_parquet(GT_XMR_SILVER_PATH)

# --- Media mobile a 10 giorni sul prezzo XMR ---
window_spec = Window.orderBy("Date").rowsBetween(-9, 0)  # 10 giorni (incluso il corrente)
xmr_df = xmr_df.withColumn("Price", avg("Price").over(window_spec))

# --- Join tra XMR e Google Trends ---
joined_df = xmr_df.join(gt_xmr_df, xmr_df.Date == gt_xmr_df.Settimana, "inner") \
                  .select(xmr_df.Date, xmr_df.Price, gt_xmr_df["interesse monero"].alias("GoogleTrend"))

# --- Salvataggio su Bucket Gold ---
save_to_gold(joined_df, XMR_GOLD_PATH)

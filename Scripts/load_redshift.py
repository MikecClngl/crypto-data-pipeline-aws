import psycopg2
import pandas as pd
from pyspark.sql import SparkSession

# Parametri di connessione
JDBC_URL = "jdbc:redshift://progettoaws.152317105038.eu-north-1.redshift-serverless.amazonaws.com:5439/dev"
USER = "admin" 
PASSWORD = "JIMFDlnbt792"  
PORT = "5439"
DATABASE = "dev"

# Funzione per connettersi a Redshift e caricare i dati
def connect_to_redshift():
    conn = psycopg2.connect(
        dbname=DATABASE,
        user=USER,
        password=PASSWORD,
        host="progettoaws.152317105038.eu-north-1.redshift-serverless.amazonaws.com",
        port=PORT
    )
    return conn

# Caricamento dei dati da S3 (per Bitcoin e Monero)
def load_data_from_s3_to_redshift():
    # Inizializza una sessione Spark per caricare i dati da Parquet su S3
    spark = SparkSession.builder.appName("S3-to-Redshift").getOrCreate()

    # Carica i dati da S3 per Bitcoin e Monero (nel bucket crypto-golden)
    btc_df = spark.read.parquet("s3://crypto-golden/btc_with_trend")
    xmr_df = spark.read.parquet("s3://crypto-golden/xmr_with_trend")

    # Converte i DataFrame Spark in Pandas DataFrame per inviarli a Redshift
    btc_pd = btc_df.toPandas()
    xmr_pd = xmr_df.toPandas()

    # Connessione a Redshift
    conn = connect_to_redshift()
    cursor = conn.cursor()

    # Creazione delle tabelle (se non esistono già)
    create_btc_table_query = """
    CREATE TABLE IF NOT EXISTS btc_with_trend (
        Date DATE,
        Price FLOAT,
        GoogleTrend FLOAT
    );
    """
    cursor.execute(create_btc_table_query)

    create_xmr_table_query = """
    CREATE TABLE IF NOT EXISTS xmr_with_trend (
        Date DATE,
        Price FLOAT,
        GoogleTrend FLOAT
    );
    """
    cursor.execute(create_xmr_table_query)

    conn.commit()

    # Inserimento dei dati da Pandas DataFrame a Redshift per Bitcoin
    for index, row in btc_pd.iterrows():
        insert_btc_query = """
        INSERT INTO btc_with_trend (Date, Price, GoogleTrend)
        VALUES (%s, %s, %s)
        """
        cursor.execute(insert_btc_query, (row['Date'], row['Price'], row['GoogleTrend']))

    # Inserimento dei dati da Pandas DataFrame a Redshift per Monero
    for index, row in xmr_pd.iterrows():
        insert_xmr_query = """
        INSERT INTO xmr_with_trend (Date, Price, GoogleTrend)
        VALUES (%s, %s, %s)
        """
        cursor.execute(insert_xmr_query, (row['Date'], row['Price'], row['GoogleTrend']))

    conn.commit()

    # Chiude la connessione
    cursor.close()
    conn.close()

# Esegui il caricamento
load_data_from_s3_to_redshift()

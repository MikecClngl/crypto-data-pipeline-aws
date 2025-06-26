# E2E AWS Pipeline - CryptoData Insights

## 📌 Descrizione del Progetto

Questo progetto implementa una pipeline End-to-End (E2E) su AWS per l'analisi delle criptovalute Bitcoin (BTC) e Monero (XMR), con l'obiettivo di trasformare dati grezzi in insight pronti per l'analisi su Amazon Redshift e la visualizzazione con Amazon QuickSight.

## 🗂️ Struttura delle Cartelle

```
E2E AWS Pipeline/
│
├── E2E_AWS_pipeline.pdf             # Documento di progetto con spiegazioni dettagliate
│
├── Datasets/                        # File grezzi CSV delle criptovalute
│   ├── Bitcoin/
│   │   ├── BTC_EUR_Historical_Data.csv
│   │   └── google_trend_bitcoin.csv
│   └── Monero/
│       ├── XMR_EUR_Historical_Data.csv
│       └── google_trend_monero.csv
│
└── Scripts/                         # Codici per l’ETL e il caricamento dati
    ├── BTC/
    │   ├── raw-silver-btc.py        # ETL da raw a silver per BTC
    │   └── silver-gold-btc.py       # ETL da silver a gold per BTC
    ├── XMR/
    │   ├── raw-silver-xmr.py        # ETL da raw a silver per XMR
    │   └── silver-gold-xmr.py       # ETL da silver a gold per XMR
    ├── load_redshift.py            # Script per caricare i dati gold su Redshift
    └── step_functions.json         # Definizione orchestrazione AWS Step Functions
```

## ⚙️ Servizi AWS Utilizzati

- **Amazon S3**: Storage dei dati grezzi, silver e gold.
- **AWS Glue**: Pulizia, trasformazione ed ETL dei dati.
- **AWS Step Functions**: Orchestrazione automatica dei job Glue.
- **Amazon Redshift**: Data warehouse per query e analisi.

## 🔄 Flusso della Pipeline

1. **Caricamento dati grezzi su S3**
2. **Pulizia dei dati (raw → silver)**
3. **Trasformazione con media mobile e join (silver → gold)**
4. **Caricamento delle tabelle BTC/XMR su Amazon Redshift**
5. **Orchestrazione completa tramite Step Functions**

## 🚀 Istruzioni per l'Esecuzione

1. **Carica i file CSV** in `Datasets/` su un bucket S3 chiamato `crypto-raw-bucket`.
2. **Avvia i job Glue** `raw-silver-btc` e `raw-silver-xmr`, poi `silver-gold-btc` e `silver-gold-xmr`.
3. **Esegui lo script `load_redshift.py`** per caricare i file Parquet da `crypto-golden` su Amazon Redshift.

## ✅ Risultati Attesi

- Tabelle `btc_with_trend` e `xmr_with_trend` su Redshift pronte per l’analisi.
- Dati puliti, con media mobile e indicatore Google Trends.
- Orchestrazione visibile e monitorabile tramite AWS Step Functions.
class Config:
    RAW_PATH = "D:\\DE_project\\data\\raw\\"  # Landing zone (bronze)
    BRONZE_PATH = "D:\\DE_project\\data\\bronze\\"
    SILVER_PATH = "D:\\DE_project\\data\\silver\\"
    GOLD_PATH = "D:\\DE_project\\data\\gold\\"

    CUSTOMER_FILE = RAW_PATH + "customers.csv"
    ACCOUNT_FILE = RAW_PATH + "accounts.csv"
    TRANSACTION_FILE = RAW_PATH + "transactions.csv"

    ENV_NAME = "DEV"

    # Add database or delta catalog if needed
    DELTA_DB = "financial_db_dev"

    # Fraud detection thresholds
    FRAUD_AMOUNT_THRESHOLD = 5000
    FRAUD_MERCHANTS = ["UnusualMerchant1", "UnusualMerchant2"]

    # Spark settings or other parameters could go here

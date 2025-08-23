import pytest
from pyspark.sql import SparkSession
from src.validation import DataValidation

@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder.master("local").config("spark.driver.host", "127.0.0.1").appName("test").getOrCreate()
    yield spark
    spark.stop()

def test_validate_account_ids_pass(spark):
    accounts = spark.createDataFrame([("ACC1",), ("ACC2",)], ["account_id"])
    transactions = spark.createDataFrame([("ACC1",), ("ACC2",)], ["account_id"])
    validator = DataValidation(spark, None)
    validator.validate_account_ids(transactions, accounts)  # should pass without error

def test_validate_account_ids_fail(spark):
    accounts = spark.createDataFrame([("ACC1",)], ["account_id"])
    transactions = spark.createDataFrame([("ACC1",), ("ACC2",)], ["account_id"])
    validator = DataValidation(spark, None)
    with pytest.raises(ValueError):
        validator.validate_account_ids(transactions, accounts)

def test_validate_balances(spark):
    accounts = spark.createDataFrame([
        ("ACC1", "Savings", 1000),
        ("ACC2", "Checking", -50),
        ("ACC3", "Loan", -200)
    ], ["account_id", "account_type", "balance"])
    validator = DataValidation(spark, None)
    with pytest.raises(ValueError):
        validator.validate_balances(accounts)

def test_validate_transactions_amount(spark):
    transactions = spark.createDataFrame([
        ("TX1", 100),
        ("TX2", 0),
        ("TX3", None),
    ], ["transaction_id", "amount"])
    validator = DataValidation(spark, None)
    filtered = validator.validate_transactions_amount(transactions)
    # Should only keep TX1 record
    assert filtered.count() == 1

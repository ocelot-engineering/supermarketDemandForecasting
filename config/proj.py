"""
Global configuration for the project
"""


# Global config --------------------------------------------------------------------------------------------------------
class Config:
    from pathlib import Path
    from re import sub

    # Paths for directories and files
    paths = {"top": Path("/Users/Patrick/PycharmProjects/supermarketDemandForecasting/")}
    paths["src"] = paths["top"].joinpath("src")
    paths["data"] = paths["top"].joinpath("data")
    paths["data_raw"] = paths["data"].joinpath("raw")
    paths["data_interim"] = paths["data"].joinpath("interim")
    paths["data_proc"] = paths["data"].joinpath("processed")
    paths["schema_dir"] = paths["src"].joinpath("data").joinpath("schemas")

    # Data config (includes file to schema mapping)
    data = {
        "schema_package": "src.data.schemas",  # TODO: derive from path above
        "schema_map": {
            "holidays_events.csv": "schema_holidays_events",
            "items.csv": "schema_items",
            "oil.csv": "schema_oil",
            "sample_submission.csv": "schema_sample_submission",
            "stores.csv": "schema_stores",
            "test.csv": "schema_test",
            "train.csv": "schema_train",
            "transactions.csv": "schema_transactions"
        }
    }

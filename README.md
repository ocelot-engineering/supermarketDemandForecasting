# Supermarket Demand Forecasting
This project is based on the 2017 Kaggle competition: 
[Corporación Favorita Grocery Sales Forecasting](https://www.kaggle.com/competitions/favorita-grocery-sales-forecasting), where the goal  was to develop a model that can accurately predict product sales for Corporación Favorita supermarkets.

## File Structure
```
├── config             <- Configurations for project. py and yaml files.
│   └── proj.py        <- Project configuration, e.g. project paths.  
├── data               <- Data storage. (git ignored)
│   ├── outputs        <- Model outputs (scored data sets).
│   ├── interim        <- Intermediate data that has been transformed.
│   ├── processed      <- The final, canonical data sets for modeling.
│   └── raw            <- The original, immutable data dump.
├── models             <- Trained and serialized models, model predictions, or model summaries. (git ignored)
├── src                <- Source code for use in this project.
│   ├── data           <- Scripts to download or generate data.
│   ├── eda            <- Scripts and notebooks for exploratory data analysis.
│   ├── features       <- Scripts to turn raw data into features for modeling.
│   ├── models         <- Scripts to train models and make predictions.
│   │   │── train      <- Scripts to train models.
│   │   └── score      <- Scripts to score models.
│   └── utils          <- Scripts with utility functions defined.
├── requirements.txt   <- Dependencies for project.
└── README.md          <- The top-level README for developers using this project.
```
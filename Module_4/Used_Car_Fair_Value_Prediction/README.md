# Used Car Fair Value Prediction

A regression project for estimating the fair market value of used cars while taking business constraints into account.

The project focuses not only on predictive accuracy but also on practical considerations such as model training speed, prediction latency and suitability for production use.

## Problem

A used-car marketplace needs a model that can estimate the fair price of a vehicle based on its characteristics.

An accurate pricing model can help:

* provide users with realistic price recommendations;
* detect significantly overpriced or underpriced listings;
* improve marketplace liquidity;
* automate part of the vehicle valuation process.

## Machine Learning Task

**Type:** Regression

**Target:** Used-car price

The goal is to predict vehicle value using historical listings and vehicle characteristics.

## Project Workflow

The project includes:

1. Exploratory data analysis
2. Data quality assessment
3. Missing value processing
4. Outlier analysis
5. Feature preprocessing
6. Train/test splitting
7. Baseline modeling
8. Gradient boosting models
9. Hyperparameter optimization
10. Model comparison
11. Analysis of predictive quality and business constraints

## Models

The project compares several machine learning approaches, including gradient boosting libraries commonly used for structured/tabular data.

Models explored include:

* Decision Trees
* Random Forest
* XGBoost
* CatBoost
* LightGBM

## Model Evaluation

Regression models are compared primarily using prediction-error metrics.

In addition to predictive quality, practical factors such as the following are considered:

* model training time;
* inference speed;
* computational complexity;
* suitability for repeated predictions.

This reflects a production-oriented approach where the model with the lowest error is not always automatically the best business solution.

## Technology Stack

* Python
* pandas
* NumPy
* scikit-learn
* XGBoost
* CatBoost
* LightGBM
* Optuna
* Matplotlib
* Seaborn

## Key Skills Demonstrated

* Regression modeling
* Gradient boosting
* Feature preprocessing
* Handling missing values
* Outlier analysis
* Hyperparameter optimization
* Model benchmarking
* Business-oriented model selection
* Performance comparison between boosting frameworks

## Business Perspective

For a pricing service, model quality should be evaluated together with operational constraints.

For example, a small improvement in prediction accuracy may not justify a substantial increase in inference latency or computational cost.

The final model should therefore represent a balance between:

**prediction quality → training cost → inference speed → maintainability**

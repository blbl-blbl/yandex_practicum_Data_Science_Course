# ML Pipeline for Sales Forecasting

An end-to-end machine learning pipeline for forecasting sales using batch-level product characteristics.

The project demonstrates the transition from an experimental machine learning workflow to an automated pipeline that includes data preparation, feature engineering, model training and prediction generation.

## Overview

Building an accurate model is only one part of a production machine learning system.

The objective of this project is to organize the complete ML workflow so that individual stages can be executed automatically and reproducibly.

The project combines classical Data Science techniques with ML Engineering practices.

## Problem

The goal is to forecast sales based on historical data and batch-level characteristics.

The solution must process incoming data, construct the required features, train a model and produce predictions through an automated workflow.

## ML Task

**Type:** Supervised machine learning / forecasting

The predictive model is trained using historical observations and engineered features describing the available data.

## Pipeline Architecture

The workflow follows an end-to-end machine learning pipeline:

```text
Raw Data
   ↓
Data Loading
   ↓
Data Validation
   ↓
Preprocessing
   ↓
Feature Engineering
   ↓
Training Dataset
   ↓
Model Training
   ↓
Model Evaluation
   ↓
Model Artifact
   ↓
Prediction Pipeline
   ↓
Sales Forecast
```

Pipeline stages are orchestrated with Apache Airflow.

## Project Workflow

The project includes:

1. Data ingestion
2. Data preprocessing
3. Feature generation
4. Training dataset creation
5. Model training
6. Hyperparameter optimization
7. Model evaluation
8. Model serialization
9. Automated pipeline execution
10. Prediction generation
11. Logging and monitoring of pipeline stages

## Machine Learning

The project uses machine learning models suitable for structured/tabular data.

CatBoost is used as one of the primary modeling tools due to its strong performance on heterogeneous tabular datasets.

Hyperparameter optimization is performed using Optuna.

## Feature Analysis

The project also applies techniques for investigating dataset structure and model behavior.

Methods and tools include:

* feature importance;
* SHAP values;
* PCA;
* t-SNE;
* clustering techniques such as K-Means.

These methods help analyze the underlying structure of the data and understand model behavior.

## Orchestration with Apache Airflow

Apache Airflow is used to organize the ML process as a collection of dependent tasks.

Conceptually, the DAG follows a structure similar to:

```text
              ┌───────────────┐
              │   Load Data   │
              └───────┬───────┘
                      ↓
              ┌───────────────┐
              │ Preprocessing │
              └───────┬───────┘
                      ↓
              ┌───────────────┐
              │   Features    │
              └───────┬───────┘
                      ↓
              ┌───────────────┐
              │ Train Model   │
              └───────┬───────┘
                      ↓
              ┌───────────────┐
              │   Evaluate    │
              └───────┬───────┘
                      ↓
              ┌───────────────┐
              │ Save Artifacts│
              └───────────────┘
```

This architecture makes the workflow reproducible and allows individual stages to be monitored independently.

## Storage & Artifacts

The pipeline works with external storage and model artifacts.

`boto3` is used for interaction with S3-compatible object storage.

Typical artifacts generated during the ML workflow may include:

* prepared datasets;
* fitted preprocessing components;
* trained models;
* evaluation results;
* prediction files.

## Logging

Logging is used throughout the workflow to make pipeline execution observable.

This is important for production ML systems because failures should be traceable to a particular processing stage.

## Technology Stack

### Data Science

* Python
* pandas
* NumPy
* scikit-learn
* CatBoost
* Optuna
* SHAP
* PCA
* t-SNE
* K-Means

### ML Engineering

* Apache Airflow
* boto3
* SQLAlchemy
* Python logging
* Model serialization

## Key Skills Demonstrated

* End-to-end ML pipeline development
* Workflow orchestration
* Feature engineering
* Model training
* Hyperparameter optimization
* ML artifact management
* Object storage integration
* Logging
* Model interpretation
* Reproducible ML workflows

## Why This Project Matters

Notebook-based experimentation is useful during research, but production ML requires additional infrastructure.

This project demonstrates the difference between:

```text
Experimental ML

data → notebook → model
```

and a more production-oriented approach:

```text
Production-oriented ML

data
 ↓
automated preprocessing
 ↓
feature pipeline
 ↓
training
 ↓
evaluation
 ↓
artifact storage
 ↓
scheduled execution
 ↓
predictions
```

The project therefore combines Data Science with practical ML Engineering concepts.

## Possible Further Improvements

A production version of the system could be extended with:

* automated data-quality checks;
* experiment tracking with MLflow;
* model registry;
* model versioning;
* data-drift monitoring;
* model-performance monitoring;
* automated retraining triggers;
* CI/CD for Airflow DAGs;
* Dockerized execution environment;
* unit and integration tests.

## Project Structure

The project contains the code and configuration required for the ML pipeline.

```text
ML-Pipeline for sales forecasting structure:
│
├── dags/
│   ├── dag.py
│   └── preprocessing.py
│
├── research_part.ipynb
├── requirements.txt
└── README.md
```


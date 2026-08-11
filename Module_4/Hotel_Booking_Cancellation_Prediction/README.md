# Hotel Booking Cancellation Prediction

A binary classification project for predicting hotel booking cancellations.

The objective is to identify reservations with a high probability of cancellation and use machine learning to support business decisions related to booking management and revenue optimization.

## Problem

Hotel booking cancellations create financial and operational uncertainty.

When a reservation is cancelled, the hotel may lose potential revenue, especially if the room cannot be resold before the scheduled arrival date.

The goal of this project is to build a model capable of identifying bookings with a high cancellation risk.

## Machine Learning Task

**Type:** Binary classification

**Target:** Booking cancellation

The model estimates whether a reservation is likely to be cancelled based on information available about the booking and customer.

## Project Workflow

The project includes:

1. Exploratory data analysis
2. Dataset validation
3. Missing value processing
4. Feature preprocessing
5. Categorical feature encoding
6. Analysis of class balance
7. Train/test splitting
8. Baseline model creation
9. Training nonlinear machine learning models
10. Hyperparameter optimization
11. Classification threshold analysis
12. Business-oriented model evaluation

## Models & Techniques

The project explores nonlinear classification models and ensemble methods suitable for tabular data.

Model comparison considers both conventional ML metrics and the practical cost of prediction errors.

## Model Evaluation

For a cancellation model, different errors can have different business consequences.

Important metrics include:

* ROC-AUC
* Precision
* Recall
* F1-score
* Confusion matrix

The classification threshold can also be adjusted depending on the preferred business trade-off.

## Technology Stack

* Python
* pandas
* NumPy
* scikit-learn
* Optuna
* XGBoost
* CatBoost
* LightGBM
* Matplotlib
* Seaborn
* SHAP

## Key Skills Demonstrated

* Binary classification
* Feature preprocessing
* Ensemble models
* Gradient boosting
* Hyperparameter tuning
* Classification threshold optimization
* Cost-sensitive model evaluation
* Model interpretation
* Business metric analysis

## Business Perspective

A hotel cancellation model can be used as part of a larger revenue-management system.

For example, bookings with a high predicted cancellation probability could trigger additional actions:

```text
Booking
   ↓
Cancellation model
   ↓
Probability of cancellation
   ↓
Risk-based action
```

Possible actions might include:

* requesting additional confirmation;
* adjusting deposit requirements;
* prioritizing communication with high-risk customers;
* improving overbooking strategies.

The model therefore acts as a decision-support component rather than simply producing a binary prediction.

## Model Interpretation

Feature importance and model interpretation techniques can be used to understand which booking characteristics have the strongest relationship with cancellation risk.

This makes the model more useful for both technical analysis and business decision making.

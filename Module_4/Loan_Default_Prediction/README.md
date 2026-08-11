# Loan Default Prediction

A machine learning project focused on predicting loan defaults among bank clients.

The goal is to identify borrowers with an increased probability of default and build a classification model that can support credit-risk decision making.

## Problem

Financial institutions need to estimate the probability that a client will fail to meet their loan obligations.

The objective of this project is to build a binary classification model that separates potentially risky borrowers from reliable clients using available customer and loan-related features.

## Project Workflow

The project includes:

1. Data exploration and quality assessment
2. Missing value analysis and preprocessing
3. Feature analysis
4. Categorical feature encoding
5. Train/test splitting
6. Model training
7. Hyperparameter tuning
8. Model comparison
9. Evaluation using classification metrics
10. Analysis of the final model

## Machine Learning Task

**Type:** Binary classification

**Target:** Loan default

The project pays particular attention to the fact that credit-risk datasets often contain class imbalance, which makes accuracy alone insufficient for model evaluation.

## Models & Techniques

Several approaches are explored and compared during the project, including tree-based models and ensemble methods.

Model quality is evaluated using classification metrics suitable for an imbalanced target.

Typical metrics considered include:

* ROC-AUC
* F1-score
* Precision
* Recall
* Confusion matrix

## Technology Stack

* Python
* pandas
* NumPy
* scikit-learn
* Matplotlib
* Seaborn
* Optuna
* imbalanced-learn

## Key Skills Demonstrated

* Binary classification
* Credit-risk modeling
* Handling imbalanced datasets
* Data preprocessing
* Feature engineering
* Cross-validation
* Hyperparameter optimization
* Threshold-based model evaluation
* Business-oriented model interpretation

## Business Perspective

In credit scoring, different types of errors have different costs.

A false negative may result in issuing a loan to a borrower who later defaults, while a false positive may lead to rejecting a potentially reliable customer.

Because of this, model selection should account not only for overall predictive performance but also for the trade-off between precision and recall.

## Project Structure

```text
Loan Default Prediction
├── Loan_Default_Prediction.ipynb
└── README.md
```


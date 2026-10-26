# Yandex Practicum Data Science — Coursework Progress

This page maps my **self-reported course progress** to the **basic + extended Data Science curriculum (2026 syllabus)** and to code that is actually available in this repository. It is meant to distinguish **studied topics** from **publicly demonstrated work**.

> **Status as of October 2026:** I stopped taking the course before formal completion. I studied the basic-track and extended-track topics **except the recommender-systems sprint**. The **graduation project was not completed**. No completed-program credential is claimed.

## How to read this page

- **Studied + published work:** the subject was covered and at least one related notebook or script is in the repository.
- **Studied; no public artifact:** the topic was covered during the course, but this repository does not demonstrate it with a corresponding project.
- **Not studied / not completed:** do not present it as completed course work.

A published notebook or educational project does **not** by itself establish commercial production experience, real-world deployment or expertise across every subtopic in the relevant syllabus module.

## Basic track

| Curriculum module | Studied topics | Evidence in this repository |
|---|---|---|
| **01 — Data analysis with SQL** | Relational tables and SQL; filtering, grouping, joins, subqueries, set operations, date/time functions, window functions, ad hoc analysis and data marts | [SQL projects](Module_1/) — ad hoc analysis and marketplace data mart |
| **02 — Data analysis with Python** | Python basics, functions and data structures, pandas, cleaning missing values/duplicates, joins, descriptive statistics, EDA, visualization, basic OOP and Git | [Python/EDA projects](Module_2/) — games, catering market, user-loyalty analysis |
| **03 — Machine learning: linear models** | Linear and logistic regression, train/validation/test methodology, cross-validation, regression/classification metrics, SVM, classifier probability calibration and feature selection | [Linear ML projects](Module_3/) — sea turtle mass, churn, banner clicks and visitor-age prediction |
| **04 — Tree-based models** | KNN, decision trees, bagging/random forests, stacking, class imbalance, time-aware validation, gradient boosting, hyperparameter tuning (including Optuna), feature importance and interpretation | [Tree and boosting projects](Module_4/) — bike demand, loan default, used-car pricing and hotel cancellations |
| **05 — Model validation, unsupervised learning and experiments** | PCA, t-SNE, K-Means, DBSCAN; ML lifecycle concepts, Airflow, batch features/inference and monitoring; probability, hypothesis testing, multiple comparisons, A/B analysis, sample sizing, t-tests, Mann–Whitney and two-proportion z-tests; interview preparation | [Unsupervised-learning notebook](Module_5/Forecasting%20order%20delays%20for%20a%20logistics%20operator.ipynb), [Airflow batch features](Module_5/First_AirFlow_Project/), [sales ML pipeline](Module_5/ML%20Pipeline%20for%20sales%20forecasting/), [A/B analysis](A_B%20Tests%20Module/AB_tests.ipynb) |

**Evidence note:** these project links provide examples of work in each module. They do not establish that each individual syllabus item was independently implemented in the files.

## Extended track

| Topic / sprint | Studied topics | Evidence in this repository |
|---|---|---|
| **24 — Big Data processing** | PySpark/Spark architecture, DataFrame transformations, performance/execution plans and S3 data marts | **Studied + published work:** [PySpark ETL pipeline](Big%20Data%20Processing/) |
| **25 — Experiment tracking** | MLflow runs, metrics, parameters, model logging, experiment comparison and versioning concepts | **Studied; no public MLflow project** |
| **26 — Deep-learning fundamentals** | Neural networks, PyTorch training, datasets/training monitoring, embeddings and text classification | **Studied + published work:** [PyTorch text sentiment classification](Deep%20Learning/) |
| **27 — Neural networks for text** | Transformer architecture, BERT/pretrained models, Hugging Face and NER fine-tuning | **Studied; no public BERT/NER project** |
| **28 — Recommender systems** | Recommendation baselines, ranking/evaluation, collaborative filtering, ALS and two-stage recommenders | **Not studied** |
| **Graduation project** | Integrated project / graduation deliverable | **Not completed** |

### Important distinction: features for recommendation vs. recommendation models

The [PySpark ETL project](Big%20Data%20Processing/) generates five feature marts intended for downstream recommendation use cases. It is a **data engineering / feature-preparation project**. It does **not** train a collaborative-filtering model, implement ALS, build a two-stage recommender or report recommendation-ranking metrics.

### Important distinction: learning vs. published proof

- **MLflow** and **BERT / Hugging Face** belong to the self-reported **studied curriculum**. They are not demonstrated by a public project in this repository.
- The [PyTorch project](Deep%20Learning/) demonstrates a relatively simple embedding + mean-pooling text classifier and an MLP baseline; it is **not** a BERT/Transformer implementation.
- The [Airflow work](Module_5/) is educational workflow orchestration. Do not interpret it as independently verified operation of a live commercial production system.

## Skills illustrated by the published portfolio

- **Data analysis:** Python, pandas, NumPy, SQL, joins, window functions, EDA and visualization.
- **Supervised ML:** regression, classification, scikit-learn, ensembles, gradient boosting, model selection and evaluation, class imbalance, hyperparameter optimization.
- **Unsupervised ML and statistics:** clustering/dimensionality reduction, hypothesis testing and A/B-test analysis.
- **Data and ML workflows:** Apache Airflow DAGs, batch feature computation, PySpark transformations and data marts stored as Parquet in S3-compatible storage.
- **Deep learning:** PyTorch training and evaluation of an MLP and an embedding-based text classifier.

For a CV, list **MLflow** and **BERT / Hugging Face** as technologies **studied in coursework** unless supported by further independent examples. Avoid claiming recommender-system development, a Yandex Practicum graduation credential or commercial ML deployment on the basis of this repository.

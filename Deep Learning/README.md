# Text Sentiment Classification with PyTorch

![Python](https://img.shields.io/badge/Python-3.x-3776AB?logo=python&logoColor=white)
![PyTorch](https://img.shields.io/badge/PyTorch-Deep%20Learning-EE4C2C?logo=pytorch&logoColor=white)
![scikit-learn](https://img.shields.io/badge/scikit--learn-Metrics%20%26%20Preprocessing-F7931E?logo=scikitlearn&logoColor=white)
![Jupyter](https://img.shields.io/badge/Jupyter-Notebook-F37626?logo=jupyter&logoColor=white)

A binary text-classification project that compares two neural-network approaches for detecting negative customer reviews:

1. an MLP trained on engineered numerical and categorical features;
2. a trainable token-embedding model with mean pooling and an MLP classifier.

The project was completed as part of the Data Science course at Yandex Practicum.

## Project Goal

The business task is to automatically identify negative reviews so that a quality-control team can prioritize them for further processing.

The target is binary:

- `1` — negative review, corresponding to a user rating of 1–2;
- `0` — all other reviews, corresponding to ratings of 3–5.

The main evaluation metric is **PR-AUC**, which is especially important here because the positive class is rare. ROC-AUC, precision, recall, F1 and accuracy are used as additional diagnostics.

## Data

The project uses the educational dataset `ds_s26_geo_reviews.csv`, provided by Yandex Practicum.

The dataset contains **11,796 reviews** and 10 columns, including review text, organization metadata, geographical information, text-length statistics and the target label.

Class distribution:

| Class | Reviews | Share |
|---|---:|---:|
| Non-negative (`target = 0`) | 10,723 | 90.90% |
| Negative (`target = 1`) | 1,073 | 9.10% |

Because of this imbalance, a trivial classifier that always predicts class 0 would already reach about **0.909 accuracy**, so accuracy alone would be misleading.

The labels are derived from user ratings rather than manual sentiment annotation. As a result, some reviews may contain criticism or mixed sentiment while still belonging to class 0.

## Train / Validation / Test Split

The same stratified split is used for both experiments:

| Split | Rows | Negative share |
|---|---:|---:|
| Train | 8,257 | 9.10% |
| Validation | 1,769 | 9.10% |
| Test | 1,770 | 9.10% |

Random seed: `42`.

## Experiment 1 — MLP on Engineered Features

The baseline neural network does not read the review text directly. Instead, it uses structured and manually engineered features.

### Features

Numerical features include:

- number of rubrics;
- review length in characters and words;
- emoji count;
- punctuation count;
- question and exclamation marks;
- newline count;
- digit count;
- uppercase-letter share;
- average word length.

The `federal_district` feature is one-hot encoded.

The final feature matrix contains **18 features**.

Continuous features are standardized with `StandardScaler`, while the categorical feature is encoded with `OneHotEncoder`.

### Training

Three MLP configurations are compared:

- `small_no_reg` — one hidden layer with 16 units;
- `deep_no_reg` — hidden layers `64 → 32`;
- `regularized` — hidden layers `64 → 32`, dropout `0.3`, and weight decay `1e-4`.

Class imbalance is handled with `BCEWithLogitsLoss(pos_weight=...)`, where the positive-class weight is about **9.99**.

The best configuration is selected by validation PR-AUC.

### Best Numerical Model

The best validation result was produced by the regularized MLP.

Test metrics:

| Metric | Value |
|---|---:|
| PR-AUC | 0.4089 |
| ROC-AUC | 0.8144 |
| Precision | 0.2534 |
| Recall | 0.7019 |
| F1 | 0.3723 |
| Accuracy | 0.7847 |

The experiment shows that surface-level properties such as review length and punctuation contain some signal, but they cannot represent the meaning of words.

## Experiment 2 — Embedding + Mean Pooling + MLP

The second model works directly with review text.

### Text Processing

The preprocessing pipeline:

1. lowercases the text;
2. tokenizes Russian and Latin words and numbers with a regular expression;
3. builds the vocabulary **only on the training split**;
4. uses `<PAD>` and `<UNK>` special tokens;
5. keeps tokens with minimum frequency 2;
6. limits vocabulary size to 20,000 tokens;
7. truncates or pads reviews to **120 tokens**.

The resulting training vocabulary contains **18,582 tokens**.

### Architecture

```text
Token IDs
   ↓
Embedding(vocab_size, 64)
   ↓
PAD mask
   ↓
Mean pooling
   ↓
Linear(64 → 64)
   ↓
ReLU
   ↓
Dropout(0.3)
   ↓
Linear(64 → 1)
```

Training configuration:

- embedding dimension: `64`;
- hidden dimension: `64`;
- dropout: `0.3`;
- optimizer: `AdamW`;
- learning rate: `1e-3`;
- weight decay: `1e-4`;
- epochs: `8`.

The same weighted `BCEWithLogitsLoss` and the same train/validation/test split are used as in the numerical experiment.

### Test Results

| Metric | Value |
|---|---:|
| PR-AUC | **0.6280** |
| ROC-AUC | **0.9431** |
| Precision | **0.5095** |
| Recall | **0.8323** |
| F1 | **0.6321** |
| Accuracy | **0.9119** |

## Model Comparison

| Approach | PR-AUC | ROC-AUC | Precision | Recall | F1 |
|---|---:|---:|---:|---:|---:|
| MLP on engineered features | 0.4089 | 0.8144 | 0.2534 | 0.7019 | 0.3723 |
| Embedding + mean pooling + MLP | **0.6280** | **0.9431** | **0.5095** | **0.8323** | **0.6321** |

The text model clearly outperforms the numerical baseline. The largest practical gains come from giving the network direct access to the vocabulary of the reviews rather than only to surface-level text statistics.

## Error Analysis

At the default threshold of `0.5`, the text model produced:

- **129 false positives**;
- **27 false negatives**.

Typical false positives include reviews whose wording is clearly critical even though their rating-based label belongs to class 0. This highlights a limitation of using user ratings as a proxy for sentiment.

Typical false negatives are short complaints or reviews where the negative meaning depends on context rather than on strongly negative individual words.

## Limitations

The text model is intentionally simple and has several important limitations:

- mean pooling loses word order;
- negation scope is not explicitly modeled;
- contrastive constructions and sarcasm are difficult to capture;
- all tokens contribute through simple averaging;
- long reviews are truncated to 120 tokens;
- the target is inferred from ratings rather than manually annotated sentiment;
- class weighting helps with imbalance but does not create additional minority-class examples.

## Possible Next Steps

Useful follow-up experiments include:

- combining the pooled text embedding with the engineered numerical features;
- replacing mean pooling with a model that preserves token order;
- evaluating recurrent or attention-based architectures;
- testing pretrained language-model embeddings;
- validating future model changes on a new untouched holdout set rather than reusing the current test set.

## Project Structure

```text
Deep Learning/
├── README.md
└── Pytorch. Text tonality.ipynb
```

## Running the Notebook

The notebook requires the source dataset, which is not stored in this repository.

Core dependencies:

```text
numpy
pandas
matplotlib
torch
scikit-learn
jupyter
```

Open and run:

[**Pytorch. Text tonality.ipynb**](Pytorch.%20Text%20tonality.ipynb)

The notebook automatically selects CUDA when available and otherwise runs on CPU.

## Key Takeaway

The main result of the project is that even a relatively simple learned text representation provides substantially more useful information for sentiment classification than engineered text statistics alone. Moving from numerical features to trainable embeddings improved test **PR-AUC from 0.4089 to 0.6280** and **F1 from 0.3723 to 0.6321**.

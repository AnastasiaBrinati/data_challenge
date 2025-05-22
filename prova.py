# 📦 Librerie fondamentali
import numpy as np
import pandas as pd

# 📊 Visualizzazione dati
import matplotlib.pyplot as plt
import seaborn as sns

# Impostazioni grafiche
sns.set(style="whitegrid")
plt.style.use("ggplot")

# 🔍 Preprocessing e trasformazioni
from sklearn.preprocessing import (
    StandardScaler, MinMaxScaler,
    LabelEncoder, OneHotEncoder
)
from sklearn.impute import SimpleImputer

# 🧠 Modellazione
from sklearn.linear_model import LogisticRegression, LinearRegression
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.svm import SVC
from sklearn.tree import DecisionTreeClassifier

# 📈 Valutazione modelli
from sklearn.model_selection import train_test_split, cross_val_score, GridSearchCV
from sklearn.metrics import (
    accuracy_score, precision_score, recall_score, f1_score,
    confusion_matrix, classification_report,
    mean_squared_error, mean_absolute_error, r2_score,
    roc_auc_score, roc_curve
)

# 🛠️ Varie utilità
import os
import time

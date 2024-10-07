from sklearn.decomposition import TruncatedSVD
from scipy.sparse import csr_matrix
import numpy as np
import pandas as pd

df = pd.read_csv("C:/Users/wange/NBA-PREDICTION-MODEL/NBA-Prediction-Model/data/nba_df_final.csv")

df = df.drop(['Unnamed: 0', "Home", "Away", "Date", "Season"], axis = 1)

svd = TruncatedSVD(n_components=15, n_iter=7, random_state=42)
svd.fit(df)

print(svd.explained_variance_ratio_)

print(svd.explained_variance_ratio_.sum())

print(svd.singular_values_)
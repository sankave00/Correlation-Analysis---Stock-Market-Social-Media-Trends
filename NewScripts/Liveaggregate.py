import csv
import pandas as pd
import numpy as np
import torch
from scipy.special import softmax
from time import sleep
import json
import os
from collections import namedtuple
from tqdm.notebook import tqdm
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import os

def initModel():
    global tokenizer
    global model
    model_type = f"cardiffnlp/twitter-roberta-base-sentiment-latest"
    tokenizer = AutoTokenizer.from_pretrained(model_type)
    model = AutoModelForSequenceClassification.from_pretrained(model_type)
def readDf(tfile,rfile):
    
    dft=pd.read_csv(os.path.join(os.path.dirname(__file__),f"../New Datasets/{tfile}"),on_bad_lines='skip',delimiter='\t')
    dfr=pd.read_csv(os.path.join(os.path.dirname(__file__),f"../New Datasets/{rfile}"),delimiter='\t')
    df=pd.concat([dft,dfr],axis=0)
    df=df.reset_index(drop=True)
    return df
def all_sentiment_scores(tweet):
    """To return the sentiment score of a Tweet as analysed by BERTweet. """
    tokens = tokenizer.encode(tweet, return_tensors='pt')
    result = model(tokens)
    a=softmax(result.logits.detach().numpy())
    return a
def formatFunc(tweet):
    return np.argmax(tweet)-1
def computeSentiment(df):
    df['Score']=df['TITLE'].apply(all_sentiment_scores)
    df['MaxScore']=df['Score'].apply(formatFunc)
    return df
def stringformat(df):
    s=[]
    df['neg_score']=''
    df['neu_score']=''
    df['pos_score']=''
    df['wt_neg']=''
    df['wt_neu']=''
    df['wt_pos']=''
    for i in range(len(df['Score'])):
        # s=str_to_list(df['Score'][i])
        # df['neg_score'][i]=s[0]
        # df['neu_score'][i]=s[1]
        # df['pos_score'][i]=s[2]
        # df['wt_neg'][i]=s[0]*df['count'][i]
        # df['wt_neu'][i]=s[1]*df['count'][i]
        # df['wt_pos'][i]=s[2]*df['count'][i]
        df['neg_score'][i]=df['Score'][i][0][0]
        df['neu_score'][i]=df['Score'][i][0][1]
        df['pos_score'][i]=df['Score'][i][0][2]
        df['wt_neg'][i]=df['Score'][i][0][0]*df['COUNT'][i]
        df['wt_neu'][i]=df['Score'][i][0][1]*df['COUNT'][i]
        df['wt_pos'][i]=df['Score'][i][0][2]*df['COUNT'][i]
    return df
def aggregation(df):
    agg_df = df.groupby(['CATEGORY', 'DATE']).agg({'neg_score': 'sum','neu_score': 'sum','pos_score': 'sum','wt_neg':'sum','wt_neu':'sum','wt_pos':'sum'})
    agg_df1 = df.groupby(['CATEGORY', 'DATE', 'MaxScore'])['MaxScore'].count().reset_index(name='COUNT')
    pivoted_df = agg_df1.pivot_table(index=['CATEGORY', 'DATE'], columns='MaxScore', values='COUNT', fill_value=0)
    pivoted_df = pivoted_df.reset_index()
    pivoted_df.columns=['CATEGORY','DATE','neg_count','neu_count','pos_count']
    resultat=pd.merge(pivoted_df,agg_df,how='inner',on=['CATEGORY','DATE'])
    col=['neg_count','neu_count','pos_count']
    resultat['COUNT']=resultat[col].sum(axis=1)
    return resultat
def readSdf():
    #dfs=pd.read_csv(f"D:\\Downloads\\live-stock.csv",on_bad_lines='skip',names=['CATEGORY','TICKER','DATE','OPEN','CLOSE','HIGH','LOW'],delimiter='\t')
    dfs=pd.read_csv(os.path.join(os.path.dirname(__file__),f"../New Datasets/live-stock.csv"),on_bad_lines='skip',names=['CATEGORY','TICKER','DATE','OPEN','CLOSE','HIGH','LOW'],delimiter='\t')
    dfs=dfs.reset_index(drop=True)
    return dfs
def mergerops(df,sdf):
    mdf = pd.merge(df, sdf, on=['CATEGORY', 'DATE'])
    mdf = mdf[['DATE','OPEN','wt_neg','wt_neu','wt_pos','CLOSE','HIGH','LOW']]
    mdf = mdf.rename(columns={'OPEN': 'prev_open', 'wt_neg': 'prev_wt_neg','wt_neu': 'prev_wt_neu','wt_pos': 'prev_wt_pos'})
    return mdf
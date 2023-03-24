# -*- coding: utf-8 -*- 

import random
import numpy as np
import pandas as pd
from tqdm import tqdm
from collections import defaultdict

import seaborn as sns
import matplotlib.pyplot as plt

from linkage_saude.exceptions import *

'''
    -------------------------------------------------
    ---------- SUMMARIES AND VISUALIZATION ----------
    ------------------------------------------------- 
'''

def score_summary(score_arr, bins, range_certain, range_potential, scale="linear"):
    '''
        Plot a distribution of the scores resulted from the data matching process.

        Args:
        -----
            score_arr:
                np.array or pd.Series. List of scores for each pair compared during
                the matching process. When a pandas Series is parsed, the index is 
                expected to be a MultiIndex containing the IDs of the records compared.
            bins:
                list. Custom histogram bins.
            range_certain:
                list. List of size two containing the lower and upper bound of the score
                range of the records considered to be matched for certain.
            range_potential:
                list. List of size two containing the lower and upper bound of the score
                range of the records considered to be potential matching.
            scale:
                String. {'linear', 'log', ...}. Scale of the y-axis.

        Return:
        -------
            info:
                Dictionary. Main information on the pairs obtained and their scores.
    '''
    fig, ax = plt.subplots(1, figsize=(7,4.8))
    s = sns.histplot(score_arr, bins=bins, color="tab:red", ax=ax, alpha=0.65)

    ax.spines['top'].set_color('none')
    ax.spines['right'].set_color('none')
    ax.spines['left'].set_position(('outward', 7))
    ax.spines['bottom'].set_position(('outward', 7))

    ax.spines["left"].set_linewidth(1.5)
    ax.spines["bottom"].set_linewidth(1.5)
    ax.spines["top"].set_linewidth(1.5)
    ax.spines["right"].set_linewidth(1.5)

    ax.tick_params(width=1.5, labelsize=11)
    ax.set_ylabel("Frequência", weight="bold", fontsize=14, labelpad=8)
    ax.set_xlabel("Score", weight="bold", fontsize=13)
    ax.grid(alpha=0.2)

    freq, bins = np.histogram(score_arr, bins=bins)
    ax.fill_between(range_potential, y1=max(freq)+10, color="tab:orange", alpha=0.2)
    ax.fill_between(range_certain, y1=max(freq)+10, color="tab:blue", alpha=0.2)

    ncertain = score_arr[(score_arr>=range_certain[0]) & (score_arr<range_certain[1])].shape[0]
    npotential = score_arr[(score_arr>=range_potential[0]) & (score_arr<range_potential[1])].shape[0]
    ndiff = score_arr[(score_arr<range_potential[0])].shape[0]

    ax.set_xticks(bins)
    ax.set_xticklabels([f"{n:.1f}" for n in bins], rotation=45)
    ax.set_yscale(scale)
    info = {"FIG": fig, "AXIS": ax, "FREQUENCY AND BINS": (freq, bins), 
            "# IGUAIS": ncertain, "# POTENCIAIS": npotential, "# DIFERENTES": ndiff }
    return info

def show_pair(pairs, left_df, right_df=None, 
              left_cols=None, right_cols=None, random_state=None):
    '''
        Show a random pair of records from the list 'pairs' obtained by the matching.

        Args:
        -----
            pairs:
                List.
            left_df:
                pandas.DataFrame.
            right_df:
                pandas.DataFrame. Default None.
            left_cols:
                List. Default None.
            right_cols:
                List. Default None.
            random_state:
                Integer. Default None.
        Return:
        -------
            display_df:
                pandas.DataFrame.
    '''
    if random_state is not None:
        random.seed(random_state)

    pair = random.choice(pairs)
    left_index, right_index = pair[0], pair[1]

    temp_left = left_df
    temp_right = right_df

    # --> Verify for columns
    if left_df is not None and left_cols is None:
        raise ValueError("Subset of columns' names must be parsed.")
    if right_df is not None and right_cols is None:
        raise ValueError("Subset of columns' names must be parsed.")

    if temp_right is None:
        display_df = pd.concat( [temp_left[left_cols].loc[left_index], temp_left[left_cols].loc[right_index]], axis=1 )
    else:
        display_df = pd.concat( [temp_left[left_cols].loc[left_index], temp_right[right_cols].loc[right_index]], axis=1 )
    return display_df 


'''
    -------------------------------------------------
    ----------------- OPERATIONAL -------------------
    ------------------------------------------------- 
'''

def find_root(index, ptr):
    dummy = index
    while ptr[dummy]>=0:
        dummy = ptr[dummy]
    return dummy

# --> Deduplication
def deduple_grouping(pairs):
    '''
        Perform grouping of matched records into a final schema file, identifying unique individuals.

        After deduplication, we use 'pairs'(a list of tuples corresponding to each pair of
        matched records), to create a hash/dictionary structure associating a given record to all its matched
        records (same person). Dictionary contains a list of matched records.

        Args:
        -----
            pairs:
                pandas.DataFrame. A dataframe containing at least two columns representing
                the matched pairs of unique records: "left" and "right".  
        Return:
        -------
            matched_records:
                collections.defaultdict. 
    '''
    left_nots = pairs["left"].tolist()
    right_nots = pairs["right"].tolist()

    # --> Define data structure of trees to aggregate several matched files through transitive relations. 
    # ----> Unique records in 'pairs'
    unique_nots = np.unique(left_nots+right_nots) 
    # ----> Tree positions of each unique record of 'pairs' (based on the union/find algorithms)
    ptr = np.zeros(unique_nots.shape[0], int) - 1
    # ----> Associate each record to its position in 'ptr' (hash)
    ptr_index = dict( zip(unique_nots, np.arange(0, unique_nots.shape[0], 1)) )

    # --> Aggregate matched records associating each unique person to a root index. 
    for index in tqdm(range(len(left_nots))):
        left = left_nots[index]
        right = right_nots[index]
        left_index = ptr_index[left]
        right_index = ptr_index[right]
    
        left_root = find_root(left_index, ptr)
        right_root = find_root(right_index, ptr)
    
        if left_root==right_root:
            continue
    
        bigger_root, bigger_index = left_root, left_index
        smaller_root, smaller_index = right_root, right_index
        if ptr[right_root]<ptr[left_root]:
            bigger_root, bigger_index = right_root, right_index
            smaller_root, smaller_index = left_root, left_index
    
        ptr[bigger_root] += ptr[smaller_root]
        ptr[smaller_root] = bigger_root
        
    matched_records = defaultdict(lambda: [])
    for index in range(len(ptr)):
        local_not = unique_nots[index]
        root_not = unique_nots[find_root(index, ptr)]
        if root_not!=local_not:
            matched_records[root_not].append(local_not)

    return matched_records

# --> Linkage
def linkage_grouping(pairs):
    '''
        Perform grouping of matched records into a final schema file, identifying unique individuals.

        After the linkage between two databases, we use 'pairs' (containing the ID of the matched 
        records) to create a hash/dictionary structure associating a given record in one database 
        to all its matched records in the other. Dictionary contains a list of matched records.

        Args:
        -----
            pairs:
                pandas.DataFrame. A dataframe containing at least two columns representing
                the matched pairs of unique records: "left" and "right".  
        Return:
        -------
            result:
                collections.defaultdict. 
    '''
    pairs_t = list(pairs.groupby("left")["right"].value_counts().index)
    result = {}
    for k, v in pairs_t:
        result.setdefault(k, []).append(v)
    return result 
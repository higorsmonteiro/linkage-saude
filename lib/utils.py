import uuid
import numpy as np
from tqdm import tqdm
from collections import defaultdict

def replace_string(string, sep=''):
    '''
        Return the input string without any special character and numbers.
    '''
    if sep=='':
        new_string = sep.join([char.upper() for char in string if char.isalnum()])
        new_string = sep.join([char.upper() for char in new_string if not char.isdigit()])
    elif sep==' ':
        new_string = []
        string_lst = string.split(sep)
        for s in string_lst:
            new_string.append(''.join([char.upper() for char in s if char.isalnum()]))
        new_string = sep.join(new_string)
    return new_string

def find_root(index, ptr):
    dummy = index
    while ptr[dummy]>=0:
        dummy = ptr[dummy]
    return dummy

# DEDUPLICATION
def grouping(pairs_df):
        '''
            Perform grouping of matched records into a final schema file, identifying unique individuals.

            After linkage is performed, we use 'pairs', a list of tuples corresponding to each pair of
            matched records, to define a new schema containing essential information from SIVEP for identified
            unique individuals having duplicated notifications. 'sivep_index_df' is the SIVEP database with
            index consistent with 'pairs'.

            Args:
                pairs:
                    List of two-dimensional tuples. Each tuple represents a matched pair of records with
                    their corresponding indexes on the original database.
                sivep_index_df:
                    pandas.DataFrame. Sivep dataframe with indexes defined according to 'pairs'.

            Return:
                None.
        '''

        # --> Get list of notification numbers (NN) from original database and associate each 
        # --> index in 'pairs' with their NNs.
        left_nots = pairs_df["left"].tolist()
        right_nots = pairs_df["right"].tolist()
        
        #notific_df = sinan_index_df["ID_GEO"]
        #left_nots = [ notific_df.loc[pair[0]] for pair in pairs ]
        #right_nots = [ notific_df.loc[pair[1]] for pair in pairs ]

        # --> Define data structure of trees to aggregate several matched files through transitive relations. 
        unique_nots = np.unique(left_nots+right_nots) # (Unique notifications in 'pairs')
        ptr = np.zeros(unique_nots.shape[0], int) - 1 # (Tree positions of each unique notification of 'pairs')

        # --> Relate each notification with its position in 'ptr' (hash)
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


# DEDUPLICATION
def linkage_grouping(pairs_df):
        '''
            Perform grouping of matched records into a final schema file, identifying unique individuals.

            After linkage is performed, we use 'pairs', a list of tuples corresponding to each pair of
            matched records, to define a new schema containing essential information from SIVEP for identified
            unique individuals having duplicated notifications. 'sivep_index_df' is the SIVEP database with
            index consistent with 'pairs'.

            Args:
                pairs:
                    List of two-dimensional tuples. Each tuple represents a matched pair of records with
                    their corresponding indexes on the original database.
                sivep_index_df:
                    pandas.DataFrame. Sivep dataframe with indexes defined according to 'pairs'.

            Return:
                None.
        '''

        pairs_t = list(pairs_df.groupby("left")["right"].value_counts().index)
        result = {}
        for k, v in pairs_t:
            result.setdefault(k, []).append(v)
        return result 
'''
    Perform deduplication through a probabilistic(approximate) approach within a given database
    based on processed columns.

    Author: Higor S. Monteiro
    Date: 2022-06-08
'''
import os
import csv
import numpy as np
import pandas as pd
from tqdm import tqdm
from tabulate import tabulate
import recordlinkage
from recordlinkage.index import SortedNeighbourhood
from src.CustomExceptions import *


class Deduple:
    def __init__(self, main_df, field_id=None, env_folder=None) -> None:
        '''
            Description.

            Args:
            -----
                main_df:
                    pandas.DataFrame. 
                field_id:
                    String. Column name to be used as unique identifier.
                env_folder:
                    String. Path where all results of the deduplication will be
                    stored. 
        '''
        self.main_df = main_df.copy()
        self.field_id = field_id
        self.env_folder = env_folder
        if self.field_id is None:
            raise UniqueIdentifierMissing("Must provide an existing column as a unique identifier")
        if self.env_folder is None:
            raise OutputPathMissing("")

        if not os.path.isdir(self.env_folder):
            os.mkdir(self.env_folder)

        self.linkage_vars = None
        self.compare_cl = None
        self.features = None

        self.pairs_left = []
        self.pairs_right = []
        self.pot_pairs_left = []
        self.pot_pairs_right = []
        self.evaluation_pot_pairs = []

    def set_linkage(self, linkage_vars, map_compare, map_threshold,
                        string_method="jarowinkler"):
        '''
            Description.

            Args:
            -----
                linkage_vars:
                    List of strings. 
                map_compare:
                    Dictionary. Keys are names in 'linkage_vars' while values are either
                    'exact' or 'string' to signal which type of comparison to use.
                map_threshold:
                    Dictionary. Keys are names in  'linkage_vars' while values are float
                    values ranging from 0.0 to 1.0 to signal the threshold to be used in
                    string comparison between fields. For exact comparison, values are None.
            Return:
                None.
        '''
        self.linkage_vars = linkage_vars

        # -- Determine the type of comparison for each pair of fields
        self.compare_cl = recordlinkage.Compare()
        for item in map_compare.items():
            # --> Define the type of comparison between fields
            key, value = item
            if value=="exact":
                self.compare_cl.exact(key, key, label=key)
            elif value=="string":
                self.compare_cl.string(key, key, label=key, method=string_method, threshold=map_threshold[key])
            else:
                pass

    # TO DO: DEDUPLE OVER CHUNKSIZES (FOR VERY LARGE DATASETS)
    def perform_linkage(self, blocking_var, window=1, output_fname="feature_pairs"):
        '''
            After the linkage settings are defined, set blocking and perform the linkage.

            Args:
                blocking_var:
                    String. Field name regarding the blocking variable for the linkage.
                window:
                    Odd Integer. Window parameter for the sorted neighborhood blocking algorithm. 
                    window equal one means exact blocking.
                chunksize:
                    Integer. Size of each partition of the right database for partitioned linkage 
                    (used for large databases).
        '''
        # --> Define blocking mechanism
        indexer = recordlinkage.Index()
        indexer.add(SortedNeighbourhood(blocking_var, blocking_var, window=window))

        # --> Create and compare pairs
        candidate_links = indexer.index(self.main_df)
        print(f"Number of pairs: {len(candidate_links)}")
        self.features = self.compare_cl.compute(candidate_links, self.main_df)

        if self.env_folder is not None:
            self.features.to_parquet(os.path.join(self.env_folder, f"{output_fname}.parquet"))

class DedupleOld:
    def __init__(self, main_df, env_folder=None) -> None:
        '''
            Perform linkage within a dataframe based on linkage variables, possibly
            storing partial potential pairs in external files (for large files).

            If one of the databases is large, it should be the right dataframe parsed in
            order to perform linkage by chunks.

            Args:
                df1:
                    pandas.DataFrame.
                df2:
                    pandas.DataFrame.
                env_folder:
                    String.
        '''
        self.main_df = main_df.copy()
        self.main_df["index_used_linkage"] = self.main_df.index

        self.env_folder = env_folder
        if not os.path.isdir(self.env_folder):
            os.mkdir(self.env_folder)

        self.linkage_vars = None
        self.compare_cl = None
        self.features = None

        self.pairs_left = []
        self.pairs_right = []
        self.pot_pairs_left = []
        self.pot_pairs_right = []
        self.evaluation_pot_pairs = []

    def set_linkage(self, linkage_vars, map_compare, map_threshold):
        '''
            Description.

            Args:
                linkage_vars:
                    List of strings. 
                map_compare:
                    Dictionary. Keys are names in 'linkage_vars' while values are either
                    'exact' or 'string' to signal which type of comparison to use.
                map_threshold:
                    Dictionary. Keys are names in  'linkage_vars' while values are float
                    values ranging from 0.0 to 1.0 to signal the threshold to be used in
                    string comparison between fields. For exact comparison, values are None.
            Return:
                None.
        '''
        self.linkage_vars = linkage_vars

        self.compare_cl = recordlinkage.Compare()
        for item in map_compare.items():
            # --> Define the type of comparison between fields
            key, value = item
            if value=="exact":
                self.compare_cl.exact(key, key, label=key)
            elif value=="string":
                self.compare_cl.string(key, key, label=key, method="jarowinkler", threshold=map_threshold[key])
            else:
                pass

    def perform_linkage(self, blocking_var, window=1, chunksize=None, cpf_field=False, cns_field=False, output_fname="features_pairs"):
        '''
            After the linkage settings are defined, set blocking and perform the linkage.

            Args:
                blocking_var:
                    String. Field name regarding the blocking variable for the linkage.
                window:
                    Odd Integer. Window parameter for the sorted neighborhood blocking algorithm. 
                    window equal one means exact blocking.
                chunksize:
                    Integer. Size of each partition of the right database for partitioned linkage 
                    (used for large databases).
        '''
        # --> Define blocking mechanism
        indexer = recordlinkage.Index()
        indexer.add(SortedNeighbourhood(blocking_var, blocking_var, window=window))

        # --> Create and compare pairs
        candidate_links = indexer.index(self.main_df)
        print(f"Number of pairs: {len(candidate_links)}")
        self.features = self.compare_cl.compute(candidate_links, self.main_df)

        if self.env_folder is not None:
            self.features.to_parquet(os.path.join(self.env_folder, f"{output_fname}.parquet"))
        

    def define_linked_pairs(self, correct_pairs, potential_pairs):
        '''
            Description.

            Args:
                correct_pairs:
                    List of 2-dimensional tuples. Pairs defined as pairs with certainty.
                potential_pairs:
                    List of 2-dimensional tuples. Pairs defined as potential pairs -> must
                    be evaluated manually.
            Return:
                None  
        '''
        self.pairs_left = [ pair[0] for pair in correct_pairs ]
        self.pairs_right = [ pair[1] for pair in correct_pairs ]
        self.pot_pairs_left = [ pair[0] for pair in potential_pairs ]
        self.pot_pairs_right = [ pair[1] for pair  in potential_pairs ]
        self.evaluation_pot_pairs = [ "NO EVALUATION" for pair in potential_pairs ]

    def evaluate_potential_pairs(self, labelfile, clear_std_function=None):
        '''
        
        '''
        label_folder = os.path.join(self.env_folder, "manual_labels")
        if not os.path.isdir(label_folder):
            os.mkdir(label_folder)

        # -- Open and fill the pair as we select the pairs. 
        with open(os.path.join(label_folder, labelfile), 'a') as f:
            csv_writer = csv.writer(f, delimiter=';', quoting=csv.QUOTE_MINIMAL)

            for n in range(len(self.pot_pairs_left)):
                rec_left = self.pot_pairs_left[n]
                rec_right = self.pot_pairs_right[n]
                if self.evaluation_pot_pairs[n]!="NO EVALUATION":
                    continue

                comparison_df = pd.concat([self.main_df[self.linkage_vars].loc[rec_left], self.main_df[self.linkage_vars].loc[rec_right]], axis=1)
                print(f"pair {n+1} out of {len(self.pot_pairs_left)}")
                print(tabulate(comparison_df, headers='keys', tablefmt='psql'))
                dummy = None

                answer = input("Matching? (If yes, answer 'S'): ")
                csv_writer.writerow([rec_left, rec_right, answer])
                self.evaluation_pot_pairs[n] = answer
                # --> clear output
                clear_std_function()
    
    def save_matchings(self, suffix=''):
        '''
        
        '''
        pairs = [ (self.pairs_left[n], self.pairs_right[n]) for n in range(len(self.pairs_left)) ]
        pairs_pot = [ (self.pot_pairs_left[n], self.pot_pairs_right[n]) for n in range(len(self.pot_pairs_left)) if self.evaluation_pot_pairs[n].lower()=="s" ]

        all_pairs = pairs+pairs_pot
        left_pairs = [pair[0] for pair in all_pairs]
        right_pairs = [pair[1] for pair in all_pairs]
        df = pd.DataFrame({"LEFT INDEX": left_pairs, "RIGHT INDEX": right_pairs})
        df.to_excel(os.path.join(self.env_folder, f"PAIRS_{suffix}.xlsx"))
        df.to_parquet(os.path.join(self.env_folder, f"PAIRS_{suffix}.parquet"))
        
            

       


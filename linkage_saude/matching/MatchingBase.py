# -*- coding: utf-8 -*- 

import os
import json
import numpy as np
import pandas as pd
import recordlinkage

from linkage_saude.exceptions import *
import linkage_saude.utils.matching as matching_utils

class MatchingBase:
    def __init__(self, left_df, right_df=None, left_id=None, right_id=None, env_folder=None) -> None:
        '''
        
        '''
        self._features = None
        self.linkage_vars = None
        self.compare_cl, self._features, self.sum_rules = None, None, None

        # --> Solve for the left dataframe
        self.left_df = left_df.copy()
        self.left_id = left_id
        if self.left_id is None:
            raise UniqueIdentifierMissing("Must provide an existing column as a unique identifier.")
        self.left_df = self.left_df.set_index(self.left_id)
        
        # --> Solve for the right dataframe, if the case
        self.right_df = None
        if right_df is not None:
            self.right_df = right_df.copy()
            self.right_id = right_id
            if self.right_id is None:
                raise UniqueIdentifierMissing("Must provide an existing column as a unique identifier.")
            self.right_df = self.right_df.set_index(self.right_id)
            
        # --> Select working folder
        self.env_folder = env_folder
        if self.env_folder is None:
            raise OutputPathMissing("Must provide a working folder.")
        
        if not os.path.isdir(self.env_folder):
            os.mkdir(self.env_folder)

    @property
    def features(self):
        if self._features is not None:
            return self._features

    @features.setter
    def features(self):
        raise AttributeError("Not possible to change this attribute from outside.")
    
    '''
        -------------------------------------------
        ------------ MATCHING SETTINGS ------------
        -------------------------------------------
    '''
    def set_linkage(self, compare_rules, sum_rules, string_method="jarowinkler", numeric_method="linear"):
        '''
            Description.

            Args:
            -----
                compare_rules:
                    Dictionary. Keys of the dictionary represent the linkage variables to be used
                    in the comparison. Each key holds a list of values containing at least a single
                    element. The first element should be always the type of comparison to be 
                    performed for the given field: {'exact', 'string'}. For 'string' comparison,
                    the second element of the list should be the threshold (0.0 to 1.0) for the
                    comparison method. For 'exact' comparison, no further values are needed.
                **kwargs:
                    Aside from the 'threshold' argument, arguments are the same as the comparison
                    methods from recordlinkage.Compare class. 
            Return:
                None.
        '''
        self.sum_rules = sum_rules
        self.linkage_vars = list(compare_rules.keys())

        # -- Settings for comparison between fields
        self.compare_cl = recordlinkage.Compare()
        for map_item in compare_rules.items():
            key, values = map_item
            if values[0]=="exact":
                self.compare_cl.exact(key, key, label=key)
            elif values[0]=="string":
                self.compare_cl.string(key, key, label=key, threshold=values[1], method=string_method)
                #self.compare_cl.string(key, key, label=key, method=string_method)
            elif values[0]=="date": 
                self.compare_cl.date(key, key, label=key)
            elif values[0]=="numeric": # can be used for timestamps
                self.compare_cl.numeric(key, key, label=key, method=numeric_method)
            elif values[0]=="geo":
                pass
            else:
                pass

    def perform_linkage(self):
        '''
        
        '''
        pass

    '''
        ------------------------------------------
        ------------ INPUT AND OUTPUT ------------
        ------------------------------------------
    '''
    def save_pairs(self, positive_pairs, potential_pairs, negative_pairs,  
                   left_cols=None, right_cols=None, 
                   division=50, overwrite=False, negative_max=None):
        '''
            Save pairs (negative, positive and potential pairs) considering a 
            format for further annotation.

            Args:
            -----
                positive_pairs:
                    List.
                potential_pairs:
                    List.
                negative_pairs:
                    List.
                left_cols:
                    List. Default None.
                right_cols:
                    List. Default None.
                division:
                    Integer.
                overwrite:
                    Boolean.
                negative_max:
                    Integer. Default None.
            Return:
            -------
                None.
        '''
        annotation_folder = os.path.join(self.env_folder, "annotation_files")
        if os.path.isfile(os.path.join(annotation_folder, "POSITIVE_PAIRS.json")) and not overwrite:
            raise AnnotationError("Overwrite of annotation files not allowed.")
        
        temp_left = self.left_df
        temp_right = self.right_df




    def create_annotation(self, certain_pairs, potential_pairs, division=50, cols=None, overwrite=False, certain_duplicate_default=None):
        '''
            Description.

            Args:
            -----
                certain_pairs:
                    List.
                potential_pairs:
                    List.
                division:
                    Integer.
                cols:
                    List.        
        '''
        annotation_folder = os.path.join(self.env_folder, "ANNOTATION")
        if os.path.isfile(os.path.join(annotation_folder, "MATCHED_PAIRS.json")) and not overwrite:
            raise AnnotationError("Overwrite of annotation files not allowed.")

        temp_df = self.left_df
        # --> POTENTIAL PAIRS (SEPARATE IN DIFFERENT FILES)
        #splitted_pot = np.split(potential_pairs, np.arange(division, potential_pairs.shape[0]+1, division))
        splitted_pot = [ potential_pairs[i:i+division] for i in range(0, len(potential_pairs)+1, division) ]
        pot_list = []
        for n in range(len(splitted_pot)):
            current_pot_list = []
            for row in splitted_pot[n]:
                pair = row
                if cols is not None:
                    left_pair = json.loads(temp_df[cols].loc[pair[0]].to_json())
                    right_pair = json.loads(temp_df[cols].loc[pair[1]].to_json())
                else:
                    left_pair = json.loads(temp_df.loc[pair[0]].to_json())
                    right_pair = json.loads(temp_df.loc[pair[1]].to_json())
                pair_element = {"a": left_pair, "b": right_pair, 
                                "identifiers": {"a": pair[0], "b": pair[1]}, 
                                "certain": "no", 
                                "same person": None,
                                "duplicate": None,
                                "keep": "a" }
                current_pot_list.append(pair_element)
            pot_list.append(current_pot_list)
        # --> CERTAIN PAIRS (ONE SINGLE FILE)
        certain_list = []
        for row in certain_pairs:
            pair = row
            if cols is not None:
                left_pair = json.loads(temp_df[cols].loc[pair[0]].to_json())
                right_pair = json.loads(temp_df[cols].loc[pair[1]].to_json())
            else:
                left_pair = json.loads(temp_df.loc[pair[0]].to_json())
                right_pair = json.loads(temp_df.loc[pair[1]].to_json())
            pair_element = {"a": left_pair, "b": right_pair, 
                            "identifiers": {"a": pair[0], "b": pair[1]}, 
                            "certain": "yes",
                            "same person": "yes",
                            "duplicate": certain_duplicate_default,
                            "keep": "a" }
            certain_list.append(pair_element)

        # -- OUTPUT
        certain_pairs_json = {"pairs": certain_list}
        with open(os.path.join(self.env_folder, "ANNOTATION", "MATCHED_PAIRS.json"), "w") as f:
            json.dump(certain_pairs_json, f)

        for n, cur_list in enumerate(pot_list):
            with open(os.path.join(self.env_folder, "ANNOTATION", f"POTENTIAL_PAIRS_{n}.json"), "w") as f:
                json.dump({"pairs": cur_list}, f)



    '''
        ---------------------------------------------------
        ------------ SUMMARY AND VISUALIZATION ------------
        ---------------------------------------------------
    '''
    def show_pair(self, pairs, left_cols=None, right_cols=None, random_state=None):
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
        display_df = matching_utils.show_pair(pairs=pairs, left_df=self.left_df, right_df=self.right_df, 
                                              left_cols=left_cols, right_cols=right_cols, random_state=random_state)
        return display_df

    def score_summary(self, score_arr, bins, range_certain, range_potential, scale="linear"):
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
        '''
        return matching_utils.score_summary(score_arr, bins, range_certain, range_potential, scale)
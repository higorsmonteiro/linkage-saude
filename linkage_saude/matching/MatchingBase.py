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
                   left_cols=None, right_cols=None, duplicate_text_default=None, 
                   overwrite=False, negative_max=5000):
        '''
            Save pairs (negative, positive and potential pairs) considering a 
            format for further annotation.

            Args:
            -----
                positive_pairs:
                    List. List of 2-tuples containing the pair of IDs representing
                    the pairs classified as positive.
                potential_pairs:
                    List. List of 2-tuples containing the pair of IDs representing
                    the pairs classified as potential pairs (to be classified manually).
                negative_pairs:
                    List. List of 2-tuples containing the pair of IDs representing
                    the pairs classified as negative.
                left_cols:
                    List. Default None. Columns to be used when saving the information of
                    a given record from the left database.
                right_cols:
                    List. Default None. Columns to be used when saving the information of
                    a given record from the right database.
                duplicate_text_default:
                    String. Default None. Default text to be used in the 'duplicate' field
                    of the positive pairs. 
                overwrite:
                    Boolean. If False it will not override the existing files of 
                    classified pairs. 
                negative_max:
                    Integer. Default None. Maximum number of negative pairs to be stored.
            Return:
            -------
                None.
        '''
        annotation_folder = os.path.join(self.env_folder, "annotation_files")
        if os.path.isfile(os.path.join(annotation_folder, "POSITIVE_PAIRS.json")) and not overwrite:
            raise AnnotationError("Overwrite of annotation files not allowed.")
        
        
        positive_json_list = matching_utils.create_json_pairs(self.left_df, self.right_df, left_cols, right_cols, 
                                                              "positive", positive_pairs, duplicate_text_default)
        potential_json_list = matching_utils.create_json_pairs(self.left_df, self.right_df, left_cols, right_cols, 
                                                               "potential", potential_pairs)
        negative_json_list = matching_utils.create_json_pairs(self.left_df, self.right_df, left_cols, right_cols, 
                                                              "negative", negative_pairs, rec_max=negative_max, 
                                                               duplicate_text_default="no")
        
        positive_pairs_json = {"pairs": positive_json_list}
        potential_pairs_json = {"pairs": potential_json_list}
        negative_pairs_json = {"pairs": negative_json_list}
        
        # --> output
        with open(os.path.join(self.env_folder, "annotation_files", "POSITIVE_PAIRS.json"), "w") as f:
            json.dump(positive_pairs_json, f)
        with open(os.path.join(self.env_folder, "annotation_files", "POTENTIAL_PAIRS.json"), "w") as f:
            json.dump(potential_pairs_json, f)
        with open(os.path.join(self.env_folder, "annotation_files", "NEGATIVE_PAIRS.json"), "w") as f:
            json.dump(negative_pairs_json, f)

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
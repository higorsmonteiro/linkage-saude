# -*- coding: utf-8 -*- 

'''
    Perform the linkage through a probabilistic(approximate) approach between two databases
    based on processed columns.

    Author: Higor S. Monteiro
    Email: higormonteiros@gmail.com
'''

import os
import json
import numpy as np
import pandas as pd
import recordlinkage
from recordlinkage.index import SortedNeighbourhood

from linkage_saude.exceptions import *
from linkage_saude.matching.MatchingBase import MatchingBase
import linkage_saude.utils.matching as matching_utils


class Deduple(MatchingBase):

    # TO DO: DEDUPLE OVER CHUNKSIZES (FOR VERY LARGE DATASETS)
    def perform_linkage(self, blocking_var, window=1, output_fname="feature_pairs", threshold=None):
        '''
            After setting the properties of the linkage, blocking is defined and the linkage is performed.

            Args:
            -----
                blocking_var:
                    String. Name of the field regarding the blocking variable for the linkage.
                window:
                    Odd Integer. Window parameter for the sorted neighborhood blocking algorithm. 
                    window equal one means exact blocking.
                chunksize: NOT IMPLEMENTED.
                    Integer. Size of each partition of the right (BOTH!) database for partitioned linkage 
                    (used for large databases).
        '''

        # --> Define blocking mechanism
        indexer = recordlinkage.Index()
        indexer.add(SortedNeighbourhood(blocking_var, blocking_var, window=window))

        # --> Create and compare pairs
        candidate_links = indexer.index(self.left_df)
        print(f"Number of pairs: {len(candidate_links)}")
        self._features = self.compare_cl.compute(candidate_links, self.left_df)

        # --> Save scores for all pairs
        self._features.to_parquet(os.path.join(self.env_folder, f"{output_fname}.parquet"))

        # --> All field scores less than 'threshold' are reduced to zero.
        if threshold is not None and threshold<=1.0:
            self._features[self._features<threshold] = 0.0

        # --> apply sum rules
        for item in self.sum_rules.items():
            key, value = item
            self._features[key] = self._features[value].sum(axis=1)

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

    def load_annotated_pairs(self):
        '''
        
        '''
        annotation_folder = os.path.join(self.env_folder, "ANNOTATION")
        files = os.listdir(annotation_folder)
        potential_pairs_file = [ n for n in files if "MATCHED" not in n ]

        
        with open(os.path.join(annotation_folder, "MATCHED_PAIRS.json"), "r") as f:
            matched = json.load(f)
        
        pot = []
        for pot_fname in potential_pairs_file:
            with open(os.path.join(annotation_folder, pot_fname), "r") as f:
                cur_pairs = json.load(f)
            pot.append(cur_pairs)

        # --> Create dataframe
        keep_ = {"a": "left", "b": "right"}
        pairs = {"left": [], "right": [], "certain": [], "keep": [], "same person": [], "duplicate": []}
        for cur_pair in matched["pairs"]:
            left_id = cur_pair["identifiers"]["a"]
            right_id = cur_pair["identifiers"]["b"]
            certain = cur_pair["certain"]
            to_keep = keep_[cur_pair["keep"]]
            is_person = cur_pair["same person"]
            is_duplicate = cur_pair["duplicate"]

            pairs["left"].append(left_id)
            pairs["right"].append(right_id)
            pairs["certain"].append(certain)
            pairs["keep"].append(to_keep)
            pairs["same person"].append(is_person)
            pairs["duplicate"].append(is_duplicate)

        for block in pot:
            for cur_pair in block["pairs"]:
                left_id = cur_pair["identifiers"]["a"]
                right_id = cur_pair["identifiers"]["b"]
                certain = cur_pair["certain"]
                is_person = cur_pair["same person"]
                is_duplicate = cur_pair["duplicate"]

                pairs["left"].append(left_id)
                pairs["right"].append(right_id)
                pairs["certain"].append(certain)
                pairs["keep"].append(to_keep)
                pairs["same person"].append(is_person)
                pairs["duplicate"].append(is_duplicate)
        
        return pd.DataFrame(pairs)
    


class PLinkage(MatchingBase):

    # TO DO: LINKAGE OVER CHUNKSIZES (FOR VERY LARGE DATASETS)
    def perform_linkage(self, blocking_var, window=1, output_fname="feature_pairs", threshold=None):
        '''
            After setting the properties of the linkage, blocking is defined and the linkage is performed.

            Args:
            -----
                blocking_var:
                    String. Name of the field regarding the blocking variable for the linkage.
                window:
                    Odd Integer. Window parameter for the sorted neighborhood blocking algorithm. 
                    window equal one means exact blocking.
                chunksize: NOT IMPLEMENTED.
                    Integer. Size of each partition of the right (BOTH!) database for partitioned linkage 
                    (used for large databases).
        '''
        # --> Define blocking mechanism
        indexer = recordlinkage.Index()
        indexer.add(SortedNeighbourhood(blocking_var, blocking_var, window=window))

        # --> Create and compare pairs
        candidate_links = indexer.index(self.left_df, self.right_df)
        print(f"Number of pairs: {len(candidate_links)}")
        self._features = self.compare_cl.compute(candidate_links, self.left_df, self.right_df)

        # --> Save scores for all pairs
        self._features.to_parquet(os.path.join(self.env_folder, f"{output_fname}.parquet"))

        # --> All field scores less than 'threshold' are reduced to zero.
        if threshold is not None and threshold<=1.0:
            self._features[self._features<threshold] = 0.0

        # --> Apply sum rules
        for item in self.sum_rules.items():
            key, value = item
            self._features[key] = self._features[value].sum(axis=1)

    def create_annotation(self, certain_pairs, potential_pairs, division=50, left_cols=None, right_cols=None, overwrite=False):
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

        temp_left_df = self.left_df
        temp_right_df = self.right_df
        # --> POTENTIAL PAIRS (SEPARATE IN DIFFERENT FILES)
        splitted_pot = np.split(potential_pairs, np.arange(division, potential_pairs.shape[0]+1, division))
        pot_list = []
        for n in range(len(splitted_pot)):
            current_pot_list = []
            for row in splitted_pot[n].iterrows():
                pair = row[0]
                if left_cols is not None and right_cols is not None:
                    left_pair = json.loads(temp_left_df[left_cols].loc[pair[0]].to_json())
                    right_pair = json.loads(temp_right_df[right_cols].loc[pair[1]].to_json())
                else:
                    left_pair = json.loads(temp_left_df.loc[pair[0]].to_json())
                    right_pair = json.loads(temp_right_df.loc[pair[1]].to_json())
                pair_element = {"a": left_pair, "b": right_pair, 
                                "identifiers": {"a": pair[0], "b": pair[1]}, 
                                "classification": None,
                                "keep": "a" }
                current_pot_list.append(pair_element)
            pot_list.append(current_pot_list)
        
        # --> CERTAIN PAIRS (ONE SINGLE FILE)
        certain_list = []
        for row in certain_pairs.iterrows():
            pair = row[0]
            if left_cols is not None and right_cols is not None:
                left_pair = json.loads(temp_left_df[left_cols].loc[pair[0]].to_json())
                right_pair = json.loads(temp_right_df[right_cols].loc[pair[1]].to_json())
            else:
                left_pair = json.loads(temp_left_df.loc[pair[0]].to_json())
                right_pair = json.loads(temp_right_df.loc[pair[1]].to_json())
            pair_element = {"a": left_pair, "b": right_pair, 
                            "identifiers": {"a": pair[0], "b": pair[1]}, 
                            "classification": "yes",
                            "keep": "a" }
            certain_list.append(pair_element)

        # -- OUTPUT
        certain_pairs_json = {"pairs": certain_list}
        with open(os.path.join(self.env_folder, "ANNOTATION", "MATCHED_PAIRS.json"), "w") as f:
            json.dump(certain_pairs_json, f)

        for n, cur_list in enumerate(pot_list):
            with open(os.path.join(self.env_folder, "ANNOTATION", f"POTENTIAL_PAIRS_{n}.json"), "w") as f:
                json.dump({"pairs": cur_list}, f)

    def load_annotated_pairs(self):
        '''
        
        '''
        annotation_folder = os.path.join(self.env_folder, "ANNOTATION")
        files = os.listdir(annotation_folder)
        potential_pairs_file = [ n for n in files if "MATCHED" not in n ]
        
        with open(os.path.join(annotation_folder, "MATCHED_PAIRS.json"), "r") as f:
            matched = json.load(f)
        
        pot = []
        for pot_fname in potential_pairs_file:
            with open(os.path.join(annotation_folder, pot_fname), "r") as f:
                cur_pairs = json.load(f)
            pot.append(cur_pairs)

        # --> Create Dataframe
        keep_ = {"a": "left", "b": "right"}
        pairs = {"left": [], "right": [], "keep": [], "match": []}
        for cur_pair in matched["pairs"]:
            left_id = cur_pair["identifiers"]["a"]
            right_id = cur_pair["identifiers"]["b"]
            to_keep = keep_[cur_pair["keep"]]
            is_match = cur_pair["classification"]

            pairs["left"].append(left_id)
            pairs["right"].append(right_id)
            pairs["keep"].append(to_keep)
            pairs["match"].append(is_match)

        for block in pot:
            for cur_pair in block["pairs"]:
                left_id = cur_pair["identifiers"]["a"]
                right_id = cur_pair["identifiers"]["b"]
                to_keep = keep_[cur_pair["keep"]]
                is_match = cur_pair["classification"]

                pairs["left"].append(left_id)
                pairs["right"].append(right_id)
                pairs["keep"].append(to_keep)
                pairs["match"].append(is_match)
        
        return pd.DataFrame(pairs)
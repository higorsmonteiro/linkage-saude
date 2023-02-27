'''
    Perform deduplication through a probabilistic(approximate) approach within a given database
    based on processed columns.

    Author: Higor S. Monteiro
    Update date: 2023-02-09
'''
import os
import json
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

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
        self._features = None
        if self.field_id is None:
            raise UniqueIdentifierMissing("Must provide an existing column as a unique identifier")
        if self.env_folder is None:
            raise OutputPathMissing("")

        self.main_df = self.main_df.set_index(field_id)
        if not os.path.isdir(self.env_folder):
            os.mkdir(self.env_folder)

        self.linkage_vars = None
        self.compare_cl, self._features, self.sum_rules = None, None, None

    @property
    def features(self):
        if self._features is not None:
            return self._features

    @property
    def scores(self):
        if self._features is not None:
            return self._features["SOMA FINAL"].value_counts().sort_index()

    @features.setter
    def features(self):
        raise AttributeError("Not possible to change this attribute from outside")

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
        candidate_links = indexer.index(self.main_df)
        print(f"Number of pairs: {len(candidate_links)}")
        self._features = self.compare_cl.compute(candidate_links, self.main_df)

        if self.env_folder is not None:
            self._features.to_parquet(os.path.join(self.env_folder, f"{output_fname}.parquet"))

        if threshold is not None and threshold<=1.0:
            self._features[self._features<threshold] = 0.0

        # apply sum rules
        for item in self.sum_rules.items():
            key, value = item
            self._features[key] = self._features[value].sum(axis=1)

    def summary_score(self, arr, bins, range_certain, range_potential, scale="linear"):
        '''
            ...
        '''
        fig, ax = plt.subplots(1, figsize=(7,4.5))
        s = sns.histplot(arr, bins=bins, color="#ff7f50", ax=ax)

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

        freq, bins = np.histogram(arr, bins=bins)
        ax.fill_between(range_potential, y1=max(freq)+10, color="tab:orange", alpha=0.2)
        ax.fill_between(range_certain, y1=max(freq)+10, color="tab:blue", alpha=0.2)
        #ax.set_ylim(0, max(freq)+10)

        ncertain = arr[(arr>=range_certain[0]) & (arr<range_certain[1])].shape[0]
        npotential = arr[(arr>=range_potential[0]) & (arr<range_potential[1])].shape[0]
        ndiff = arr[(arr<range_potential[0])].shape[0]

        ax.set_xticks(bins)
        ax.set_xticklabels([f"{n:.2f}" for n in bins], rotation=35)
        ax.set_yscale(scale)
        return {"FIG": fig, "AXIS": ax, "FREQUENCY AND BINS": (freq, bins), 
                "# IGUAIS": ncertain, "# POTENCIAIS": npotential, "# DIFERENTES": ndiff}

    def verify_pair_subset(self, subset, random_state=None, cols=None):
        '''
        
        '''
        temp_df = self.main_df
        index = subset.sample(n=1, random_state=random_state).index 
        if cols is not None:
            return pd.concat([temp_df[cols].loc[index[0][0]], temp_df[cols].loc[index[0][1]]], axis=1)
        return pd.concat([temp_df.loc[index[0][0]], temp_df.loc[index[0][1]]], axis=1)

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

        temp_df = self.main_df
        # --> POTENTIAL PAIRS (SEPARATE IN DIFFERENT FILES)
        splitted_pot = np.split(potential_pairs, np.arange(division, potential_pairs.shape[0]+1, division))
        pot_list = []
        for n in range(len(splitted_pot)):
            current_pot_list = []
            for row in splitted_pot[n].iterrows():
                pair = row[0]
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
        for row in certain_pairs.iterrows():
            pair = row[0]
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
        
            

       


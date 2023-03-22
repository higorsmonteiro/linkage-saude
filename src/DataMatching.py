'''
    Perform the linkage through a probabilistic(approximate) approach between two databases
    based on processed columns.

    Author: Higor S. Monteiro
    Email: higormonteiros@gmail.com
'''
import os
import json
import random
import numpy as np
import pandas as pd
import recordlinkage
from recordlinkage.index import SortedNeighbourhood

import lib.data_matching_utils as matching_utils
from src.CustomExceptions import *

class DataMatching:
    def __init__(self) -> None:
        '''
        
        '''
        self._features = None
        self.linkage_vars = None
        self.compare_cl, self._features, self.sum_rules = None, None, None

    @property
    def features(self):
        if self._features is not None:
            return self._features

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
    
# -->
# ------> DEDUPLICATION
# -->
class Deduple(DataMatching):
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

        self.main_df = self.main_df.set_index(field_id)
        if not os.path.isdir(self.env_folder):
            os.mkdir(self.env_folder)

        super().__init__()

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


    def verify_pair(self, pairs, cols=None, random_state=None):
        '''
            Visualization of one of the pairs parsed according to the columns' names given.

            Args:
            -----
                pairs:
                    List. List of 2-tuples containing the pairs parsed.
                cols:
                    List of Strings. Names to display when comparing the records.
                random_state:
                    Integer. To fix the seed of the display.
            Return:
            -------
                vis_df:
                    pandas.DataFrame.

        '''
        if random_state is not None:
            random.seed(random_state)
        
        temp_df = self.main_df
        pair = random.choice(pairs)
        left_index, right_index = pair[0], pair[1]

        if cols is not None:
            vis_df = pd.concat( [temp_df[cols].loc[left_index], temp_df[cols].loc[right_index]], axis=1 )
        else:
            vis_df = pd.concat( [temp_df.loc[left_index], temp_df.loc[right_index]], axis=1 )
        return vis_df

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
    

# -->
# ------> PROBABILISTIC LINKAGE
# -->
class PLinkage(DataMatching):
    def __init__(self, left_df, right_df, left_id, right_id, env_folder=None) -> None:
        '''
            Perform linkage between two dataframes based on linkage variables, possibly
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
        self.left_df = left_df.copy()
        self.right_df = right_df.copy()
        self.left_id = left_id
        self.right_id = right_id
        self.env_folder = env_folder

        if self.left_id is None:
            raise UniqueIdentifierMissing("Must provide an existing column as a unique identifier (LEFT ID)")
        if self.right_id is None:
            raise UniqueIdentifierMissing("Must provide an existing column as a unique identifier (RIGHT ID)")
        if self.env_folder is None:
            raise OutputPathMissing("")
        
        self.left_df = self.left_df.set_index(left_id)
        self.right_df = self.right_df.set_index(right_id)
        if not os.path.isdir(self.env_folder):
            os.mkdir(self.env_folder)

        super().__init__()

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

        if self.env_folder is not None:
            self._features.to_parquet(os.path.join(self.env_folder, f"{output_fname}.parquet"))

        if threshold is not None and threshold<=1.0:
            self._features[self._features<threshold] = 0.0

        # --> apply sum rules
        for item in self.sum_rules.items():
            key, value = item
            self._features[key] = self._features[value].sum(axis=1)

    def verify_pair(self, pairs, left_cols=None, right_cols=None, random_state=None):
        '''
            Visualization of one of the pairs parsed according to the columns' names given.

            Args:
            -----
                pairs:
                    List. List of 2-tuples containing the pairs parsed.
                cols:
                    List of Strings. Names to display when comparing the records.
                random_state:
                    Integer. To fix the seed of the display.

        '''
        if random_state is not None:
            random.seed(random_state)
        
        temp_left_df = self.left_df
        temp_right_df = self.right_df
        pair = random.choice(pairs)
        left_index, right_index = pair[0], pair[1]

        if left_cols is not None and right_cols is not None:
            vis_df = pd.concat( [temp_left_df[left_cols].loc[left_index], temp_right_df[right_cols].loc[right_index]], axis=1 )
        else:
            vis_df = pd.concat( [temp_left_df.loc[left_index], temp_right_df.loc[right_index]], axis=1 )
        return vis_df

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
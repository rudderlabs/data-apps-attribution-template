from math import factorial
from typing import List, Optional, Union, Dict, Tuple
import itertools
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
from collections import defaultdict


# Shapley values calculation: 

# Computing normalizing factor for marginal contribution
shapley_weight = lambda p, s: (factorial(s)*factorial(p-s-1)/factorial(p))

# Getting shapley value of a given channel and values mapping for channel subsets.
def compute_shapley_values(v_values_map: dict, 
                            channel: str,
                            n_channels: int) -> float:
    # Initiating shap with marginal contribution from S = null subset
    shap = 1/n_channels * v_values_map.get(channel, 0) 
    for subset_str, subset_contrib in v_values_map.items():
        subset = subset_str.split(",")
        if channel not in subset:
            subset_union_channel = ','.join(sorted(subset + [channel]))
            marginal_contrib = (v_values_map.get(subset_union_channel,0) -
                                v_values_map.get(subset_str,0))
            shap += shapley_weight(n_channels, len(subset)) * marginal_contrib
    
    return shap

# Helper function to generate subsets for a given set of channels
def generate_subsets(touchpoints: List[str]) -> List[List[str]]:
    subset_list = []
    for subset_size in range(len(touchpoints)):
        for subset in itertools.combinations(touchpoints, subset_size + 1):
            subset_list.append(list(sorted(subset)))
    return subset_list

# Computes the utility function value v(s) for given subset S, using contribution values for all subsets.
def utility_function(touchpoint_set: List[str], 
                     contributions_mapping: dict) -> Union[int, float]:
    subset_list = generate_subsets(touchpoint_set)
    return sum([contributions_mapping.get(','.join(subset),0) for subset in subset_list])


# Master function combining all the above functions to compute shapley values for each touchpoint from a list of journeys. 
def get_shapley_values(journeys_list: List[List[str]], 
                       contribs_list: List[Union[int, float]])->Optional[Dict[str, float]]:
    """

    Args:
        journeys_list (List[List[str]]): List of journeys.
         Each journey is a list of touchpoints..
        contribs_list (List[Union[int, float]]): List of contributions corresponding to each journey in journeys_list.
         Should have same length as journeys_list

    Returns:
        Dict[str, float]: A dictionary with key as channel/touchpoint, and Shapley value as its value 
    """
    try:
        flattened_journeys = [channel for journey in journeys_list for channel in set(journey)]
        unique_channels = sorted(list(set(flattened_journeys)))
        all_subsets = generate_subsets(unique_channels)
        contrib_map = {}
        for n, journey in enumerate(journeys_list):
            journey_ = ",".join(sorted(set(journey))) # Ensures deduplication and sorting of journeys
            contrib_map[journey_] = contrib_map.get(journey_,0) + contribs_list[n]
        v_values = {}
        for subset in all_subsets:
            v_values[",".join(subset)] = utility_function(subset, contrib_map)

        shapley_values = {}
        for channel in unique_channels:
            shapley_values[channel] = compute_shapley_values(v_values, 
                                                              channel, 
                                                              len(unique_channels))
        return shapley_values
    except Exception as e:
        print(e)
        return None
    
#  Markov chain values



def generate_transition_counts(journey_list: List[List[str]], 
                               distinct_touches_list: List[str], 
                               is_positive: bool):
    if is_positive:
        destination_idx = -1
    else:
        destination_idx = -2
    transition_counts = np.zeros(((len(distinct_touches_list)+3), (len(distinct_touches_list)+3)))
    for journey in journey_list:
        transition_counts[0, (distinct_touches_list.index(journey[0])+1)] += 1 # First point in the path
        for n, touch_point in enumerate(journey):
            if n == len(journey) - 1:
                # Reached last point
                transition_counts[(distinct_touches_list.index(touch_point)+1), destination_idx] += 1
                transition_counts[destination_idx, destination_idx]+=1
            else:
                transition_counts[(distinct_touches_list.index(touch_point)+1), (distinct_touches_list.index(journey[n+1]) + 1)] +=1
    transition_labels = distinct_touches_list.copy()
    transition_labels.insert(0, "Start")
    transition_labels.extend(["Dropoff", "Converted"])
    return transition_counts, transition_labels

row_normalize_np_array = lambda transition_counts: transition_counts / transition_counts.sum(axis=1)[:, np.newaxis]

def plot_transitions(transition_probabilities: np.array, labels: List[str], title="Transition Probabilities", show_annotations=True):
    ax = sns.heatmap(transition_probabilities,
                     linewidths=0.5,
                     robust=True, 
                     annot_kws={"size":8}, 
                     annot=show_annotations,
                     fmt=".2f",
                     cmap="YlGnBu",
                     xticklabels=labels,
                     yticklabels=labels)
    ax.tick_params(labelsize=10)
    ax.figure.set_size_inches((16, 10))
    ax.set_ylabel("Previous Step")
    ax.set_xlabel("Next Step")
    ax.set_title(title);


def get_transition_probabilities(converted_touchpoints_list: List[List[int]], 
                                 dropoff_touchpoints_list: List[List[int]], 
                                 distinct_touches_list: List[str], 
                                 visualize=False) -> Tuple[np.array, List[str]]:
    pos_transitions, _ = generate_transition_counts(converted_touchpoints_list, distinct_touches_list, is_positive=True)
    neg_transitions, labels = generate_transition_counts(dropoff_touchpoints_list, distinct_touches_list, is_positive=False)
    all_transitions = pos_transitions + neg_transitions
    transition_probabilities = row_normalize_np_array(all_transitions)
    if visualize:
        plot_transitions(transition_probabilities, labels, show_annotations=True)
    return transition_probabilities, labels

def converge(transition_matrix, max_iters=200, verbose=True):
    T_upd = transition_matrix
    prev_T = transition_matrix
    for i in range(max_iters):
        T_upd = np.matmul(transition_matrix, prev_T)
        if np.abs(T_upd - prev_T).max()<1e-5:
            if verbose:
                print(f"{i} iters taken for convergence")
            return T_upd
        prev_T = T_upd
    if verbose:
        print(f"Max iters of {max_iters} reached before convergence. Exiting")
    return T_upd


def get_removal_affects(transition_probs, labels, ignore_labels=["Start", "Dropoff","Converted"], default_conversion=1.):
    removal_affect = {}
    for n, label in enumerate(labels):
        if label in ignore_labels:
            continue
        else:
            drop_transition = transition_probs.copy()
            drop_transition[n,:] = 0.
            drop_transition[n,-2] = 1.
            drop_transition_converged = converge(drop_transition, 500, False)
            removal_affect[label] = default_conversion - drop_transition_converged[0,-1]
    return removal_affect

def get_markov_attribution(tp_list_positive: List[List[int]],
                           tp_list_negative: List[List[int]], 
                           distinct_touches_list: List[str], 
                           visualize=False) -> Tuple[Dict[str, float], np.array]:
    transition_probabilities, labels = get_transition_probabilities(tp_list_positive, tp_list_negative, distinct_touches_list, visualize=visualize)
    transition_probabilities_converged = converge(transition_probabilities, max_iters=500, verbose=False)
    removal_affects = get_removal_affects(transition_probabilities, labels, default_conversion=transition_probabilities_converged[0,-1])
    total_conversions = len(tp_list_positive)
    attributable_conversions = {}
    total_weight = sum(removal_affects.values())
    for tp, weight in removal_affects.items():
        attributable_conversions[tp] = weight/total_weight * total_conversions
    return attributable_conversions, transition_probabilities

# First touch and last touch

def get_single_touch_attribution(df: pd.DataFrame, col_events: str, last_touch: bool, normalize: bool) -> Optional[dict]:
    try:
        if last_touch:
            idx = -1
        else:
            idx = 0
        return df[col_events].apply(lambda event_list: event_list[idx]).value_counts(normalize=normalize).to_dict()
    except Exception as e:
        print(e)
        return None

# Util functions

def merge_dictionaries(dictionaries:Tuple[Optional[dict]], labels:Tuple[str]) -> pd.DataFrame:
    merged_dict = defaultdict(dict)
    for n, (dictionary, label) in enumerate(zip(dictionaries, labels)):
        if dictionary is not None:
            for key, value in dictionary.items():
                merged_dict[key][label] = value
    return pd.DataFrame.from_dict(merged_dict, orient='index')


# Test cases: 
# Case 1: When one of the dicts is empty
assert (merge_dictionaries([None, {"a":1,"b":2}, {"a":4, "b":5}] , ['c1', 'c2', 'c3']) == 
        pd.DataFrame.from_dict({"a":[1,4],"b":[2,5]}, orient='index',columns=['c2','c3'])).all().all()

# Case 2: Mismatching in touch points. Some touchpoints are missing in one of the dictionaries
assert (merge_dictionaries([{"a":1,"b":2}, {"a":1}, {"a":4, "b":5}] , ['c1', 'c2', 'c3']).fillna(-1) == 
        pd.DataFrame.from_dict({"a":[1,1,4],"b":[2,None,5]}, orient='index',columns=['c1','c2','c3']).fillna(-1)).all().all()
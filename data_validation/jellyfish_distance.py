# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import jellyfish


def extract_closest_match(search_key, target_list, score_cutoff=0):
    """Return str value from target list with highest score using Jaro
    for String distance.

     search_key (str): A string used to search for closest match.
     target_list (list): A list of strings for comparison.
     score_cutoff (float): A score cutoff (betwen 0 and 1) to be met.
    """
    highest_score = score_cutoff
    highest_value_key = None

    for target_key in target_list:
        score = jellyfish.jaro_similarity(search_key, target_key)
        if score >= highest_score:
            highest_score = score
            highest_value_key = target_key

    return highest_value_key

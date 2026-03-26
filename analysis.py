"""
ie-cor cognate set clustering
this script does one thing: looks through the alsoUsedInOtherMeanings field
in cognateclass.csv and groups together any cognate set IDs that reference
each other. the result is a csv where each row is a cluster of cognate sets
that all trace back to the same root, even if they cover different meanings.

this is the starting point declan described. just collecting the IDs.
meaning names, root forms, and deeper analysis can be layered on later.

input:  cognateclass.csv
output: id_clusters.csv
"""

import pandas as pd
import re
from collections import defaultdict

# load the file
# read the cognateclass csv into a datafram
df = pd.read_csv("cognateclass.csv", low_memory=False)
print(f"loaded {len(df)} rows from cognateclass.csv")
# parse the alsoUsedInOtherMeanings field
# this field contains free-text notes like: "Cf. 'freeze' in /cognate/881/"
# we want to extract just the numeric IDs from those strings
# re.findall uses a pattern to pull out all numbers that appear in /cognate/NUMBER/ format
def extract_ids(text):
    # if the cell is empty, return an empty list
    if pd.isna(text):
        return []
    # find all occurrences of /cognate/NUMBER/ and return just the numbers as integers
    return [int(x) for x in re.findall(r"/cognate/(\d+)/", str(text))]

# build a map of which IDs are connected to which
# adjacency is a dictionary where each key is a cognate set ID,
# and its value is the set of all IDs it is connected to (including itself)
adjacency = defaultdict(set)
for _, row in df.iterrows():
    source_id = row["id"]
    referenced_ids = extract_ids(row["alsoUsedInOtherMeanings"])
    # only process rows that actually reference other cognate sets
    if referenced_ids:
        # add the source id to its own group
        adjacency[source_id].add(source_id)
        # add each referenced id, and make the connection go both ways
        for ref_id in referenced_ids:
            adjacency[source_id].add(ref_id)
            adjacency[ref_id].add(source_id)
print(f"found {len(adjacency)} cognate sets with cross-meaning references")

# merge overlapping groups into final clusters
# the problem: if A references B, and B references C, then A, B, and C
# should all be in the same cluster -- even if A never directly mentions C
# union-find is a standard algorithm for merging overlapping groups like this - referenced ai/claude 
# parent[x] tracks which "representative" ID stands for x's group
parent = {}
def find(x):
    # every id starts as its own representative
    parent.setdefault(x, x)
    # follow the chain up to find the root representative of this group
    if parent[x] != x:
        parent[x] = find(parent[x])  # path compression
    return parent[x]

def union(a, b):
    # merge the groups that a and b belong to
    parent[find(a)] = find(b)

# go through all connections and merge groups accordingly
for node, neighbors in adjacency.items():
    for neighbor in neighbors:
        union(node, neighbor)

# now group all ids by their shared representative
clusters = defaultdict(set)
for node in parent:
    clusters[find(node)].add(node)

print(f"merged into {len(clusters)} clusters")

# write output
# build one row per cluster for the output csv
rows = []
for representative, members in clusters.items():
    rows.append({
        # sort the ids so the output is consistent and easy to read
        "cognateset_ids": " | ".join(str(i) for i in sorted(members)),
        "n_cognatesets":  len(members),
    })

# sort so the largest clusters appear first
output = pd.DataFrame(rows).sort_values("n_cognatesets", ascending=False)
output.to_csv("id_clusters.csv", index=False)

print(f"done -- {len(output)} clusters written to id_clusters.csv")
print("\ntop 5 largest clusters:") # 5 small enough number for unknown length but can be altered
print(output.head().to_string(index=False))
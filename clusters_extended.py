import pandas as pd

# file paths
CLUSTERS_CSV = "id_clusters.csv"
FORMS_CSV = "forms.csv"
COGNATES_CSV = "cognates.csv"
PARAMETERS_CSV = "parameters.csv"

# load files
print("loading files...")
clusters = pd.read_csv(CLUSTERS_CSV)
forms = pd.read_csv(FORMS_CSV)
cognates = pd.read_csv(COGNATES_CSV)
parameters = pd.read_csv(PARAMETERS_CSV)

# build lookup: parameter id -> concept name
param_lookup = dict(zip(parameters["ID"], parameters["Name"]))

# build lookup: form id -> (form string, meaning)
form_lookup = {}
for _, row in forms.iterrows():
    form_lookup[row["ID"]] = {
        "form": str(row.get("Form", "")),
        "param_id": row.get("Parameter_ID", "")
    }

# build lookup: cognate set id -> list of (form, meaning) pairs
print("building cognate set -> forms lookup...")

# figure out column names (they vary slightly across ie-cor versions)
cog_cols = cognates.columns.tolist()
print(f"  cognates columns: {cog_cols}")

# try common column name patterns
def find_col(df, candidates):
    for c in candidates:
        if c in df.columns:
            return c
    return None

cogset_col = find_col(cognates, ["Cognateset_ID", "Cognate_Set_ID", "cognateset_id", "CognatesetID"])
formid_col = find_col(cognates, ["Form_ID", "form_id", "FormID"])

print(f"  using cognateset col: {cogset_col}")
print(f"  using form_id col: {formid_col}")

cogset_to_forms = {}
for _, row in cognates.iterrows():
    cog_id = row.get(cogset_col)
    form_id = row.get(formid_col)
    if pd.isna(cog_id) or pd.isna(form_id):
        continue
    try:
        cog_id = int(cog_id)
    except (ValueError, TypeError):
        continue
    if cog_id not in cogset_to_forms:
        cogset_to_forms[cog_id] = []
    if form_id in form_lookup:
        entry = form_lookup[form_id]
        meaning = param_lookup.get(entry["param_id"], "")
        cogset_to_forms[cog_id].append({
            "form": entry["form"],
            "meaning": str(meaning)
        })

# process each cluster and expand by root form
print("processing clusters...")
output_rows = []

for _, cluster_row in clusters.iterrows():
    # parse pipe-separated cognate set ids
    raw_ids = str(cluster_row["cognateset_ids"])
    cog_ids = [int(x.strip()) for x in raw_ids.split("|") if x.strip().lstrip('-').isdigit()]

    # parse root forms and languages
    raw_roots = str(cluster_row.get("root_forms", ""))
    raw_langs = str(cluster_row.get("root_languages", ""))
    root_list = [r.strip() for r in raw_roots.split("|")]
    lang_list = [l.strip() for l in raw_langs.split("|")]

    # pad lang list to match root list
    while len(lang_list) < len(root_list):
        lang_list.append("")

    # gather all reflexes and meanings for this cluster
    all_forms = []
    all_meanings = []
    for cog_id in cog_ids:
        for entry in cogset_to_forms.get(cog_id, []):
            f = entry["form"]
            m = entry["meaning"]
            if f and f != "nan":
                all_forms.append(f)
            if m and m != "nan":
                all_meanings.append(m)

    reflexes_str = " | ".join(all_forms)
    meanings_str = " | ".join(all_meanings)
    distinct_count = len(set(m for m in all_meanings if m))

    # one row per unique root form
    seen_roots = set()
    for root, lang in zip(root_list, lang_list):
        if not root or root == "nan":
            continue
        if root in seen_roots:
            continue
        seen_roots.add(root)

        output_rows.append({
            "etymon": root,
            "root_language": lang if lang != "nan" else "",
            "german_meaning (LIV)": "",
            "cognate_sets": " | ".join(str(i) for i in cog_ids),
            "reflexes": reflexes_str,
            "meanings of reflexes": meanings_str,
            "number of distinct meanings": distinct_count
        })

# write output, as csv in the format discussed in the meeting
output_df = pd.DataFrame(output_rows)
output_df.to_csv("iecor_enriched.csv", index=False)

print(f"\ndone!")
print(f"  total rows: {len(output_df)}")
print(f"  unique etyma: {output_df['etymon'].nunique()}")
print(f"\nsample output (first 3 rows):")
print(output_df.head(3).to_string())
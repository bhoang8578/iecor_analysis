import pandas as pd
from rdflib import Graph, Namespace
from rdflib.namespace import RDFS

# file paths
CLUSTERS_CSV   = "id_clusters.csv"
FORMS_CSV      = "forms.csv"
COGNATES_CSV   = "cognates.csv"
PARAMETERS_CSV = "parameters.csv"
LIV_TTL        = "liv.ttl"

# path to your hand-curated gloss lookup table
# format: two tab-separated columns, no header
#   column 1: pie etymon label exactly as it appears in liv_pie_etymon output
#   column 2: english gloss for that root
# example row:
#   *bʰer-	to carry, bear
# leave this as an empty string if you haven't created the file yet
# the script will skip the lookup gracefully and leave the column blank
GLOSS_TSV = "pie_glosses.tsv"

# load IE-CoR files
print("loading IE-CoR files...")
clusters   = pd.read_csv(CLUSTERS_CSV)
forms      = pd.read_csv(FORMS_CSV)
cognates   = pd.read_csv(COGNATES_CSV)
parameters = pd.read_csv(PARAMETERS_CSV)

# parameter id -> concept name
param_lookup = dict(zip(parameters["ID"], parameters["Name"]))

# form id -> {form string, param_id}
form_lookup = {}
for _, row in forms.iterrows():
    form_lookup[row["ID"]] = {
        "form":     str(row.get("Form", "")),
        "param_id": row.get("Parameter_ID", "")
    }

# detect column names in cognates, which vary across ie-cor versions
def find_col(df, candidates):
    for c in candidates:
        if c in df.columns:
            return c
    return None

cogset_col = find_col(cognates, ["Cognateset_ID", "Cognate_Set_ID",
                                  "cognateset_id", "CognatesetID"])
formid_col = find_col(cognates, ["Form_ID", "form_id", "FormID"])
print(f"  cognateset col : {cogset_col}")
print(f"  form_id col    : {formid_col}")

# cognate set id -> list of {form, meaning}
print("building cognate set -> forms lookup...")
cogset_to_forms = {}
for _, row in cognates.iterrows():
    cog_id  = row.get(cogset_col)
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
        entry   = form_lookup[form_id]
        meaning = param_lookup.get(entry["param_id"], "")
        cogset_to_forms[cog_id].append({
            "form":    entry["form"],
            "meaning": str(meaning)
        })

# parse the lila liv turtle file to build a latin lemma -> pie root mapping
# the public lila file contains no german gloss literals, only structural links
# so we use this to find the liv's own pie root label for a cluster,
# which then serves as the key for the english gloss lookup below
latin_to_pie = {}

try:
    print("parsing LiLa LIV Turtle file...")
    g = Graph()
    g.parse(LIV_TTL, format="turtle")

    qres = g.query("""
        PREFIX rdfs:     <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX lemonEty: <http://lari-datasets.ilc.cnr.it/lemonEty#>
        PREFIX ontolex:  <http://www.w3.org/ns/lemon/ontolex#>

        SELECT ?latinLabel ?pieLabel
        WHERE {
            ?entry  a                   ontolex:LexicalEntry ;
                    rdfs:label          ?latinLabel ;
                    lemonEty:etymology  ?etym .
            ?etym   lemonEty:etymon     ?etymon .
            ?etymon rdfs:label          ?pieLabel .
        }
    """)

    for row in qres:
        latin = str(row.latinLabel).strip().lower()
        pie   = str(row.pieLabel).strip()
        # keep first match only; a latin verb can have multiple etyma
        if latin not in latin_to_pie:
            latin_to_pie[latin] = pie

    print(f"  loaded {len(latin_to_pie)} latin -> pie mappings from LIV")

except FileNotFoundError:
    print(f"  WARNING: {LIV_TTL} not found, skipping LIV alignment")
except Exception as e:
    print(f"  WARNING: could not parse LIV turtle: {e}")

# load the hand-curated english gloss lookup
# keys are pie etymon labels matching what the lila liv outputs
# e.g. "*bʰer-" -> "to carry, bear"
# this is the step that actually populates the english_meaning column
pie_to_gloss = {}

try:
    gloss_df = pd.read_csv(GLOSS_TSV, sep="\t", header=None,
                           names=["etymon", "gloss"],
                           dtype=str)
    # strip whitespace from both columns so minor formatting differences
    # in the tsv don't cause silent misses
    gloss_df["etymon"] = gloss_df["etymon"].str.strip()
    gloss_df["gloss"]  = gloss_df["gloss"].str.strip()
    pie_to_gloss = dict(zip(gloss_df["etymon"], gloss_df["gloss"]))
    print(f"  loaded {len(pie_to_gloss)} english glosses from {GLOSS_TSV}")
except FileNotFoundError:
    print(f"  NOTE: {GLOSS_TSV} not found, english_meaning column will be blank")
    print(f"        create the file when ready and re-run to populate it")
except Exception as e:
    print(f"  WARNING: could not load gloss TSV: {e}")

# scan a cluster's reflex forms for any latin lemma present in the lila liv data
# returns the pie etymon label from the liv if found, otherwise empty string
def lookup_liv_etymon(reflex_forms_str):
    if not latin_to_pie or not reflex_forms_str:
        return ""
    for token in reflex_forms_str.split("|"):
        candidate = token.strip().lower()
        if candidate in latin_to_pie:
            return latin_to_pie[candidate]
    return ""

# look up an english gloss for a pie etymon label
# the pie_etymon argument is whatever lookup_liv_etymon returned
# returns empty string if no match in the tsv
def lookup_english_gloss(pie_etymon):
    if not pie_to_gloss or not pie_etymon:
        return ""
    # try exact match first
    if pie_etymon in pie_to_gloss:
        return pie_to_gloss[pie_etymon]
    # try stripping the disambiguation suffix e.g. "*bʰer-{1}" -> "*bʰer-"
    # some lila labels carry a numbered suffix that may not appear in your tsv
    base = pie_etymon.split("{")[0].strip()
    return pie_to_gloss.get(base, "")

# build output: one row per reflex
# etyma, cognate sets, liv etymon, english meaning, and meanings of reflexes
# are all cluster-level attributes so they repeat on every reflex row
# this keeps the reflex as the unit of granularity while preserving
# all cluster context on each row
print("processing clusters...")
output_rows = []

for _, cluster_row in clusters.iterrows():

    # parse pipe-separated cognate set ids for this cluster
    raw_ids = str(cluster_row["cognateset_ids"])
    cog_ids = [
        int(x.strip())
        for x in raw_ids.split("|")
        if x.strip().lstrip("-").isdigit()
    ]

    # collect root forms and languages, deduplicating while preserving order
    raw_roots = str(cluster_row.get("root_forms", ""))
    raw_langs = str(cluster_row.get("root_languages", ""))
    root_list = [r.strip() for r in raw_roots.split("|")
                 if r.strip() and r.strip() != "nan"]
    lang_list = [l.strip() for l in raw_langs.split("|")]

    seen = set()
    unique_roots = []
    unique_langs = []
    for r, l in zip(root_list, lang_list + [""] * len(root_list)):
        if r not in seen:
            seen.add(r)
            unique_roots.append(r)
            unique_langs.append(l if l != "nan" else "")

    # these are cluster-level attributes that repeat on every reflex row
    etyma_str     = " | ".join(unique_roots)
    langs_str     = " | ".join(unique_langs)
    cogsets_str   = " | ".join(str(i) for i in cog_ids)

    # gather all reflex forms and meanings across every cognate set
    # reflexes are collected individually since each gets its own row
    # meanings stay grouped as a cluster-level attribute
    all_forms    = []
    all_meanings = []
    for cog_id in cog_ids:
        for entry in cogset_to_forms.get(cog_id, []):
            f = entry["form"]
            m = entry["meaning"]
            if f and f != "nan":
                all_forms.append(f)
            if m and m != "nan":
                all_meanings.append(m)

    # meanings stay joined as a single cluster-level string
    meanings_str   = " | ".join(all_meanings)
    distinct_count = len(set(m for m in all_meanings if m))

    # attempt to match this cluster to a lila liv pie etymon via its latin reflexes
    # pass all forms joined so lookup_liv_etymon can scan across them
    liv_pie_etymon  = lookup_liv_etymon(" | ".join(all_forms))
    english_meaning = lookup_english_gloss(liv_pie_etymon)

    # emit one row per individual reflex form
    # if the cluster has no reflexes at all, emit one row with a blank reflex
    # so the cluster is still represented in the output
    if not all_forms:
        output_rows.append({
            "etyma":                       etyma_str,
            "root_languages":              langs_str,
            "liv_pie_etymon":              liv_pie_etymon,
            "english_meaning":             english_meaning,
            "cognate_sets":                cogsets_str,
            "reflex":                      "",
            "meanings of reflexes":        meanings_str,
            "number of distinct meanings": distinct_count,
        })
    else:
        for reflex in all_forms:
            output_rows.append({
                "etyma":                       etyma_str,
                "root_languages":              langs_str,
                "liv_pie_etymon":              liv_pie_etymon,
                "english_meaning":             english_meaning,
                "cognate_sets":                cogsets_str,
                "reflex":                      reflex,
                "meanings of reflexes":        meanings_str,
                "number of distinct meanings": distinct_count,
            })

# write output csv
output_df = pd.DataFrame(output_rows)
output_df.to_csv("iecor_enriched.csv", index=False)

print(f"\ndone!")
print(f"  total rows (= reflexes)      : {len(output_df)}")
print(f"  unique clusters              : {output_df['cognate_sets'].nunique()}")
print(f"  clusters with LIV match      : {(output_df['liv_pie_etymon'] != '').sum()}")
print(f"  clusters with english gloss  : {(output_df['english_meaning'] != '').sum()}")
print(f"\nsample output (first 3 rows):")
print(output_df.head(3).to_string())
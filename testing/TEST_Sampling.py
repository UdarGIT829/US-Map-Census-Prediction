from Dataset_Builder import PhaseB_1, PhaseB_2   # or PhaseB_1 for user targets
from feature_families import FeatureFamilies
from recipe_sampler import RecipeConfig, run_recipe_sampling

# 1) Build dataset (no change to your existing flow)
dataset = PhaseB_1(
    level="state",
    exclude_states_fips=[2, 15],
)
X = dataset["X"]
y = dataset["y"]

# 2) Load feature family tree (Phase 1 output)
ff = FeatureFamilies.from_json("feature_families.json")

print("HERE")
cfg = RecipeConfig(
    n_recipes=200,          # target total, but will be raised if coverage needs more
    min_families=1,
    max_families=3,
    min_features=5,         # you can lower this now, too
    max_features=30,        # smaller chunks → more recipes
    cv_folds=3,
    random_state=42,
    coverage_rounds=1,      # <-- key change
)

results = run_recipe_sampling(X, y, ff, cfg)

column_scores = results["column_scores"]
family_scores = results["family_scores"]

print("Column score average: ", end="")
print(column_scores["mean_score"].mean())
print(column_scores.head(n=10))
print(family_scores.head(n=10))

    ### Printing result
from dp_labels import load_dp_mapping

# outside run_recipe_sampling, e.g. in your analysis script:
dp_map = load_dp_mapping("dp_mapping.json")
ff = FeatureFamilies.from_json("feature_families.json")


annotated = annotate_column_scores(
    col_scores,
    ff,
    dp_map,
    include_concept=True,
    top_n=30,    # show top 30
)

print(annotated.to_string(index=False))

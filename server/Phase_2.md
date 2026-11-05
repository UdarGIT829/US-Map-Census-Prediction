PHASE 2 — Recipe Sampler
========================
Last updated: 2026-04-13


OVERVIEW
--------
Phase 2 is the combination-based feature evaluation engine. It sits immediately
after Phase 1 screening in the pipeline and before any final model training or
result delivery.

Phase 2 does not select a single best model. Instead, it answers a different
question: given a pre-screened set of ACS features, which features and feature
families tend to produce higher predictive performance when combined with others?
It answers this by repeatedly sampling random subsets of features ("recipes"),
training a CatBoost model on each subset using 3-fold cross-validation, and
aggregating the per-recipe scores back to the column and family level.

The key insight is that univariate correlation (Phase 1) cannot detect which
features are most useful in combination. A variable may be weakly correlated
with y on its own but consistently improve model performance when paired with
complementary variables. Recipe sampling surfaces this combination-aware signal.

Phase 2 does NOT:
  - Select a final model for deployment
  - Tune hyperparameters
  - Guarantee causal relationships
  - Handle the collinearity problem directly (it circumvents it by never
    training on the full feature set at once)


WHY RECIPE SAMPLING EXISTS
---------------------------
After Phase 1, the feature set is reduced from ~528 columns to roughly 50–150
passing columns. This is still too many for a reliable multi-column model at
n=51 (state level), but it is small enough for exhaustive-ish combination
exploration.

The specific problems recipe sampling solves:

1. COMBINATION EFFECTS
   Univariate screening ranks features independently. Two features that are each
   weakly correlated with y may jointly explain variance that neither explains
   alone (e.g. poverty rate + insurance coverage together predict health outcomes
   better than either alone). Recipe sampling detects this.

2. REDUNDANCY DETECTION (IMPLICIT)
   If two features are nearly collinear (e.g. median household income and
   poverty rate), recipes containing both will not consistently outscore recipes
   containing only one. Their individual column scores will be similar. This
   implicitly penalises redundant pairs without requiring explicit collinearity
   handling.

3. FAMILY-LEVEL SIGNAL
   ACS families (DP02 Social, DP03 Economic, DP04 Housing, DP05 Demographic)
   contain many correlated variables. By aggregating recipe scores to the family
   and subfamily level, Phase 2 identifies which conceptual domains are most
   predictive of the target — a more interpretable output than a ranked column
   list.

4. PAIRWISE CO-ELEVATION
   The pairwise scores output identifies feature pairs that consistently appear
   together in high-scoring recipes, above and beyond what either feature
   achieves alone (lift). This is the most direct signal of genuine combination
   effects.


HOW RECIPE SAMPLING WORKS
--------------------------
A "recipe" is a list of ACS feature column names. For each recipe, Phase 2:
  1. Subsets X to the recipe's columns
  2. Drops rows with any NaN in those columns or in y
  3. Runs 3-fold cross-validation with a CatBoostRegressor
  4. Records the mean and std of R² across the 3 folds

The full sampling run has two phases:

COVERAGE PHASE (runs first):
  Every usable feature is guaranteed to appear in at least one recipe. The
  procedure is:
    - Flatten all features across all families into a single deduplicated list
    - For each coverage round (default: 1):
        - Shuffle the list
        - Partition into chunks of size up to max_features
  Each coverage round adds one set of recipes where every feature appears
  exactly once with a different random set of neighbors. With coverage_rounds=1,
  each feature appears in ~1 recipe. With coverage_rounds=5 (the default in
  RecipeConfig), each feature appears in ~5 recipes before random sampling begins.
  Coverage recipes are derived from the family tree so that family membership
  is still tracked per recipe.

RANDOM PHASE (runs second):
  After coverage recipes are generated, the remainder of the n_recipes budget
  is filled with randomly sampled recipes. Each random recipe:
    - Picks 1–max_families top-level families (randomly)
    - Draws a random number of features from those families
      (between min_features and max_features, distributed proportionally
      across the chosen families)
  If the coverage phase alone exceeds n_recipes, random sampling is skipped
  entirely to preserve coverage guarantees.


EVALUATION MODEL AND CV STRATEGY
----------------------------------
_evaluate_recipe() uses CatBoostRegressor with 3-fold cross-validation. This
is the intended design for the rewritten code, replacing the earlier
ElasticNet + single train/val split approach.

WHY CATBOOST OVER ELASTICNET:
  CatBoost's ordered boosting is specifically designed for small-sample regimes.
  At n=51, ElasticNet's linear assumptions are a real constraint, whereas
  CatBoost can capture non-linear relationships that are plausible in ACS data
  (e.g. threshold effects in poverty or housing cost variables). CatBoost also
  requires no feature scaling, removing the StandardScaler from the pipeline.

WHY 3-FOLD CV OVER SINGLE SPLIT:
  The recipe sampler's job is ranking, not prediction. For stable relative
  rankings at n=51, single-split R² has enormous variance — a bad random split
  can flip the ranking of two features entirely. 3-fold CV gives train sizes of
  ~34 rows. That is small, but it is exactly the regime CatBoost is designed
  for, and the mean over 3 folds is substantially more stable than a single
  split score. 5-fold was considered but rejected: at n=51 it only adds ~7 rows
  per fold over 3-fold and costs 67% more compute per recipe.

WHY NOT K-FOLD IN Training.py's train_elasticnet():
  That function is the outer final-model evaluation layer, distinct from the
  inner per-recipe CV. The two CV uses have different roles:
    - Inner CV (inside _evaluate_recipe): stable relative ranking of recipes
      and features during the sampling phase.
    - Outer CV (in train_elasticnet): confirmation that the final selected
      model generalises, reported as a metric in the paper.

SCORE_STD IS NOW MEANINGFUL:
  With 3-fold CV, score_std is the standard deviation of R² across the 3 folds,
  not a hardcoded 0.0. A recipe with high mean but high std is unstable — its
  apparent performance depends heavily on which rows land in which fold. Column
  ranking should use mean_score - score_std as a conservative criterion rather
  than mean_score alone, to deprioritise features that only score well on
  lucky splits.

IMPLEMENTATION (target state for _evaluate_recipe):

  from sklearn.model_selection import cross_val_score
  from catboost import CatBoostRegressor

  model = CatBoostRegressor(
      iterations=200,
      depth=4,
      learning_rate=0.1,
      loss_function='RMSE',
      random_seed=cfg.random_state,
      verbose=0,
  )
  scores = cross_val_score(model, X_clean, y_clean, cv=cfg.cv_folds, scoring='r2')
  score_mean = scores.mean()
  score_std = scores.std()

  cfg.cv_folds defaults to 3 and is now wired into cross_val_score. It is
  configurable without any interface change — callers that previously set it
  to 3 and saw it ignored will now have it respected.


FILE STRUCTURE
--------------
recipe_sampler.py          — Phase 2 core module (self-contained)
  RecipeConfig             — dataclass, all sampling parameters
  RecipeResult             — dataclass, per-recipe output
  run_recipe_sampling()    — public entry point, returns result dict
  annotate_column_scores() — enriches column_scores with labels and family paths
  _build_coverage_recipes()    — coverage phase recipe construction
  _build_family_to_columns()   — maps top-level families to X columns via ff
  _filter_constant_columns()   — drops constant/all-NaN columns before sampling
  _sample_recipe_features()    — samples one random recipe (family-stratified)
  _evaluate_recipe()           — runs 3-fold CV with CatBoostRegressor per recipe

Training.py                — standalone ElasticNet training script (CLI)
  build_dataset()          — thin wrapper around PhaseB_1 / PhaseB_2
  train_elasticnet()       — full train/test split + 3-fold CV + joblib save.
                             This is the OUTER validation layer (final model
                             confirmation), distinct from the INNER 3-fold CV
                             inside _evaluate_recipe() (recipe ranking).
  main()                   — CLI entrypoint (argparse)
  NOTE: Training.py uses the OLD pipeline (dp_mapping.json, static ff.json,
  userdata_cancerDataset_provider). It must be rewired to the Phase 0/1
  metadata flow before it can be used in production. See INTEGRATION STATUS.

godot_friendly_api.py      — FastAPI HTTP server, Godot-facing
  /analyze  POST           — full pipeline endpoint (dataset build → Phase 2)
  NOTE: Also uses the OLD pipeline. See INTEGRATION STATUS.


INPUT CONTRACT
--------------
run_recipe_sampling(X, y, ff, cfg) expects:

  X    — pd.DataFrame, rows=geographies, columns=DP variable codes.
         Must already be filtered to E-suffix passing columns (Phase 1 output).
         Values are numeric (float/NaN). Constant columns are dropped internally.

  y    — pd.Series or np.ndarray, same index as X (if Series).
         The target variable (e.g. cancer incidence, opioid death rate).

  ff   — FeatureFamilies instance built from passing_columns only.
         This MUST be filtered_ff from Phase 1, not the full family tree.
         Columns in X that are absent from ff.col_index are silently ignored
         (they will not appear in any recipe or any output).

  cfg  — RecipeConfig instance. If None, defaults are used.


OUTPUT CONTRACT
---------------
run_recipe_sampling() returns a dict with four keys:

  "recipes"         — list[RecipeResult]
                      One entry per recipe evaluated (coverage + random).
                      Fields: recipe_id, features, families, score_mean,
                      score_std, n_features, n_rows_used, error.
                      score_std is the std of R² across the cv_folds folds.
                      A high score_std relative to score_mean indicates an
                      unstable recipe — its performance depends heavily on
                      which rows land in which fold.
                      error is non-None if that recipe raised an exception.
                      Recipes with NaN score_mean are retained for diagnostics
                      but excluded from all aggregations.

  "column_scores"   — pd.DataFrame, one row per feature that appeared in at
                      least one non-NaN recipe:
                        feature      — DP variable code (e.g. "DP03_0062E")
                        mean_score   — mean R² across all recipes containing
                                       this feature (NaN recipes excluded)
                        count        — number of recipes this feature appeared in
                      Sorted by mean_score desc, then count desc.

  "family_scores"   — pd.DataFrame, one row per (top_family, subfamily) pair
                      that has at least one feature in column_scores:
                        top_family   — e.g. "DP03"
                        subfamily    — e.g. "INCOME AND BENEFITS (...)"
                        mean_score   — mean of member features' mean_scores
                        feature_count — number of features in this subfamily
                      Sorted by mean_score desc, then feature_count desc.

  "pairwise_scores" — pd.DataFrame, one row per feature pair (a, b) that
                      co-occurred in at least 5 non-NaN recipes:
                        feature_a, feature_b  — the pair (sorted lexicographically)
                        mean_score            — mean R² of recipes containing both
                        lift                  — mean_score minus average of the two
                                               features' individual mean_scores.
                                               Positive lift = genuine synergy.
                        co_occurrence_count   — number of recipes they shared
                      Sorted by lift desc, then co_occurrence_count desc.
                      Pairs with fewer than 5 co-occurrences are excluded
                      (insufficient data for a reliable lift estimate).


annotate_column_scores(column_scores, ff, dp_map, *, include_concept, top_n)
  Enriches the plain column_scores DataFrame with human-readable labels and
  family paths. Returns a new DataFrame with columns:
    feature, label, top_family, subfamily, inner_subfamily, mean_score, count
  dp_map must be a dp_mapping dict from load_dp_mapping() or
  metadata_to_dp_mapping(). In the updated pipeline this should come from
  Phase 0 get_all_metadata() via metadata_to_dp_mapping(), NOT from the
  static dp_mapping.json file.


RECIPECONFIG PARAMETERS
------------------------
  n_recipes       int   = 200   Target total recipe count (coverage + random).
                                If coverage alone exceeds this, random phase
                                is skipped. Actual count may exceed n_recipes
                                when coverage_rounds * ceil(n_cols / max_features)
                                > n_recipes.

  min_families    int   = 1     Minimum top-level families per random recipe.
  max_families    int   = 5     Maximum top-level families per random recipe.
                                Range is [1, 5] by default. The godot_friendly_api
                                currently overrides this to max_families=3 to
                                keep recipes more focused.

  min_features    int   = 20    Minimum features per random recipe.
  max_features    int   = 60    Maximum features per random recipe AND chunk
                                size for coverage recipes. At n=51 state rows,
                                keep max_features well below n to avoid the
                                p >> n problem even within a single recipe.
                                The godot_friendly_api overrides to max_features=30.

  cv_folds        int   = 3     Number of folds for cross_val_score inside
                                _evaluate_recipe(). 3 is the recommended value
                                at n=51: gives train sizes of ~34, which is the
                                regime CatBoost is designed for. 5-fold adds
                                only ~7 rows per fold and costs 67% more compute.
                                Do not set below 3.

  random_state    int   = 42    Seed for all RNG operations. Set to None for
                                non-deterministic runs.

  coverage_rounds int   = 5     Number of shuffle-and-chunk passes over the
                                full feature list. More rounds = more recipes,
                                better per-feature score estimates, slower run.
                                The godot_friendly_api overrides to 1.


SCORING INTERPRETATION
-----------------------
Column mean_score is the average across recipes of the mean 3-fold R² for that
feature. This score reflects how well that feature tends to contribute to
predictive models when combined with different partners — not how well it
predicts y in isolation (that is Phase 1's job).

Because score_std is now meaningful, the recommended ranking criterion is
mean_score - score_std rather than mean_score alone. This conservatively
deprioritises features that only score well on lucky fold splits. The
column_scores DataFrame is sorted by mean_score for simplicity, but callers
that want the conservative ranking should compute the adjusted criterion
from the available columns.

Important caveats:
  - R² can be negative (model worse than predicting the mean). Negative scores
    are included in averages. A column with consistently negative mean_score
    is actively harmful in combination and should be treated as noise.
  - R² at n=38 (75% of 51 states after NaN dropping) is extremely noisy.
    The ranking is meaningful in aggregate; individual column scores are not.
  - mean_score is sensitive to which other features a column was paired with.
    Coverage recipes mitigate this by ensuring diverse pairing, but some
    variance remains.
  - Family scores are more stable than column scores because they average
    across all member features. Prefer family scores for high-level
    interpretation; use column scores only for identifying the best
    representatives within a family.


PAIRWISE LIFT INTERPRETATION
------------------------------
lift = pair_mean_score − (mean_score_a + mean_score_b) / 2

A lift near 0 means the pair performs no better together than expected from
their individual scores. Positive lift indicates synergy — these two features
explain variance that neither captures alone. Negative lift means including
both hurts (likely collinearity or redundancy).

Minimum co_occurrence_count is 5. Below this threshold, the lift estimate is
too noisy to trust. With n_recipes=200 and typical feature set sizes (~80
features), most pairs will co-occur in 5–15 recipes under the default
coverage_rounds=5 + random sampling budget.


INTEGRATION STATUS
------------------
recipe_sampler.py itself is pipeline-ready. It accepts filtered_ff from Phase 1
and has no dependency on static JSON files.

The following files still use the OLD pipeline and must be updated:

  godot_friendly_api.py                                 STATUS: NEEDS REWIRE
    Current issues:
      - Calls FeatureFamilies.from_json("feature_families.json") — must switch
        to filtered_ff from phase1_screening output.
      - Calls load_dp_mapping("dp_mapping.json") — must switch to
        metadata_to_dp_mapping(metadata) from Phase 0 get_all_metadata().
      - Calls build_dataset() from Training.py which calls PhaseB_1 without
        metadata or year parameters — must pass metadata explicitly.
      - Does not call phase1_screening at all — must insert Phase 1 between
        Dataset_Builder and recipe_sampler calls.
    After rewire, the /analyze endpoint flow should be:
      make_user_target_df() → get_all_metadata() → PhaseB_1(metadata=...) →
      generate_feature_families(metadata) → run_screening() →
      run_recipe_sampling(filtered_X, y, filtered_ff, cfg) →
      annotate_column_scores(..., dp_map from metadata)

  Training.py                                           STATUS: NEEDS REWIRE
    Current issues:
      - build_dataset() calls PhaseB_1 without metadata/year parameters.
      - Uses get_cancerIncidence_df() (old loader) instead of CancerLoader.
      - Does not call phase1_screening — trains directly on raw ~528 columns.
    After rewire, Training.py should be a CLI harness for full pipeline runs
    (Phase 0 → Phase 1 → Phase 2 → final ElasticNet), not a shortcut to
    raw dataset training. Its train_elasticnet() function (which uses proper
    train/test split and K-fold CV) is the correct final model evaluation
    approach, distinct from the fast single-split scoring in _evaluate_recipe().


DATA FLOW THROUGH PHASE 2
--------------------------

  [Phase 1 output]
    passing_columns  — list[str], ~50-150 E-suffix variable codes
    filtered_ff      — FeatureFamilies, keyed to passing_columns only
    X (filtered)     — pd.DataFrame, n_rows × len(passing_columns)
    y                — pd.Series, n_rows
          ↓
  run_recipe_sampling(X, y, filtered_ff, cfg)
          ↓
  _build_family_to_columns(X.columns, filtered_ff)
    → fam_to_cols: {top_family: [col, ...]}
          ↓
  _filter_constant_columns(X, cols) per family
    → removes all-NaN and zero-variance columns
          ↓
  Coverage phase: _build_coverage_recipes(fam_to_cols, cfg, rng)
    → list of feature lists, each ≤ max_features long
          ↓
  Random phase: _sample_recipe_features(rng, fam_to_cols, cfg) × remaining budget
    → additional feature lists, family-stratified
          ↓
  _evaluate_recipe(X, y, feature_list, cfg) for each recipe
    → (score_mean, score_std=0.0, n_rows_used)
          ↓
  Aggregation:
    col_stats → column_scores DataFrame
    fam_agg   → family_scores DataFrame
    pair_stats → pairwise_scores DataFrame (min 5 co-occurrences)
          ↓
  annotate_column_scores(column_scores, filtered_ff, dp_map)
    → adds label, top_family, subfamily, inner_subfamily
          ↓
  [godot_friendly_api returns JSON to Godot]
    column_score_average, annotated_results, family_scores, pairwise_scores


WHAT "FULLY WORKING" LOOKS LIKE FOR PHASE 2
--------------------------------------------
Phase 2 is considered fully working when ALL of the following are true:

1. RECIPE GENERATION
   - Coverage phase guarantees every passing column appears in ≥ 1 recipe.
   - With coverage_rounds=5, every passing column appears in ~5 coverage recipes
     before random phase begins.
   - Random phase produces family-stratified recipes within configured bounds.
   - No recipe contains duplicate columns.
   - Recipes with < 10 clean rows return NaN score and are retained in
     results["recipes"] with error=None (NaN is not an error; it is a valid
     result for small-N situations after NaN row dropping).

2. EVALUATION CORRECTNESS
   - _evaluate_recipe runs 3-fold CV with CatBoostRegressor (cfg.cv_folds=3).
   - score_mean is the mean R² across folds (possibly negative).
   - score_std is the std of R² across folds (non-zero, meaningful).
   - n_rows_used reflects actual clean rows after NaN dropping, not original n.

3. COLUMN SCORES
   - column_scores contains one row per feature that appeared in ≥ 1 non-NaN recipe.
   - mean_score is the unweighted mean R² across all appearances.
   - count reflects true appearance count.
   - Sorted by mean_score desc, then count desc.
   - Features that only appeared in NaN-scoring recipes are ABSENT (not zero).

4. FAMILY SCORES
   - family_scores contains one row per (top_family, subfamily) pair.
   - mean_score is the mean of member features' mean_scores (not a re-average
     of raw recipe scores).
   - Only subfamilies with at least one feature in column_scores are present.

5. PAIRWISE SCORES
   - pairwise_scores contains only pairs with co_occurrence_count ≥ 5.
   - lift is correctly computed as pair_mean − average(individual means).
   - Keys (feature_a, feature_b) are lexicographically sorted (no duplicate
     pairs in reverse order).

6. ANNOTATION
   - annotate_column_scores returns a DataFrame with all required columns.
   - Features absent from ff.col_index have None for family fields (not an error).
   - Sorting by mean_score desc, count desc is preserved after annotation.

7. INTEGRATION (STATUS: TO DO — matches Phase 1 item 4)
   - godot_friendly_api /analyze passes filtered_ff from run_screening() into
     run_recipe_sampling() rather than loading feature_families.json.
   - annotate_column_scores receives dp_map from metadata_to_dp_mapping(metadata)
     rather than loading dp_mapping.json.
   - Full pipeline runs end-to-end for all four demonstration datasets.


KNOWN RISKS AND MITIGATIONS
----------------------------
1. NEGATIVE R² SCORES
   At n=34 (train size with 3-fold CV at state level), CatBoost with
   iterations=200 may overfit or underfit on some folds, producing negative R²
   on those folds. This is expected and acceptable — negative mean_score values
   indicate the feature combination is actively unhelpful in that recipe context.
   They are included in column averages and deprioritised automatically by sort
   order. A high score_std accompanying a negative mean_score is the clearest
   signal to discard a feature.

2. COVERAGE RECIPE COUNT EXPLOSION
   With 100 passing columns and max_features=30, one coverage round produces
   ceil(100/30) = 4 recipes. With coverage_rounds=5, that is 20 coverage
   recipes. This is well within the n_recipes=200 budget. However, if Phase 1
   passes 500+ columns (e.g. if min_abs_r threshold is lowered), coverage
   explodes: 500/30 × 5 = 84 coverage recipes, which consumes the budget fast.
   Monitor passing_columns count from Phase 1 — if it exceeds ~150, consider
   increasing min_abs_r or reducing coverage_rounds.

3. PAIRWISE SPARSITY
   With n_recipes=200 and 100 features, the expected co-occurrence count for
   any given pair is approximately 200 × (max_features/100)² ≈ small. Pairs
   that never reach 5 co-occurrences are silently dropped from pairwise_scores.
   Increasing n_recipes or coverage_rounds improves pairwise coverage but at
   linear cost in compute time.

4. SCORE NOISE AT COUNTY LEVEL
   At county level within one state (n~58 for California), 3-fold CV gives
   train sizes of ~38 rows — slightly better than state level. CatBoost handles
   this regime well, but absolute R² values are still unreliable. Recipe scores
   are valid for ranking purposes only. Do not interpret county-level recipe
   scores as model performance estimates.

5. GODOT API LATENCY
   CatBoost with 3-fold CV is slower per recipe than the previous single-split
   ElasticNet approach. With n_recipes=200 and coverage_rounds=1 (the current
   godot_friendly_api settings), a typical run is expected to take longer than
   the old implementation — exact timing should be benchmarked once the rewrite
   is complete. The conservative API settings (coverage_rounds=1, max_features=30,
   n_recipes=200) are retained to limit the recipe count. If latency is
   unacceptable in practice, reduce n_recipes before reducing cv_folds — the
   stability benefit of 3-fold outweighs the cost of fewer recipes.
   
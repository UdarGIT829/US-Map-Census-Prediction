PHASE 1 — Feature Screening & Dataset Assembly
===============================================
Last updated: 2026-04-13


OVERVIEW
--------
Phase 1 sits between the data server (Phase 0) and the recipe sampler (Phase 2).
It has two responsibilities:

  1. DATASET ASSEMBLY — Pull ACS data from Phase 0, wire in the user's target
     variable (y), filter to analysis-ready columns (E-suffix only), and
     produce a clean (X, y) pair.

  2. UNIVARIATE SCREENING — Before any ML runs, compute Pearson and Spearman
     correlations of every column against y, apply Benjamini-Hochberg FDR
     correction, and produce a ranked shortlist of columns and families.
     This shortlist is what Phase 2 (recipe sampling) operates on.

Phase 1 does NOT train any models. It is purely statistical screening and
data preparation.


WHY PHASE 1 EXISTS
------------------
The feature matrix X has approximately 528 E-suffix columns per geography/year
(after filtering out PE, margins, annotations, and all-NaN columns).
With only 51 rows (one per US state) or ~58 rows (counties within one state),
training any model on all 528 columns simultaneously is hopeless — this is the
p >> n problem.

ElasticNet or any regularized model will find spurious fits. The recipe sampler
(Phase 2) was designed to work around this by sampling subsets, but it can only
produce meaningful signal if it starts from a pre-filtered set where the noise
columns have already been removed.

Phase 1 solves this by doing simple, honest univariate statistics first:
  - Pearson r: measures linear association
  - Spearman ρ: measures monotonic association (robust to skew, which ACS data has)
  - p-values: assess whether each correlation could be chance
  - BH-FDR correction: controls false discovery rate across ~500 simultaneous tests
    At 528 columns, you expect ~26 false positives at p=0.05 by chance alone.
    BH-FDR at q=0.1 is the standard correction for wide screening like this.

After screening, Phase 2 receives a much smaller, more reliable feature set.


NOTE ON E-ONLY FILTERING
------------------------
Dataset_Provider filters X to E-suffix columns only (raw count estimates),
not E + PE (percent estimates). This was a deliberate simplification:
  - PE columns are derived from E columns (PE = E / total * 100), so including
    both would introduce near-perfect collinearity between paired variables
  - Phase 1 screening is already operating at n=51 where collinearity is a
    serious concern even without the PE redundancy
  - The recipe sampler downstream also benefits from a less redundant input

If percent-form variables are needed for a specific analysis, this can be
revisited by changing is_dp_estimate() to is_dp_analysis_var() in
Dataset_Provider.py — the infrastructure already supports it.


THE N=51 PROBLEM
----------------
When operating at state level, X has ~51 rows (50 states + DC).
The exact count after joining with a user target depends on coverage:
  - All-invasive cancer incidence 2022 has all 51 states → inner join gives 51
  - Opioid death data has 48–51 states depending on year (CDC suppresses
    states with < 10 deaths; rare for opioids but possible for early years)
  - Rare cancer sites may have fewer states → inner join gives fewer rows
  - Corn (NASS SURVEY) has ~37–41 states — non-producing states are simply
    absent from the data, not suppressed. Set min_states=30 for corn.
  - UFO sightings cover all 51 states for years 2009–2016 but the data
    tops out at 2017 (partial year only) — ACS vintages 2013–2016 are the
    supported range.
  - Puerto Rico (FIPS 72) is present in ACS but absent from all four
    demonstration datasets, so it drops out of every inner join

This means:
  - Univariate correlation is fine — you can correlate any single column with y
    using n=51 and get a meaningful p-value
  - Multi-column models are not fine — p >> n with 528 columns and 51 rows
  - The recipe sampler is valid AFTER Phase 1 reduces the column count to ~50-100

At county level within a single state (e.g. California has 58 counties), n is
even smaller. The same screening approach applies but thresholds may need to
be looser (p < 0.2 rather than p < 0.1) given the smaller sample.


FILE INVENTORY — CURRENT STATE
-------------------------------

All rewiring is complete. The static dp_mapping.json dependency has been
removed from the entire pipeline. The files below reflect the current state.

  dp_labels.py                                               STATUS: DONE
    Provides: split_dp_var, is_dp_estimate, is_dp_percent,
    is_dp_analysis_var, is_dp_margin, metadata_to_dp_mapping,
    human_label, make_readable_view.
    metadata_to_dp_mapping() converts Phase 0 get_all_metadata() output
    into the dp_map dict shape expected by human_label().
    load_dp_mapping() is kept as a dev/CLI utility only — it must not
    be called in the production pipeline.
    metadata=None now raises ValueError immediately in all production paths.

  Dataset_Provider.py                                        STATUS: DONE
    Reads ACS data from DuckDB and returns {X, y, meta}.
    Accepts metadata (Phase 0 get_all_metadata output) and year as
    explicit parameters — both are stored in dataset["meta"] and
    propagated through the pipeline.
    Filters X to E-suffix columns only, coerces to numeric, drops all-NaN.
    Year-aware WHERE clause: if a year column exists in the DuckDB table
    and the requested year is found in it, filters to that year.
    NOTE: Dataset_Provider still reads from DuckDB directly rather than
    calling acs_loader functions. This is acceptable — the DuckDB files
    are the authoritative local cache and the acs_loader rewire is
    deferred. The metadata dict is provided by the caller (from Phase 0)
    so vintage-matching is correct regardless of the read path.

  Dataset_Builder.py                                         STATUS: DONE
    Orchestrates dataset construction. Two paths:
      PhaseB_1 — user has provided a target variable (e.g. cancer incidence)
      PhaseB_2 — no user data; uses an internal ACS column as the target
    Both paths require metadata from Phase 0 get_all_metadata() — passing
    metadata=None raises ValueError immediately. There is no fallback.
    PhaseB_1 infers target_col from the user-supplied DataFrame automatically
    (whichever column is not the join key), so any column name works.
    make_stub_user_target_half_dp() is kept for development use only and
    is clearly marked TEST-ONLY. It is not called in production.

  generate_feature_families.py                               STATUS: DONE
    Builds the 3-level family hierarchy from Phase 0 metadata.
    generate_feature_families(metadata) is the pipeline-facing entry point.
    It requires a metadata dict — passing None raises ValueError.
    build_feature_family_tree() retains a dp_mapping_path fallback for the
    CLI dev tool only (main()), not reachable from the pipeline.
    Must be called per-year since labels change between vintages.

  feature_families.py                                        STATUS: DONE
    The FeatureFamilies class and FamilyPath dataclass. No changes needed
    since the big pipeline rewrite — the interface is already correct.

    Constructor: FeatureFamilies(tree) — takes the tree dict directly,
    which is exactly what generate_feature_families(metadata) returns.
    No file I/O required in the production pipeline. from_json() exists
    as a classmethod but is not called anywhere in the pipeline.

    Key interface used by phase1_screening.py and Phase 2:
      ff.col_index          — dict[str, FamilyPath], reverse index of all
                              columns in the tree. Tests assert
                              filtered_ff.col_index.keys() == passing_columns
                              exactly (no extras, no gaps).
      ff.path_for_column()  — FamilyPath lookup for a single column
      ff.columns_in_subfamily() — all columns for a (top_family, subfamily)
      ff.filter_known_columns() — subset a column list to tree members
      ff.families() / ff.subfamilies() / ff.inner_subfamilies() — iterators

    CRITICAL — filtered_ff construction: phase1_screening.py must build
    filtered_ff from a PRUNED tree containing only passing_columns, not
    from the full tree. The tests enforce that col_index.keys() equals
    passing_columns exactly. The correct approach is to prune the full
    tree dict to only include passing columns before passing it to
    FeatureFamilies() — do not pass the full tree and expect col_index
    to self-filter. generate_feature_families() returns the full tree;
    phase1_screening.py is responsible for the pruning step.

  base_loader.py                                             STATUS: DONE
    Abstract base class for all Phase 1 target variable loaders.
    Subclasses must define ACS_PAIRINGS and ACS_CAUTION_VINTAGES class
    attributes and implement load(). The base class provides:
      _resolve_year()       — acs_year/year disambiguation with pairing table
      _resolve_window()     — compute list of years in an aggregation window
      _aggregate_window()   — aggregate per-year DataFrames via mean or sum,
                              with inner-join semantics (states missing in any
                              window year are dropped with a warning)
      _validate_output()    — enforces the two-column contract on every load()
      _enforce_min_states() — raises if row count falls below min_states
      _check_csv_exists()   — raises FileNotFoundError with a clear message
      last_load_info        — populated after every successful load(); passed
                              to run_screening() as loader_info for traceability
    target_col is always required and has no default — the caller (or
    front-end) must supply it explicitly. Nothing in the pipeline is
    hardcoded to any specific column name.

  cancer_loader.py                                           STATUS: DONE
    CancerLoader(BaseTargetLoader) — loads CDC USCS cancer incidence CSV.
    window parameter: window=1 (default, single year) or window=5 (sums
    counts across the 5-year window ending at the resolved cancer year,
    matching the ACS 5-year survey scope). Counts are summed — 5-year
    total incidence is a well-defined epidemiological quantity.
    Backward-compatible module-level shims kept for test_phase1.py:
      load_cancer_target(), list_cancer_sites(), available_years()
    New code should use CancerLoader directly.
    See INPUT DATA section for full source and metric details.

  opioid_loader.py                                           STATUS: DONE
    OpioidLoader(BaseTargetLoader) — loads CDC WONDER opioid death data.
    Three metrics: age_adjusted_rate (default), crude_rate, deaths.
    window parameter: window=1 (default) or window=5. Rates are averaged
    across years (mean); death counts are summed. States suppressed in
    ANY window year are excluded from the output.
    Suppressed states (CDC count < 10) are dropped with a warning;
    n_suppressed and suppressed_states recorded in last_load_info.
    See INPUT DATA section for full source and metric details.

  corn_loader.py                                             STATUS: DONE
    CornLoader(BaseTargetLoader) — loads USDA NASS crop production data.
    Used for any NASS crop CSV produced by fetch_crop_nass.py, not just
    corn — the class name reflects its origin but the implementation is
    general. CSV column "value_numeric" is the pre-parsed numeric target.
    min_states defaults to 30 (corn covers ~37–41 states; the base class
    default of 45 would always fail for crops).
    See INPUT DATA section for full source and metric details.

  ufo_loader.py                                              STATUS: DONE
    UFOLoader(BaseTargetLoader) — loads NUFORC UFO sighting data.
    Source CSV: nuforc_events_with_state_code.csv (individual sighting
    records with State_Code FIPS, 97k rows, 1930s–2017).
    ACS vintages supported: 2013–2016 only. 2017 data is a partial year
    (~1,200 sightings vs ~5–8k for full years) and is excluded.
    Aggregation: sightings are summed over the full 5-year window matching
    the ACS vintage (e.g. ACS 2016 → sightings 2012–2016). This is the
    correct approach because single-year UFO counts are noisy (driven by
    reporting volume, not real-world variation).
    Metrics: "sightings_per_100k" (requires population Series, recommended)
             or "raw_count" (for diagnostic use / dry run).
    See INPUT DATA section for full source and metric details.

  phase1_screening.py                                        STATUS: DONE
    The core Phase 1 module. run_screening(X, y, metadata, config, loader_info)
    returns column_stats, family_stats, passing_columns, filtered_ff,
    and loader_info. The loader_info parameter is optional — it is stamped
    onto the result dict as-is for traceability and does not affect any
    screening logic. See PHASE 1 MODULE SPECIFICATION below.

Files that are now redundant and can be deleted:

  dp_mapping.json
    Was a static 2023-only snapshot of ACS variable metadata. Now replaced
    by Phase 0's get_all_metadata(year, groups) KV cache. Safe to delete.

  feature_families.json
    Was generated from dp_mapping.json. Now regenerated in-memory from live
    Phase 0 metadata each run. Safe to delete.

  build_dp_mapping.py
    Fetched DP metadata and wrote dp_mapping.json. Fully superseded by
    Phase 0's get_all_metadata(). Safe to delete.

  populate_phase0_db.py
    Superseded by the populate_all_geographies() section added to
    test_phase0.py. Safe to delete.


INPUT DATA
----------
Phase 1 is a general-purpose pipeline. The target variable (y) can be any
state-level numeric outcome supplied as a two-column DataFrame with columns
[state_fips, value]. The pipeline does not care what the target represents —
the same screening logic applies regardless of domain.

Four target datasets are used in the project to demonstrate generalisability:

TARGET LOADERS
--------------
All loaders live in phase_1/input/ and subclass BaseTargetLoader. Each
produces a DataFrame with columns ["state", <target_col>] where "state" is
a zero-padded 2-digit FIPS string and target_col is supplied by the caller.
All are compatible with PhaseB_1(user_target_df=...) without any pipeline
changes. After a successful load(), loader.last_load_info contains a dict
describing what was loaded; pass it to run_screening() as loader_info.

  CancerLoader  (cancer_loader.py)
    Source: CDC United States Cancer Statistics (USCS) program
    File:   phase_1/input/United States and Puerto Rico Cancer Statistics,
            1999-2022 Incidence.csv
    Metric: Raw incidence count per state (metric="count", only option).
            Population-normalised rates must be derived post-join by dividing
            by ACS DP05_0001E. See KNOWN RISKS item 5.
    Coverage: 50 states + DC; 104 cancer site types; years 1999–2022
    ACS pairing: ACS 2023 → cancer year 2022 (no 2023 publication yet).
                 All other vintages pair 1:1.
    Notes:  - FIPS codes are NOT zero-padded in the source; loader zero-pads
            - No suppression in this export (all cells are positive integers)
            - Use CancerLoader.list_sites() and .available_years() to enumerate
    Default: cancer_site="All Invasive Cancer Sites Combined"

  OpioidLoader  (opioid_loader.py)
    Source: CDC WONDER Multiple Cause of Death database
    File:   phase_1/input/opioid_deaths_2018_2023.csv
    Metrics: age_adjusted_rate (default) — deaths per 100k, age-adjusted.
                 RECOMMENDED. Controls for differing age structures between
                 states, removing the primary confound with ACS DP05 variables.
             crude_rate — deaths per 100k, unadjusted.
             deaths — raw death count. Strongly correlated with population
                 size; large-state ACS variables will score highly for the
                 wrong reasons. Use only if that is intentional.
    Coverage: 50 states + DC; years 2018–2023
    ACS pairing: Direct 1:1 for all vintages 2018–2023. Unlike cancer,
                 2023 opioid data exists — no fallback needed.
    Notes:  - CDC suppresses cells with < 10 deaths; suppressed states are
              dropped with a warning and recorded in last_load_info
            - Strong, specific ACS correlates (disability, unemployment,
              rural housing, low education) — used to validate that the
              pipeline finds known socioeconomic predictors

  CornLoader  (corn_loader.py)                               STATUS: DONE
    Source: USDA National Agricultural Statistics Service (NASS) Quick Stats API
    Fetch:  fetch_crop_nass.py --commodity "CORN" (or any other crop)
    File:   phase_1/input/corn_production_by_state.csv (or <slug>_by_state.csv)
    Metric: value_numeric — the numeric value for the selected data item.
            For CORN GRAIN PRODUCTION the unit is BU (bushels).
            For CORN GRAIN YIELD the unit is BU/ACRE.
    Coverage: ~37–41 states depending on year; non-producing states are
              absent (not suppressed). min_states=30 recommended.
    ACS pairing: Direct 1:1 for all vintages 2018–2023.
    Notes:  - fetch_crop_nass.py is a general crop fetcher — not corn-specific.
              Supports --batch mode to pull all labor-intensive crops at once,
              with a state-count confirmation prompt before each save.
            - CornLoader is also used for all other NASS crops (cotton, wheat,
              tobacco, etc.) — pass the appropriate CSV via csv_path.
            - Crop choice matters enormously for which ACS families surface:
              * CORN YIELD (BU/ACRE): rurality proxies, Czech/Nordic ancestry
                (Corn Belt demographics), housing occupancy — driven by the
                white owner-operator profile of mechanised Midwest farming.
              * COTTON (LB): Hispanic/Latino workforce, low income, disability
                — driven by the farm labor demographic of the Deep South.
              * Choosing the metric and crop is a research decision, not a
                pipeline decision. Both results are honest.
            - Raw production (total BU) is dominated by state size — use yield
              (BU/ACRE) or harvested area for cross-state comparisons.

  UFOLoader  (ufo_loader.py)                                 STATUS: DONE
    Source: National UFO Reporting Center (NUFORC) via Kaggle dataset
    File:   phase_1/input/nuforc_events_with_state_code.csv
            97,073 individual sighting records; columns include Year, State_Code
            (integer FIPS), City, Shape, Duration, Summary, Event_URL.
    Metric: "sightings_per_100k" (recommended) — 5-year aggregate sighting
            count divided by ACS DP05_0001E (total population). Population
            Series must be passed at construction time.
            "raw_count" — 5-year aggregate only; use for dry runs where
            population data is not yet available.
    Coverage: All 51 states for 2009–2016. 2017 is partial year and excluded.
    ACS pairing: 2013–2016 only (5-year windows: 2013→2009–2013, etc.)
    Notes:  - Included as a novelty/stress-test target. There is no causal
              relationship between ACS demographics and UFO sightings.
            - Expected pipeline behaviour: surfaces reporting-bias correlates
              (internet access proxies, rural/urban split, education) rather
              than causal relationships. This is the correct result.
            - Absence of causal grounding is a feature, not a bug — it
              demonstrates the pipeline working honestly on an arbitrary
              numeric target and makes correlation ≠ causation visible.
            - Single-year sighting counts are noisy (NUFORC reporting volume
              varies by year due to media attention and database growth).
              The 5-year window aggregation in UFOLoader is mandatory, not
              optional, for this reason.

CONTRACT FOR ALL LOADERS
-------------------------
Every loader must return a pd.DataFrame with exactly two columns:
  "state"      — zero-padded 2-digit FIPS string ("01" through "56", no "72")
  <target_col> — float64, the outcome value for that state; name supplied by
                 the caller, never hardcoded in the loader itself
Index is the default integer index. No NaN values in target_col (rows with
missing data are dropped inside the loader, not passed to the pipeline).
Puerto Rico (FIPS "72") must be absent — it is not in the ACS state DB.
This contract is enforced by BaseTargetLoader._validate_output() on every
load() call, so violations raise immediately rather than propagating silently.

When window > 1, the output still satisfies this same two-column contract —
the aggregation is internal to the loader and invisible to the pipeline.
States missing in any window year are dropped before output (inner join
semantics), so the returned DataFrame is always complete and consistent.


DATA FLOW THROUGH THE FULL PIPELINE
------------------------------------

  [User uploads target data to Godot frontend]
          ↓
  godot_friendly_api.py receives POST /analyze
          ↓
  make_user_target_df() cleans and normalises user target → user_target_df
  OR one of the target loaders (CancerLoader, OpioidLoader,
     CornLoader, UFOLoader) for bundled demonstration datasets.
  In both cases target_col name is supplied by the caller.
          ↓
  metadata = acs_loader.get_all_metadata(year, groups)  ← vintage-matched
          ↓
  Dataset_Builder.PhaseB_1(metadata=metadata, user_target_df=user_target_df)
    → calls Dataset_Provider.dataset_provider()
    → reads from DuckDB (populated by test_phase0.py / Phase 0 API)
    → infers target_col from user_target_df automatically
    → returns {X, y, meta}  where meta["acs_metadata"] = metadata
          ↓
  generate_feature_families(metadata)
    → regenerates family tree from current year metadata (in-memory, fast)
          ↓
  phase1_screening.run_screening(X, y, metadata, loader_info=loader.last_load_info)
    → returns column_stats, family_stats, passing_columns, filtered_ff,
      loader_info (stamped through for traceability, not used in logic)
          ↓
  recipe_sampler.run_recipe_sampling(filtered_X, y, filtered_ff, cfg)
    → returns column_scores, family_scores, pairwise_scores
          ↓
  annotate_column_scores() attaches human-readable labels
          ↓
  godot_friendly_api returns JSON response to Godot


PHASE 1 MODULE SPECIFICATION — phase1_screening.py
---------------------------------------------------

Input:
  X         — pd.DataFrame, rows=geographies, columns=DP variable codes
               Filtered to E-suffix columns only (done by Dataset_Provider).
               Values are numeric (float/NaN).
  y         — pd.Series, same index as X, the target variable
  metadata  — dict {varcode: {label, concept, group}} from get_all_metadata()
               Used for labelling output only, not for filtering.
  config    — ScreeningConfig dataclass (see below)

Output dict:
  "column_stats"    — pd.DataFrame, one row per column, sorted by |r| desc:
                        feature, label, group,
                        pearson_r, pearson_p, pearson_p_adj,
                        spearman_r, spearman_p, spearman_p_adj,
                        abs_r (max of |pearson_r|, |spearman_r|),
                        passes_fdr (bool)
  "family_stats"    — pd.DataFrame, one row per (top_family, subfamily):
                        top_family, subfamily,
                        mean_abs_r, max_abs_r, n_significant, n_total
  "passing_columns" — list[str], variable codes where passes_fdr=True
  "filtered_ff"     — FeatureFamilies built from passing_columns only
  "loader_info"     — dict from BaseTargetLoader.last_load_info, or None.
                      Stamped through as-is; never affects screening logic.
                      Useful for audit trails and paper reproducibility.

ScreeningConfig dataclass:
  fdr_threshold     float = 0.1    # BH-FDR q threshold
  min_abs_r         float = 0.3    # minimum |r| regardless of FDR result.
                                   # Primary lever for column count control.
                                   # At n=51 with raw count targets, BH-FDR
                                   # alone passes ~90% of columns due to
                                   # population-size confounds. The 0.3 floor
                                   # is the practical guard against this.
  use_pearson       bool  = True   # include Pearson in abs_r calculation
  use_spearman      bool  = True   # include Spearman in abs_r calculation
  min_nonnan_frac   float = 0.8    # drop columns with >20% NaN before testing


BH-FDR CORRECTION — HOW IT WORKS
----------------------------------
Benjamini-Hochberg controls the False Discovery Rate — the expected proportion
of significant results that are false positives.

Algorithm (applied separately to Pearson and Spearman p-values):
  1. Sort p-values p(1) ≤ p(2) ≤ ... ≤ p(m)
  2. Find largest k where p(k) ≤ (k/m) * q
  3. Reject all hypotheses i ≤ k (declare significant)

A column "passes_fdr" if EITHER its BH-adjusted Pearson p OR its BH-adjusted
Spearman p is below fdr_threshold. Using both gives more power than either alone
because Pearson catches linear relationships and Spearman catches monotonic ones.

Implementation: scipy.stats.pearsonr, scipy.stats.spearmanr for raw stats.
statsmodels.stats.multitest.multipletests(method='fdr_bh') for correction.
Both are standard scientific Python libraries.


WHAT PHASE 1 DOES NOT DO
-------------------------
- It does not train any models
- It does not guarantee the passing columns are causally related to y —
  correlation is not causation, especially with ACS data where many variables
  are highly collinear (income, education, and poverty are all correlated
  with each other and with almost everything else)
- It does not handle the collinearity problem — that is deferred to Phase 2
  (the recipe sampler explores combinations, not individual columns)
- It does not support multiple target variables simultaneously
- It does not support longitudinal/delta analysis (see Phase_0.md)
- It does not write any files or have side effects
- It does not call Phase 0 APIs — it receives metadata from the caller


KNOWN RISKS AND MITIGATIONS
----------------------------
1. COLLINEARITY
   ACS variables are massively collinear. Many DP03 income variables are
   near-linear combinations of each other. Univariate screening will flag
   all of them as significant, not just the "best" one. This is expected and
   acceptable — Phase 2's recipe sampler handles collinearity implicitly by
   measuring what families produce better scores when combined.

2. ECOLOGICAL FALLACY
   All analysis is at the geography level (state or county), not the individual
   level. A finding that "% college educated correlates with cancer incidence
   by state" does NOT mean educated individuals are more likely to get cancer.
   This is a fundamental limitation of ecological data and must be stated
   clearly in any write-up.

3. MULTIPLE COMPARISONS
   Even with BH-FDR at q=0.1, with ~528 E-suffix columns you expect ~53
   significant results by chance. The FDR threshold means 10% of your
   "significant" columns are expected false positives. This is acceptable for
   screening (you want recall, not precision) but must not be confused with
   confirmed findings.

4. SMALL N
   At state level, n=51. Pearson r needs roughly n=30 for its distributional
   assumptions to hold. n=51 is marginal. Spearman is more robust. Both should
   always be reported together.

5. COUNT VS RATE
   The cancer CSV provides raw counts, not age-adjusted rates. Raw counts
   strongly correlate with population size, which means large-population ACS
   variables (total households, total workers, etc.) may score highly in
   screening purely because large states have both more people and more cancer
   cases. The opioid loader defaults to age_adjusted_rate precisely to avoid
   this confound. The crop loader uses total production or yield depending on
   the data item selected — yield (BU/ACRE) is the recommended metric for
   cross-state crop comparisons as it removes the state-size confound.

6. CROP COVERAGE AND METRIC CHOICE
   Not all 51 states produce meaningful quantities of any given crop. States
   with no recorded production will be absent from the inner join, reducing n
   below 51. Set min_states=30 when using CornLoader. The reduced n makes the
   small-N problem (item 4) more acute.
   More importantly, crop metric choice drives which ACS families surface:
   raw production (BU) surfaces state-size correlates; yield (BU/ACRE)
   surfaces farming efficiency correlates; harvested area surfaces land-use
   correlates. This is a research design decision that must be documented.

7. UFO ACS VINTAGE MISMATCH
   The NUFORC dataset tops out at 2017 (partial year). This restricts UFO
   analysis to ACS vintages 2013–2016, which are pre-COVID and pre-2020
   racial/ethnic redesign. Results are not directly comparable to the 2023
   ACS vintage used for cancer and opioid analysis. When presenting UFO
   results alongside other datasets in the paper, note the vintage difference
   and its implications for any cross-dataset comparisons.

8. WINDOW AGGREGATION AND TEMPORAL MATCHING
   The ACS 5-year product pools five survey years. A single-year outcome
   target (window=1) is temporally mismatched — you are correlating a 5-year
   demographic average against a 1-year outcome snapshot. This is standard
   practice in epidemiology and defensible, but window=5 is the more correct
   approach: it sums or averages the outcome over the same 5-year window as
   the ACS survey, producing a temporally matched comparison.
   For cancer and opioid, window=5 is recommended for the paper's primary
   results. window=1 results can be reported as a robustness check.
   UFOLoader always uses a 5-year window internally (single-year sighting
   counts are too noisy to use). CornLoader does not support window > 1
   because NASS crop data is already a single-year annual estimate.


FEATURE FAMILIES TREE — STRUCTURE AND REGENERATION
----------------------------------------------------
The feature family tree organises all E-suffix DP variables into a
3-level hierarchy derived from their ACS label strings:

  TopFamily      = DP table (DP02, DP03, DP04, DP05)
  Subfamily      = First semantic chunk of the label after stripping
                   "Estimate!!", "Percent!!" etc.
                   Example: "INCOME AND BENEFITS (IN 2023 INFLATION-ADJUSTED DOLLARS)"
  InnerSubfamily = Second semantic chunk
                   Example: "Total households"

This tree is used by the recipe sampler to sample features by family rather
than randomly, ensuring coverage across conceptual groupings.

The tree is regenerated in-memory on every pipeline run from the Phase 0
metadata dict. It is never read from disk in production. This means:
  - Label changes between vintages automatically produce correct subfamilies
  - New variables added between vintages are automatically placed in the tree
  - Regeneration is fast (~50ms) and happens as part of pipeline setup


GODOT INTEGRATION
-----------------
godot_friendly_api.py is the HTTP endpoint that Godot calls. It currently
jumps directly from dataset build to recipe sampling. Phase 1 slots in
between:

  Before Phase 1:
    dataset build → recipe_sampler(all ~528 E columns)

  After Phase 1:
    dataset build → phase1_screening(~528 columns) → recipe_sampler(~50-150 columns)

The API response shape does not need to change — Godot receives the same
annotated column scores and family scores it always did. Phase 1 just makes
those scores more reliable by ensuring Phase 2 wasn't operating on noise.

Optionally, the Phase 1 column_stats DataFrame can be included in the API
response as a "screening_results" field, giving Godot visibility into which
columns passed/failed screening and their raw correlation values. This is
useful for debugging and for showing the user which demographic factors were
considered.


WHAT "FULLY WORKING" LOOKS LIKE FOR PHASE 1
--------------------------------------------
Phase 1 is considered fully working when ALL of the following are true:

1. DATASET ASSEMBLY                                          STATUS: DONE
   - metadata comes from Phase 0 get_all_metadata() — metadata=None raises
   - X contains only E-suffix columns, coerced to numeric, ~500-560 columns
   - y is correctly aligned to X's index
   - target_col is inferred from user_target_df automatically
   - Both state-level (n~51) and county-level (n~58 for CA) work
   - All 51 states present in DuckDB (populated by test_phase0.py)

2. FEATURE FAMILIES REGENERATION                            STATUS: DONE
   - generate_feature_families(metadata) works without file I/O
   - Output tree is consistent with the year being analysed
   - All E columns in X are present somewhere in the tree
   - Regeneration happens automatically, not manually

3. UNIVARIATE SCREENING                                     STATUS: DONE
   - phase1_screening produces column_stats with all required fields
   - BH-FDR correction is applied correctly
   - passing_columns is non-empty for all four demonstration targets
   - passing_columns is substantially smaller than the full column set
   - family_stats correctly aggregates per (top_family, subfamily)
   - NaN columns (constant-input) are dropped from column_stats output
   - Tested against all 73 cancer sites with >= 40 state coverage

4. INTEGRATION                                              STATUS: TO DO
   - Full pipeline runs end-to-end:
     Phase 0 metadata → Dataset_Builder → phase1_screening →
     FeatureFamilies → recipe_sampler → annotated results
   - godot_friendly_api /analyze endpoint returns valid results
   - All four demonstration datasets (cancer, opioid, corn/cotton, UFO)
     run end-to-end without pipeline changes

5. TEST HARNESS (test_phase1.py)                            STATUS: DONE
   - Sections 1-5 pass: dp_labels, generate_feature_families, cancer_loader,
     Dataset_Provider, Dataset_Builder
   - Multi-vintage test (Section 5b) passes for all 6 representative vintages:
     2009, 2015, 2019, 2020, 2021, 2023
   - Sections 6-7 (TestPhase1Screening, TestEndToEnd) pass
   - Section 8 (TestMultiSiteScreening) passes: all 73 eligible cancer sites
     screened successfully with full structural validation
   - populate_all_geographies() in test_phase0.py now runs with
     ALL_ACS_VINTAGES (2009-2023). The DB is fully warm.

6. LOADER TEST HARNESS (test_dataset_loaders.py)            STATUS: DONE
   - Section 1: BaseTargetLoader contract tests pass (pure, no I/O)
     Includes: two-column contract, FIPS padding, duplicate detection,
     year resolution, _validate_output rejection cases, last_load_info
     lifecycle, min_states enforcement, empty target_col rejection.
   - Section 2: CancerLoader unit and CSV tests pass
     Regression anchor: California all-sites 2022 count = 184,176
     Multi-vintage smoke test: 2009, 2015, 2019, 2021, 2022, 2023 all pass
   - Section 3: OpioidLoader unit and CSV tests pass
     Regression anchors: WV age-adjusted rate > 50 and ranks top 3;
     MN age-adjusted rate < 20; suppression count consistent with raw CSV
   - Section 4: Cross-loader integration tests pass — both loaders produce
     identical-contract output, survive PhaseB_1 unmodified, target_col
     name flows through to ds["y"].name, loader_info survives into screening
   - corn_loader and ufo_loader sections: TO DO (add when tests written)

7. DEMONSTRATION DATASETS                                   STATUS: DONE
   All four loaders implemented and tested via dry_run_iterative_screening.py.
   See DRY RUN RESULTS section below for observed ACS family signals.
   - CancerLoader:  DONE — window=1 and window=5 both working
   - OpioidLoader:  DONE — window=1 and window=5 both working
   - CornLoader:    DONE — used for all NASS crops via csv_path parameter
   - UFOLoader:     DONE — always uses 5-year window; ACS 2013–2016 only


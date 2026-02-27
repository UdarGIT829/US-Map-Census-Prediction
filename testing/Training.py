#!/usr/bin/env python3

"""
Training script for ACS-based models.

- Uses PhaseB_1 / PhaseB_2 from Dataset_Builder to build the dataset.
- Trains an ElasticNet regression model in a sklearn Pipeline.
- Prints basic metrics and saves the fitted model to disk.
"""

from Dataset_Builder import PhaseB_1, PhaseB_2

import argparse
import joblib
import numpy as np

from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import ElasticNet
from sklearn.metrics import mean_squared_error, r2_score


def build_dataset(has_user_data: bool, level: str, selected_state_fips: int,
                  dp_col: str, exclude_counties_fips=None, exclude_states_fips=None):
    """
    Wrapper around PhaseB_1 / PhaseB_2 so training code doesn't care
    about the details of dataset construction.
    """
    phase_kwargs = dict(
        level=level,
        selected_state_fips=selected_state_fips,
        dp_col=dp_col,
        exclude_counties_fips=exclude_counties_fips,
        exclude_states_fips=exclude_states_fips,
    )

    if has_user_data:
        dataset = PhaseB_1(**phase_kwargs)
    else:
        dataset = PhaseB_2(**phase_kwargs)

    X = dataset["X"].to_numpy()         # features
    y = np.asarray(dataset["y"])        # target

    return dataset, X, y


def train_elasticnet(X, y, alpha=1.0, l1_ratio=0.5, random_state=42):
    """
    Build and train an ElasticNet model inside a sklearn Pipeline.

    - StandardScaler -> ElasticNet
    - Returns fitted pipeline and some quick metrics.
    """

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=random_state
    )

    model = make_pipeline(
        StandardScaler(),
        ElasticNet(
            alpha=1.0,
            l1_ratio=0.5,
            random_state=random_state,
            max_iter=10000,      # more iterations for convergence
        ),
    )

    model.fit(X_train, y_train)

    # Predictions & metrics
    y_pred = model.predict(X_test)
    mse = mean_squared_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)

    # Quick cross-val for sanity (on full data)
    cv_scores = cross_val_score(
        model,
        X,
        y,
        cv=3,                  # fewer folds for tiny N
        scoring="r2",
    )

    metrics = {
        "mse": mse,
        "r2": r2,
        "cv_r2_mean": float(cv_scores.mean()),
        "cv_r2_std": float(cv_scores.std()),
    }

    return model, metrics


def main():
    parser = argparse.ArgumentParser(description="Train ElasticNet on ACS dataset")

    parser.add_argument(
        "--has-user-data",
        action="store_true",
        help="Use PhaseB_1 (stubbed user target) instead of PhaseB_2 (Census target).",
    )
    parser.add_argument(
        "--level",
        choices=["county", "state"],
        default="county",
        help="Aggregation level for dataset (default: county).",
    )
    parser.add_argument(
        "--state-fips",
        type=int,
        default=6,
        help="State FIPS used by PhaseB_* for county-level pulls (default: 6).",
    )
    parser.add_argument(
        "--dp-col",
        type=str,
        default="DP05_0094E",
        help="DP column to use as target (or as base for stub target).",
    )
    parser.add_argument(
        "--model-out",
        type=str,
        default="elasticnet_model.joblib",
        help="Path to save trained model (joblib).",
    )

    # Optional: you can extend this later for exclude-counties/states via CLI if you want
    args = parser.parse_args()

    # ---- Phase B: build dataset ----
    dataset, X, y = build_dataset(
        has_user_data=args.has_user_data,
        level=args.level,
        selected_state_fips=args.state_fips,
        dp_col=args.dp_col,
        exclude_counties_fips=[1, 37, 59] if args.level == "county" else None,
        exclude_states_fips=None,  # or e.g. [2, 15] for AK, HI at state level
    )

    print(f"Dataset built: X shape = {X.shape}, y length = {len(y)}")

    # ---- Train ElasticNet ----
    model, metrics = train_elasticnet(X, y)

    print("\n=== Training metrics ===")
    for k, v in metrics.items():
        print(f"{k}: {v:.4f}")

    # ---- Save model ----
    joblib.dump(model, args.model_out)
    print(f"\nModel saved to: {args.model_out}")


if __name__ == "__main__":
    main()

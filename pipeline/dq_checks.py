"""
dq_checks.py — Data Quality validation checks
Project: orders_pipeline
"""

import pandas as pd


def check_null_rate(df, column, threshold=0.05):
    """Check if null rate for a column exceeds threshold."""
    null_rate = df[column].isnull().mean()
    return {
        "check": f"null_rate_{column}",
        "value": round(null_rate, 4),
        "threshold": threshold,
        "passed": null_rate <= threshold,
    }


def check_duplicate_rate(df, threshold=0.01):
    """Check if duplicate row rate exceeds threshold."""
    dup_rate = df.duplicated().mean()
    return {
        "check": "duplicate_rate",
        "value": round(dup_rate, 4),
        "threshold": threshold,
        "passed": dup_rate <= threshold,
    }


def _to_numeric_series(series):
    """Coerce a series to numeric, turning non-numeric values into NaN."""
    return pd.to_numeric(series, errors="coerce")


def check_amount_range(df, min_val=0, max_val=9999):
    """Check if amount values fall within expected range (non-numeric treated as invalid)."""
    if "amount" not in df.columns:
        return {"check": "amount_range", "passed": True, "note": "column absent"}
    numeric = _to_numeric_series(df["amount"])
    non_numeric = df["amount"].notna() & numeric.isna()
    non_numeric_count = int(non_numeric.sum())
    in_range = numeric.dropna().between(min_val, max_val).all()
    passed = bool(in_range) and non_numeric_count == 0
    return {
        "check": "amount_range",
        "min": min_val,
        "max": max_val,
        "non_numeric_count": non_numeric_count,
        "passed": passed,
    }


def check_quantity_range(df, min_val=1, max_val=100):
    """Check if quantity values fall within expected range."""
    if "quantity" not in df.columns:
        return {"check": "quantity_range", "passed": True, "note": "column absent"}
    numeric = _to_numeric_series(df["quantity"])
    in_range = numeric.dropna().between(min_val, max_val).all()
    return {
        "check": "quantity_range",
        "min": min_val,
        "max": max_val,
        "passed": bool(in_range),
    }


def run_all_checks(df):
    """Run all DQ checks and return results list."""
    results = []
    for col in ["order_id", "customer_id", "order_date", "amount", "status"]:
        if col in df.columns:
            results.append(check_null_rate(df, col))
    results.append(check_duplicate_rate(df))
    results.append(check_amount_range(df))
    results.append(check_quantity_range(df))
    passed = sum(1 for r in results if r.get("passed"))
    score = round(passed / len(results) * 100, 1) if results else 0
    print(f"DQ Score: {score}% ({passed}/{len(results)} checks passed)")
    return results

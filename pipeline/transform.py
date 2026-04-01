"""
transform.py — Silver layer transformation logic
Project: orders_pipeline
"""

import pandas as pd


def normalize_status(df):
    """Normalize status column to title case."""
    if "status" in df.columns:
        df["status"] = df["status"].str.title()
    return df


def flag_negative_amounts(df):
    """Flag negative amount values as invalid."""
    if "amount" in df.columns:
        df["amount_valid"] = df["amount"].apply(
            lambda x: False if pd.notna(x) and x < 0 else True
        )
    return df


def flag_invalid_emails(df):
    """Flag emails missing @ symbol as invalid."""
    if "email" in df.columns:
        df["email_valid"] = df["email"].apply(
            lambda x: "@" in str(x) if pd.notna(x) else False
        )
    return df


def deduplicate(df, keys):
    """Remove duplicate rows based on composite key."""
    existing = [k for k in keys if k in df.columns]
    return df.drop_duplicates(subset=existing, keep="first") if existing else df


def run_transforms(df):
    """Run all Silver layer transformations."""
    df = normalize_status(df)
    df = flag_negative_amounts(df)
    df = flag_invalid_emails(df)
    df = deduplicate(df, ["order_id", "customer_id", "order_date"])
    return df


if __name__ == "__main__":
    df = pd.read_csv("data/transformed_data.csv")
    df = run_transforms(df)
    print(f"Transform complete: {len(df)} rows")

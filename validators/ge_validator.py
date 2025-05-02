import great_expectations as ge
import pandas as pd
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def create_data_expectations(df, table_name, context):
    """
    Create Great Expectations suite for data validation

    Args:
        df: DataFrame containing data to validate
        table_name: Name of the table
        context: Great Expectations context

    Returns:
        suite: Expectation suite
    """
    # Create a new expectation suite for the table
    expectation_suite_name = f"{table_name}_data_expectations"
    context.create_expectation_suite(expectation_suite_name, overwrite_existing=True)

    # Create a validator
    batch = context.get_batch(
        {"table": df.drop(["__schema__", "__table__"], axis=1, errors='ignore')},
        expectation_suite_name=expectation_suite_name
    )

    # Add basic expectations
    for column in df.columns:
        if column not in ["__schema__", "__table__"]:
            batch.expect_column_to_exist(column)

            # Check data type based on first non-null value
            non_null_values = df[column].dropna()
            if len(non_null_values) > 0:
                first_value = non_null_values.iloc[0]

                if isinstance(first_value, (int, float)):
                    batch.expect_column_values_to_be_in_type_list(
                        column,
                        ["int", "float", "INTEGER", "FLOAT", "NUMERIC", "int64", "float64"]
                    )
                elif isinstance(first_value, str):
                    if column.lower().endswith('date'):
                        # Check if it's a date string
                        batch.expect_column_values_to_match_regex(
                            column,
                            r"^\d{4}-\d{2}-\d{2}$"
                        )
                    else:
                        batch.expect_column_values_to_be_in_type_list(
                            column,
                            ["str", "STRING", "TEXT", "VARCHAR", "object"]
                        )

    # Save the expectation suite
    batch.save_expectation_suite(discard_failed_expectations=False)

    return context.get_expectation_suite(expectation_suite_name)


def validate_data(df, expectation_suite, context):
    """
    Validate data against expectations

    Args:
        df: DataFrame to validate
        expectation_suite: Great Expectations suite
        context: Great Expectations context

    Returns:
        dict: Validation results
    """
    # Create a validator
    batch = context.get_batch(
        {"table": df.drop(["__schema__", "__table__"], axis=1, errors='ignore')},
        expectation_suite_name=expectation_suite.expectation_suite_name
    )

    # Run validation
    result = batch.validate()

    return result


def compare_data_with_ge(df1, df2, table_name, context):
    """
    Compare two DataFrames using Great Expectations

    Args:
        df1: First DataFrame
        df2: Second DataFrame
        table_name: Name of the table
        context: Great Expectations context

    Returns:
        dict: Validation results
    """
    # Create expectations based on first DataFrame
    expectation_suite = create_data_expectations(df1, table_name, context)

    # Validate second DataFrame against those expectations
    validation_result = validate_data(df2, expectation_suite, context)

    return validation_result
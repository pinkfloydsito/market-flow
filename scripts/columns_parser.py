import os
import pandas as pd
import argparse


def clean_column_name(col_name):
    """
    Cleans a column name by:
    - Replacing spaces with underscores.
    - Removing special characters.
    - Normalizing to lowercase.
    - Adding a prefix if the name starts with a number.
    """
    col_name = col_name.strip().lower()
    col_name = col_name.replace(" ", "_")
    col_name = "".join(e for e in col_name if e.isalnum() or e == "_")
    if col_name[0].isdigit():
        col_name = f"col_{col_name}"
    return col_name


def generate_sql_schema(df, table_name):
    """
    Generates a SQL CREATE TABLE statement for the given DataFrame.
    """
    sql_schema = f"CREATE TABLE {table_name} (\n"
    for col in df.columns:
        sql_schema += f"    {col} TEXT,\n"
    sql_schema = sql_schema.rstrip(",\n") + "\n);"
    return sql_schema


def main():
    parser = argparse.ArgumentParser(
        description="Clean CSV column names and generate SQL schema."
    )
    parser.add_argument("csv_file", help="Path to the CSV file")
    parser.add_argument(
        "--table_name", default="cleaned_table", help="Name of the SQL table"
    )
    parser.add_argument("--output_sql", help="Path to save the SQL schema file")
    args = parser.parse_args()

    try:
        df = pd.read_csv(args.csv_file)
    except Exception as e:
        print(f"Error loading CSV: {e}")
        return

    df.columns = [clean_column_name(col) for col in df.columns]
    print("Normalized and cleaned column names:")
    print(df.columns)

    sql_schema = generate_sql_schema(df, args.table_name)

    if args.output_sql:
        try:
            with open(args.output_sql, "w") as file:
                file.write(sql_schema)
            print(f"\nSQL schema saved to {args.output_sql}")
        except Exception as e:
            print(f"Error saving SQL schema: {e}")


if __name__ == "__main__":
    main()

import pandas as pd


def clean_csv_file(file_path):
    """
    Clean CSV file by handling encoding issues
    """
    try:
        encodings = ["utf-8", "latin1", "iso-8859-1"]
        df = None

        for encoding in encodings:
            try:
                df = pd.read_csv(file_path, encoding=encoding)
                break
            except UnicodeDecodeError:
                continue

        if df is None:
            raise ValueError("Could not read file with any encoding")

        # Clean special characters if needed
        for column in df.select_dtypes(include=["object"]):
            df[column] = df[column].str.encode("ascii", "ignore").str.decode("ascii")

        # Save cleaned file
        clean_file_path = file_path.replace(".csv", "_cleaned.csv")
        df.to_csv(clean_file_path, index=False, encoding="utf-8")

        return clean_file_path

    except Exception as e:
        print(f"Error cleaning {file_path}: {str(e)}")
        raise

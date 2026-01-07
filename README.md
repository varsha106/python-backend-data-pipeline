# python-backend-data-pipeline
Python-based backend and data processing project demonstrating data transformation, automation, and analytical workflows using clean, modular, and production-ready practices.
"""
python-backend-data-pipeline
Author: Varsha Puli

Description:
A simple, modular Python data pipeline that demonstrates
data loading, cleaning, transformation, and analysis.
Designed with backend and production-ready practices.
"""

import pandas as pd
from typing import Optional


class DataPipeline:
    def __init__(self, input_path: str):
        self.input_path = input_path
        self.data: Optional[pd.DataFrame] = None

    def load_data(self) -> pd.DataFrame:
        """Load data from CSV file"""
        try:
            self.data = pd.read_csv(self.input_path)
            print("Data loaded successfully.")
            return self.data
        except Exception as e:
            raise RuntimeError(f"Failed to load data: {e}")

    def clean_data(self) -> pd.DataFrame:
        """Clean missing values and duplicates"""
        if self.data is None:
            raise ValueError("Data not loaded")

        self.data.drop_duplicates(inplace=True)
        self.data.fillna(method="ffill", inplace=True)

        print("Data cleaned successfully.")
        return self.data

    def transform_data(self) -> pd.DataFrame:
        """Apply basic transformations"""
        if self.data is None:
            raise ValueError("Data not loaded")

        # Example transformation
        numeric_columns = self.data.select_dtypes(include=["int64", "float64"]).columns
        for col in numeric_columns:
            self.data[col] = self.data[col].astype(float)

        print("Data transformed successfully.")
        return self.data

    def analyze_data(self) -> pd.DataFrame:
        """Generate summary statistics"""
        if self.data is None:
            raise ValueError("Data not loaded")

        summary = self.data.describe()
        print("Data analysis completed.")
        return summary

    def run(self) -> pd.DataFrame:
        """Run the full pipeline"""
        self.load_data()
        self.clean_data()
        self.transform_data()
        return self.analyze_data()


if __name__ == "__main__":
    pipeline = DataPipeline(input_path="data/input.csv")
    results = pipeline.run()
    print(results)

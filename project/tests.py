import unittest
import os
import subprocess
import pandas as pd

class TestDataPipeline(unittest.TestCase):

    def setUp(self):
        # File paths and URLs
        self.output_dir = './data'
        self.health_file = os.path.join(self.output_dir, 'health_cleaned_south_america.csv')
        self.gdp_file = os.path.join(self.output_dir, 'gdp_cleaned_south_america.csv')
        self.db_path = os.path.join(self.output_dir, 'data_cleaned_south_america.db')

    def test_health_expenditure_file_exists(self):
        """Test if the health expenditure file exists after running the pipeline."""
        print("Testing if health expenditure file exists...")
        if os.path.isfile(self.health_file):
            print(f"Success: {self.health_file} exists.")
        else:
            print(f"Failure: {self.health_file} does not exist.")
        self.assertTrue(os.path.isfile(self.health_file), f"{self.health_file} does not exist")
    
    def test_gdp_growth_file_exists(self):
        """Test if the GDP growth file exists after running the pipeline."""
        print("Testing if GDP growth file exists...")
        if os.path.isfile(self.gdp_file):
            print(f"Success: {self.gdp_file} exists.")
        else:
            print(f"Failure: {self.gdp_file} does not exist.")
        self.assertTrue(os.path.isfile(self.gdp_file), f"{self.gdp_file} does not exist")

    def test_sqlite_db_exists(self):
        """Test if the SQLite database file exists after running the pipeline."""
        print("Testing if SQLite database exists...")
        if os.path.isfile(self.db_path):
            print(f"Success: {self.db_path} exists.")
        else:
            print(f"Failure: {self.db_path} does not exist.")
        self.assertTrue(os.path.isfile(self.db_path), f"{self.db_path} does not exist")

    def test_blank_column_removed(self):
        """Test if blank columns were removed from the datasets."""
        print("Testing if blank columns were removed...")
        health_df = pd.read_csv(self.health_file)
        gdp_df = pd.read_csv(self.gdp_file)

        # Ensure there are no columns that are completely empty
        if health_df.isnull().all(axis=0).any():
            print("Blank columns found in health expenditure data.")
        else:
            print("No blank columns found in health expenditure data.")
        
        if gdp_df.isnull().all(axis=0).any():
            print("Blank columns found in GDP growth data.")
        else:
            print("No blank columns found in GDP growth data.")

        self.assertFalse(health_df.isnull().all(axis=0).any(), "Blank columns found in health expenditure data")
        self.assertFalse(gdp_df.isnull().all(axis=0).any(), "Blank columns found in GDP growth data")

    def test_data_reshaped(self):
        """Test if the data was reshaped into long format correctly."""
        print("Testing if data was reshaped into long format...")
        health_df = pd.read_csv(self.health_file)
        gdp_df = pd.read_csv(self.gdp_file)

        # Ensure the data has been reshaped (check for 'Year' and 'Value' columns)
        if 'Year' in health_df.columns and 'Value' in health_df.columns:
            print("Health expenditure data reshaped correctly.")
        else:
            print("Health expenditure data reshaping failed.")
        
        if 'Year' in gdp_df.columns and 'Value' in gdp_df.columns:
            print("GDP growth data reshaped correctly.")
        else:
            print("GDP growth data reshaping failed.")

        self.assertIn('Year', health_df.columns, "Year column is missing in health expenditure data")
        self.assertIn('Value', health_df.columns, "Value column is missing in health expenditure data")

        self.assertIn('Year', gdp_df.columns, "Year column is missing in GDP growth data")
        self.assertIn('Value', gdp_df.columns, "Value column is missing in GDP growth data")

    def test_database_exist(self):
        """Test if database exists after pipeline execution."""
        print("Testing if database exists after pipeline execution...")
        subprocess.run(['python', 'pipeline.py'], check=True)
        if os.path.exists(self.db_path):
            print(f"Success: Database {self.db_path} exists.")
        else:
            print(f"Failure: Database {self.db_path} does not exist.")
        self.assertTrue(os.path.exists(self.db_path), f"Database '{self.db_path}' does not exist.")

if __name__ == '__main__':
    unittest.main()

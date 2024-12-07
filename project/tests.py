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
        self.assertTrue(
            os.path.isfile(self.health_file),
            f"Failure: {self.health_file} does not exist. Success: {self.health_file} exists."
        )

    def test_gdp_growth_file_exists(self):
        """Test if the GDP growth file exists after running the pipeline."""
        self.assertTrue(
            os.path.isfile(self.gdp_file),
            f"Failure: {self.gdp_file} does not exist. Success: {self.gdp_file} exists."
        )

    def test_sqlite_db_exists(self):
        """Test if the SQLite database file exists after running the pipeline."""
        self.assertTrue(
            os.path.isfile(self.db_path),
            f"Failure: {self.db_path} does not exist. Success: {self.db_path} exists."
        )

    def test_blank_column_removed(self):
        """Test if blank columns were removed from the datasets."""
        health_df = pd.read_csv(self.health_file)
        gdp_df = pd.read_csv(self.gdp_file)

        # Assert no blank columns in health expenditure data
        self.assertTrue(
            not health_df.isnull().all(axis=0).any(),
            "Blank columns found in health expenditure data"
        )

        # Assert no blank columns in GDP growth data
        self.assertTrue(
            not gdp_df.isnull().all(axis=0).any(),
            "Blank columns found in GDP growth data."
        )

    def test_data_reshaped(self):
        """Test if the data was reshaped into long format correctly."""
        health_df = pd.read_csv(self.health_file)
        gdp_df = pd.read_csv(self.gdp_file)

        # Assert that 'Year' and 'Value' columns exist in health expenditure data
        self.assertTrue(
        'Year' in health_df.columns and 'Value' in health_df.columns,
        "Health expenditure data reshaping failed. 'Year' and 'Value' columns missing."
    )
        print("Test success: 'Year' and 'Value' columns found in health expenditure data.")

        # Assert that 'Year' and 'Value' columns exist in GDP growth data
        self.assertTrue(
        'Year' in gdp_df.columns and 'Value' in gdp_df.columns,
        "GDP growth data reshaping failed. 'Year' and 'Value' columns missing."
    )
        print("Test success: 'Year' and 'Value' columns found in GDP growth data.")

        # Test for missing values in health expenditure data
        self.assertTrue(
        not health_df.isnull().any().any(),
        "Health expenditure data contains missing values."
    )
        print("Test success: No missing values found in health expenditure data.")

        # Test for missing values in GDP growth data
        self.assertTrue(
        not gdp_df.isnull().any().any(),
        "GDP growth data contains missing values."
    )
    print("Test success: No missing values found in GDP growth data.")


    def test_database_exist(self):
        """Test if database exists after pipeline execution."""
        # Print the current working directory for debugging (optional)
        print(f"Current working directory: {os.getcwd()}")

        # Update this line to the correct path
        pipeline_path = os.path.abspath('./pipeline.py')  # Correct path here

        # Print a custom message when pipeline is found
        print("Pipeline found and executing...")

        # Run the pipeline script
        try:
            subprocess.run(['python', pipeline_path], check=True)
        except subprocess.CalledProcessError as e:
            self.fail(f"Pipeline execution failed with error: {e}")
    
        # Generate the absolute path for the database
        db_path = os.path.abspath('./data/data_cleaned_south_america.db')
    
        # Assert the database exists
        self.assertTrue(
        os.path.exists(db_path),
        f"Failure: Database {db_path} does not exist. Success: Database creation verified."
    )



if __name__ == '__main__':
    unittest.main()
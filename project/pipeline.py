# question
# Follow your project plan to build an automated data pipeline for your project
#     Write a script (for example in Python or Jayvee) that pulls the data sets you chose from the internet, transforms it and fixes errors, and finally stores your data in the /data directory
#         Place the script in the /project directory (any file name is fine)
#         Add a /project/pipeline.sh that starts your pipeline as you would do from the command line as entry point:
#             E.g. if you run your script on your command line using `python3 /project/pipeline.py`, create a /project/pipeline.sh with the content:
#                     #!/bin/bash
#                     python3 /project/pipeline.py
#     The output of the script should be: datasets in your /data directory (e.g., as SQLite databases)
#         Do NOT check in your data sets, just your script
#         You can use .gitignore to avoid checking in files on git
#         This data set will be the base for your data report in future project work.
# Update the issues and project plan if necessary............


import pandas as pd
import requests, os
from zipfile import ZipFile
from sqlalchemy import create_engine


class Pipeline:
    def __init__(self, url1, url2, save_file_name):
        self.url1 = url1
        self.url2 = url2
        self.data1 = None
        self.data2 = None
        path = 'sqlite:///data//' + save_file_name + '.sqlite'
        self.engine = create_engine(path, echo=False)
        self.files_to_delete = []

    def get_data(self):
        self.data1, items_to_delete1 = get_data_helper(self.url1, 2, "Capital Bikeshare")
        self.data2, items_to_delete2 = get_data_helper(self.url2, 0, "Seoul Bikeshare")
        self.files_to_delete.extend(items_to_delete1)
        self.files_to_delete.extend(items_to_delete2)


    def transform_data(self):
        self.data1.drop(self.data1.columns[0], axis=1, inplace=True)  # Deleting instant as it is just an index
        self.data1.dropna(thresh=3)  # Deleting a row if it has 3 or more NA values
        self.data1.bfill()  # Filling the remaining NA values backward (Imputation)
        
        self.data2.dropna(thresh=3)  # Deleting a row if it has more 3 or more NA values
        self.data2.bfill()  # Filling the remaining NA values backward (Imputation)

    def save_data(self):
        self.data1.to_sql("Capital Bikeshare", self.engine, if_exists='replace', index=False)  # saving Capital Bikeshare data
        self.data2.to_sql("Seoul Bikeshare", self.engine, if_exists='replace', index=False)  # saving Seoul Bikeshare data

        for pa in self.files_to_delete:  # Removing downloaded and extracted data
            os.remove(pa)
        
        self.engine.dispose()

    def run_pipeline(self):
        self.get_data()
        print("Got the Datasets!")
        self.transform_data()
        print("Datasets Transformed!")
        self.save_data()
        print("Datasets Saved!")


def get_data_helper(url, idx, filename):
    """Helper function to get the data, as we may use it get data from several urls"""
    print("Downloading", url)
    response = requests.get(url)

    if response.status_code == 200:
        filename = filename + ".zip"

        # Write the downloaded content to the file
        with open(filename, 'wb') as f:
            f.write(response.content)

        # Extract the CSV file from the zip
        with ZipFile(filename, 'r') as zip_ref:
            csv_filename = zip_ref.namelist()[idx]  # Get the csv file name
            zip_ref.extract(csv_filename)  # Extract the file

        # Load the extracted CSV file into a pandas DataFrame
        df = pd.read_csv(csv_filename, encoding='unicode_escape')

        return df, [filename, csv_filename]
    else:
        print(f"Download failed for {url}. Status code: {response.status_code}")


if __name__ == '__main__':
    pipe = Pipeline("https://archive.ics.uci.edu/static/public/275/bike+sharing+dataset.zip",
                    "https://archive.ics.uci.edu/static/public/560/seoul+bike+sharing+demand.zip",
                    "bike_data")
    pipe.run_pipeline()
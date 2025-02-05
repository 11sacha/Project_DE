import os
import pandas as pd
import datetime
import traceback

FilePath = os.path.dirname(__file__)
os.chdir(os.path.join(FilePath, '../'))
ProjectPath = os.getcwd()
InputFolder = os.path.join(ProjectPath, r'Input')
OutputFolder = os.path.join(ProjectPath, r'Output')


def run_process():

    def clean_data(df):
        print('Simple data cleaning begins..')
        df.columns = [col.strip().lower().replace(",", '') for col in df.columns]  # Standardize column names
        df.dropna(inplace=True)
        print('Data cleaning done.')
        return df

    try:
        print('Process begins..')
        file_names = ["bikes_dataset.csv", "car_dataset.csv", "trucks_dataset.csv"]

        for file in file_names:
            print(file)
            input_path = os.path.join(InputFolder, file)
            output_path = os.path.join(OutputFolder, file)
            
            if os.path.exists(input_path):
                df = pd.read_csv(input_path, error_bad_lines=False, warn_bad_lines=True)
                df_cleaned = clean_data(df)
                df_cleaned.to_csv(output_path, index=False)
                print(f"File: {file} processed")
        
        print('Process completed.')

    except Exception as e:
        print(f'Error: {e}')
        traceback.print_exc()

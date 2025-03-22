import pandas as pd
import matplotlib.pyplot as plt
from prefect import task, flow, get_run_logger


@task(name = 'Read Data')
def read_data():
    # Step 1: Fetch Data
    print("Reading data...")
    # Assume a dataset with sales figures and other fields is provided.
    df = pd.read_csv("analytics_data.csv")
    print(f"Data shape: {df.shape}")
    return df

@task(name = 'Validate Data')
def validate_data(df):
    print("Validating data...")
    missing_values = df.isnull().sum()
    print("Missing values:\n", missing_values)
    # For simplicity, drop any rows with missing values
    df_clean = df.dropna()
    return df_clean

@task(name = 'Transform Data')
def transform_data(df_clean):
    print("Transforming data...")
    # For example, if there is a "sales" column, create a normalized version.
    if "sales" in df_clean.columns:
        df_clean["sales_normalized"] = (df_clean["sales"] - df_clean["sales"].mean()) / df_clean["sales"].std()
    return df_clean

@task(name = 'Generate Analytics Report')
def generate_analytics_report(df_clean):
    print("Generating analytics report...")
    summary = df_clean.describe()
    summary.to_csv("analytics_summary.csv")
    print("Summary statistics saved to analytics_summary.csv")
    return summary

@task(name = 'Create Histogram')
def create_histogram(df_clean):
    if "sales" in df_clean.columns:
        plt.hist(df_clean["sales"], bins=20)
        plt.title("Sales Distribution")
        plt.xlabel("Sales")
        plt.ylabel("Frequency")
        plt.savefig("sales_histogram.png")
        plt.close()
        print("Sales histogram saved to sales_histogram.png")

@task(name = 'Complete')
def complete():
    print("Analytics pipeline completed.")

@flow(name = 'analytics_pipeline')
def analytics_pipeline():
    df = read_data()
    df_clean = validate_data(df)
    df_clean = transform_data(df_clean)
    summary = generate_analytics_report(df_clean)
    create_histogram(df_clean)
    complete()

if __name__ == "__main__":   
    analytics_pipeline()

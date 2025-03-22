import pandas as pd
import matplotlib.pyplot as plt
from prefect import task, flow, get_run_logger

@task
def fetch_data():
    print("Reading data...")
    # Assume a dataset with sales figures and other fields is provided.
    df = pd.read_csv("data/analytics_data.csv")
    return df
    
@task
def validate_data(df):
    missing_values = df.isnull().sum()
    print("Missing values:\n", missing_values)
    # For simplicity, drop any rows with missing values
    df_clean = df.dropna()
    return df_clean

@task
def transform_data(df_clean):
    # For example, if there is a "sales" column, create a normalized version.
    if "sales" in df_clean.columns:
        df_clean["sales_normalized"] = (df_clean["sales"] - df_clean["sales"].mean()) / df_clean["sales"].std()
    return df_clean

@task
def generate_analytics_report(df_clean):
    print("Generating analytics report...")
    summary = df_clean.describe()
    summary.to_csv("data/analytics_summary.csv")
    print("Summary statistics saved to data/analytics_summary.csv")

@task
def plot_sales(df_clean):
    if "sales" in df_clean.columns:
        plt.hist(df_clean["sales"], bins=20)
        plt.title("Sales Distribution")
        plt.xlabel("Sales")
        plt.ylabel("Frequency")
        plt.savefig("data/sales_histogram.png")
        plt.close()
        print("Sales histogram saved to data/sales_histogram.png")

@flow()
def main():
    # Step 1: Fetch Data
    df = fetch_data()
    print(f"Data shape: {df.shape}")

    # Step 2: Validate Data
    print("Validating data...")
    df_clean = validate_data(df)

    # Step 3: Transform Data
    print("Transforming data...")
    df_clean = transform_data(df_clean)
    
    # Step 4: Generate Analytics Report
    generate_analytics_report(df_clean)

    # Step 5: Create a Histogram for Sales Distribution
    plot_sales(df_clean)

    print("Analytics pipeline completed.")

if __name__ == "__main__":
    main()

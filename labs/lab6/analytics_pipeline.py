import pandas as pd
import matplotlib.pyplot as plt
from prefect import task, flow, get_run_logger
 
@task(log_prints=True)
def read_csv():
    logger = get_run_logger()
    df = pd.read_csv("data/analytics_data.csv")
    print(f"Data shape: {df.shape}")
    logger.info(f"Data shape: {df.shape}")
    return df


@task(log_prints=True)
def validate_and_clean(df: pd.DataFrame):
    logger = get_run_logger()
    logger.info("Validating data...")
    missing_values = df.isnull().sum()
    logger.info("Missing values:\n", missing_values)
    # For simplicity, drop any rows with missing values
    df_clean = df.dropna()
    return df_clean

@task(log_prints=True)
def transform_data(df_clean: pd.DataFrame):
    logger = get_run_logger()
    logger.info("Transforming data...")
    if "sales" in df_clean.columns:
        df_clean["sales_normalized"] = (df_clean["sales"] - df_clean["sales"].mean()) / df_clean["sales"].std()
    return df_clean

@task(log_prints=True)
def generate_report(df_clean: pd.DataFrame):
    logger = get_run_logger()
    logger.info("Generating analytics report...")
    summary = df_clean.describe()
    summary.to_csv("data/analytics_summary.csv")
    logger.info("Summary statistics saved to data/analytics_summary.csv")

@task(log_prints=True)
def create_histogram(df_clean: pd.DataFrame):
    logger = get_run_logger()
    if "sales" in df_clean.columns:
        plt.hist(df_clean["sales"], bins=20)
        plt.title("Sales Distribution")
        plt.xlabel("Sales")
        plt.ylabel("Frequency")
        plt.savefig("data/sales_histogram.png")
        plt.close()
        print("Sales histogram saved to data/sales_histogram.png")
        logger.info("Sales histogram saved to data/sales_histogram.png")

    logger.info("Analytics pipeline completed.")
    
@flow(name = "analytics pipeline")
def main():
    # Step 1: Fetch Data
    print("Reading data...")
    # Assume a dataset with sales figures and other fields is provided.
    df = read_csv()    

    # Step 2: Validate Data
    print("Validating data...")
    df = validate_and_clean(df)

    # Step 3: Transform Data
    print("Transforming data...")
    df = transform_data(df)

    # Step 4: Generate Analytics Report
    print("Generating analytics report...")
    generate_report(df)

    # Step 5: Create a Histogram for Sales Distribution
    create_histogram(df)
    print("Analytics pipeline completed.")

if __name__ == "__main__":
    main()

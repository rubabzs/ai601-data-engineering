import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score
from sklearn.preprocessing import StandardScaler
import joblib
from prefect import task, flow, get_run_logger


@task(name = 'Read Data')
def read_data(dataset_path: str):    
    print(f"Reading data from {dataset_path}")
    df = pd.read_csv(dataset_path)
    print(f"Data shape: {df.shape}")
    return df

@task(name = 'Validate Data')
def validate_data(df):
    print("Validating data")
    missing_values = df.isnull().sum()
    print("Missing values:\n", missing_values)
    # Fill missing numeric values with the median
    df.fillna(df.median(numeric_only=True), inplace=True)
    return df

@task(name = 'Transform Data')
def transform_data(df):
    print("Transforming data")
    # Assume the last column is the target variable.
    features = df.iloc[:, :-1]
    target = df.iloc[:, -1]
    scaler = StandardScaler()
    scaled_features = scaler.fit_transform(features)
    # Reassemble the DataFrame with scaled features
    df_transformed = pd.DataFrame(scaled_features, columns=features.columns)
    df_transformed["target"] = target.values
    return df_transformed

@task(name="Train Model", retries=3, retry_delay_seconds=10)
def train_model(df, test_size: float = 0.2, random_state: int = 42):
    print("Training model")
    X = df.drop("target", axis=1)
    y = df["target"]
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=test_size, random_state=random_state)
    model = RandomForestClassifier(random_state=random_state)
    model.fit(X_train, y_train)
    print("Model training complete")
    return model, X_test, y_test

@task(name = 'Evaluate Model')
def evaluate_model(model, X_test, y_test):
    print("Evaluating model")
    predictions = model.predict(X_test)
    acc = accuracy_score(y_test, predictions)
    print(f"Model accuracy: {acc}")
    return acc

@task(name = 'Save Model')
def save_model(model, accuracy: float, threshold: float, model_path: str = "model.joblib"):
    if accuracy >= threshold:
        print(f"Accuracy {accuracy} meets threshold {threshold}. Saving model to {model_path}")
        joblib.dump(model, model_path)
    else:
        print(f"Accuracy {accuracy} below threshold {threshold}. Model not saved.")

@task(name = 'Complete')
def complete():
    print("ML pipeline completed.")

@flow(name = 'ml_pipeline')
def ml_pipeline():
    dataset_path = "/home/hadoop/AI601_lab6/lab6/Iris.csv"
    accuracy_threshold = 0.9
    test_size = 0.2

    df = read_data(dataset_path)
    df_validated = validate_data(df)
    df_transformed = transform_data(df_validated)
    model, X_test, y_test = train_model(df_transformed, test_size=test_size)
    accuracy = evaluate_model(model, X_test, y_test)
    save_model(model, accuracy, accuracy_threshold)
    complete()

if __name__ == "__main__":
    ml_pipeline()
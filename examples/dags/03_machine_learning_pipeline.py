"""
Machine Learning Pipeline with Slurm Executor

This example demonstrates a complete ML pipeline using different Slurm partitions
for different computational requirements (CPU for preprocessing, GPU for training).
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    "owner": "ml-team",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "ml_pipeline_slurm",
    default_args=default_args,
    description="Complete ML pipeline using Slurm executor",
    schedule_interval="0 3 * * 1",  # Weekly on Mondays at 3 AM
    catchup=False,
    max_active_runs=1,
    tags=["slurm", "machine-learning", "gpu", "training"],
)


def check_data_availability():
    """Check if new training data is available."""
    import os
    import random

    # Simulate data availability check
    data_available = random.choice([True, True, False])  # 66% chance of data

    if data_available:
        print("✓ New training data is available")
        return "prepare_training_data"
    else:
        print("✗ No new training data available, skipping training")
        return "skip_training"


# Start
start = DummyOperator(task_id="start", dag=dag)

# Data availability check
check_data = BranchPythonOperator(
    task_id="check_data_availability",
    python_callable=check_data_availability,
    executor_config={
        "slurm": {
            "partition": "normal",
            "cpus_per_task": 1,
            "mem": "256M",
            "time_limit": "00:01:00",
        }
    },
    dag=dag,
)

# Skip path for when no data is available
skip_training = DummyOperator(task_id="skip_training", dag=dag)

# Data preparation - CPU intensive
prepare_training_data = BashOperator(
    task_id="prepare_training_data",
    bash_command="""
    echo "=== Preparing Training Data ==="
    
    WORK_DIR="/tmp/ml_pipeline_{{ ds_nodash }}"
    mkdir -p "$WORK_DIR/data" "$WORK_DIR/models" "$WORK_DIR/results"
    
    echo "Creating synthetic dataset..."
    
    # Generate synthetic training data
    python3 << 'EOF'
import numpy as np
import pandas as pd
import os

work_dir = "/tmp/ml_pipeline_{{ ds_nodash }}"

# Generate synthetic dataset for binary classification
np.random.seed(42)
n_samples = 10000
n_features = 20

# Generate features
X = np.random.randn(n_samples, n_features)

# Generate target with some signal
weights = np.random.randn(n_features) * 0.5
y = (np.dot(X, weights) + np.random.randn(n_samples) * 0.1 > 0).astype(int)

# Create DataFrame
feature_names = [f'feature_{i:02d}' for i in range(n_features)]
df = pd.DataFrame(X, columns=feature_names)
df['target'] = y

# Split into train/validation/test
train_size = int(0.7 * len(df))
val_size = int(0.15 * len(df))

train_df = df[:train_size]
val_df = df[train_size:train_size + val_size]
test_df = df[train_size + val_size:]

# Save datasets
train_df.to_csv(f'{work_dir}/data/train.csv', index=False)
val_df.to_csv(f'{work_dir}/data/validation.csv', index=False)
test_df.to_csv(f'{work_dir}/data/test.csv', index=False)

print(f"Generated datasets:")
print(f"  Training: {len(train_df)} samples")
print(f"  Validation: {len(val_df)} samples")
print(f"  Test: {len(test_df)} samples")
print(f"  Features: {n_features}")

# Generate data statistics
stats = {
    'n_samples': len(df),
    'n_features': n_features,
    'train_samples': len(train_df),
    'val_samples': len(val_df),
    'test_samples': len(test_df),
    'target_balance': df['target'].value_counts().to_dict()
}

import json
with open(f'{work_dir}/data/data_stats.json', 'w') as f:
    json.dump(stats, f, indent=2)

print("Data preparation completed!")
EOF
    
    echo "Data preparation completed successfully"
    echo "Files created:"
    ls -la "$WORK_DIR/data/"
    """,
    executor_config={
        "slurm": {
            "partition": "normal",
            "cpus_per_task": 2,
            "mem": "2G",
            "time_limit": "00:10:00",
        }
    },
    dag=dag,
)

# Feature engineering - CPU intensive
feature_engineering = BashOperator(
    task_id="feature_engineering",
    bash_command="""
    echo "=== Feature Engineering ==="
    
    WORK_DIR="/tmp/ml_pipeline_{{ ds_nodash }}"
    
    python3 << 'EOF'
import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler, PolynomialFeatures
from sklearn.decomposition import PCA
import joblib
import os

work_dir = "/tmp/ml_pipeline_{{ ds_nodash }}"

# Load datasets
train_df = pd.read_csv(f'{work_dir}/data/train.csv')
val_df = pd.read_csv(f'{work_dir}/data/validation.csv')
test_df = pd.read_csv(f'{work_dir}/data/test.csv')

print("Starting feature engineering...")

# Separate features and target
feature_cols = [col for col in train_df.columns if col != 'target']
X_train = train_df[feature_cols]
y_train = train_df['target']

X_val = val_df[feature_cols]
y_val = val_df['target']

X_test = test_df[feature_cols]
y_test = test_df['target']

print(f"Original features: {len(feature_cols)}")

# 1. Standardization
print("Applying standardization...")
scaler = StandardScaler()
X_train_scaled = scaler.fit_transform(X_train)
X_val_scaled = scaler.transform(X_val)
X_test_scaled = scaler.transform(X_test)

# 2. Polynomial features (degree 2, interaction only)
print("Creating interaction features...")
poly = PolynomialFeatures(degree=2, interaction_only=True, include_bias=False)
X_train_poly = poly.fit_transform(X_train_scaled)
X_val_poly = poly.transform(X_val_scaled)
X_test_poly = poly.transform(X_test_scaled)

print(f"Features after polynomial expansion: {X_train_poly.shape[1]}")

# 3. PCA for dimensionality reduction
print("Applying PCA...")
pca = PCA(n_components=50)
X_train_pca = pca.fit_transform(X_train_poly)
X_val_pca = pca.transform(X_val_poly)
X_test_pca = pca.transform(X_test_poly)

print(f"Features after PCA: {X_train_pca.shape[1]}")
print(f"Explained variance ratio: {pca.explained_variance_ratio_.sum():.3f}")

# Save processed features
np.save(f'{work_dir}/data/X_train_processed.npy', X_train_pca)
np.save(f'{work_dir}/data/X_val_processed.npy', X_val_pca)
np.save(f'{work_dir}/data/X_test_processed.npy', X_test_pca)

np.save(f'{work_dir}/data/y_train.npy', y_train.values)
np.save(f'{work_dir}/data/y_val.npy', y_val.values)
np.save(f'{work_dir}/data/y_test.npy', y_test.values)

# Save preprocessing pipelines
joblib.dump(scaler, f'{work_dir}/models/scaler.pkl')
joblib.dump(poly, f'{work_dir}/models/poly_features.pkl')
joblib.dump(pca, f'{work_dir}/models/pca.pkl')

print("Feature engineering completed!")
EOF
    """,
    executor_config={
        "slurm": {
            "partition": "normal",
            "cpus_per_task": 4,
            "mem": "4G",
            "time_limit": "00:15:00",
        }
    },
    dag=dag,
)

# Model training - potentially GPU intensive
train_models = BashOperator(
    task_id="train_models",
    bash_command="""
    echo "=== Training Models ==="
    
    WORK_DIR="/tmp/ml_pipeline_{{ ds_nodash }}"
    
    python3 << 'EOF'
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.svm import SVC
from sklearn.metrics import classification_report, roc_auc_score
import joblib
import json
import time

work_dir = "/tmp/ml_pipeline_{{ ds_nodash }}"

# Load processed data
X_train = np.load(f'{work_dir}/data/X_train_processed.npy')
X_val = np.load(f'{work_dir}/data/X_val_processed.npy')
X_test = np.load(f'{work_dir}/data/X_test_processed.npy')

y_train = np.load(f'{work_dir}/data/y_train.npy')
y_val = np.load(f'{work_dir}/data/y_val.npy')
y_test = np.load(f'{work_dir}/data/y_test.npy')

print(f"Training data shape: {X_train.shape}")
print(f"Validation data shape: {X_val.shape}")

# Define models to train
models = {
    'logistic_regression': LogisticRegression(random_state=42, max_iter=1000),
    'random_forest': RandomForestClassifier(n_estimators=100, random_state=42, n_jobs=-1),
    'gradient_boosting': GradientBoostingClassifier(n_estimators=100, random_state=42),
    'svm': SVC(probability=True, random_state=42)
}

results = {}

for model_name, model in models.items():
    print(f"\\nTraining {model_name}...")
    start_time = time.time()
    
    # Train model
    model.fit(X_train, y_train)
    
    # Predict on validation set
    val_pred = model.predict(X_val)
    val_prob = model.predict_proba(X_val)[:, 1]
    
    # Calculate metrics
    val_auc = roc_auc_score(y_val, val_prob)
    
    training_time = time.time() - start_time
    
    print(f"  Training time: {training_time:.2f} seconds")
    print(f"  Validation AUC: {val_auc:.4f}")
    
    # Save model
    joblib.dump(model, f'{work_dir}/models/{model_name}.pkl')
    
    # Store results
    results[model_name] = {
        'val_auc': val_auc,
        'training_time': training_time
    }

# Save training results
with open(f'{work_dir}/results/training_results.json', 'w') as f:
    json.dump(results, f, indent=2)

# Find best model
best_model_name = max(results.keys(), key=lambda k: results[k]['val_auc'])
best_auc = results[best_model_name]['val_auc']

print(f"\\nBest model: {best_model_name} (AUC: {best_auc:.4f})")

# Save best model info
with open(f'{work_dir}/models/best_model.txt', 'w') as f:
    f.write(best_model_name)

print("Model training completed!")
EOF
    """,
    executor_config={
        "slurm": {
            "partition": "normal",  # Could be 'gpu' if available
            "cpus_per_task": 8,  # More CPUs for parallel training
            "mem": "8G",
            "time_limit": "00:30:00",
        }
    },
    dag=dag,
)

# Model evaluation
evaluate_models = BashOperator(
    task_id="evaluate_models",
    bash_command='''
    echo "=== Model Evaluation ==="
    
    WORK_DIR="/tmp/ml_pipeline_{{ ds_nodash }}"
    
    python3 << 'EOF'
import numpy as np
import pandas as pd
import joblib
import json
from sklearn.metrics import classification_report, confusion_matrix, roc_auc_score

work_dir = "/tmp/ml_pipeline_{{ ds_nodash }}"

# Load test data
X_test = np.load(f'{work_dir}/data/X_test_processed.npy')
y_test = np.load(f'{work_dir}/data/y_test.npy')

# Get best model name
with open(f'{work_dir}/models/best_model.txt', 'r') as f:
    best_model_name = f.read().strip()

print(f"Evaluating best model: {best_model_name}")

# Load and evaluate best model
best_model = joblib.load(f'{work_dir}/models/{best_model_name}.pkl')

# Make predictions
test_pred = best_model.predict(X_test)
test_prob = best_model.predict_proba(X_test)[:, 1]

# Calculate metrics
test_auc = roc_auc_score(y_test, test_prob)
test_report = classification_report(y_test, test_pred, output_dict=True)

print(f"Test AUC: {test_auc:.4f}")
print("\\nClassification Report:")
print(classification_report(y_test, test_pred))

# Save evaluation results
evaluation_results = {
    'best_model': best_model_name,
    'test_auc': test_auc,
    'classification_report': test_report,
    'confusion_matrix': confusion_matrix(y_test, test_pred).tolist()
}

with open(f'{work_dir}/results/evaluation_results.json', 'w') as f:
    json.dump(evaluation_results, f, indent=2)

# Generate model card
model_card = f"""
# Model Card: {best_model_name}

## Model Performance
- **Test AUC**: {test_auc:.4f}
- **Precision**: {test_report['1']['precision']:.4f}
- **Recall**: {test_report['1']['recall']:.4f}
- **F1-Score**: {test_report['1']['f1-score']:.4f}

## Dataset Information
- **Training samples**: {len(np.load(f'{work_dir}/data/y_train.npy'))}
- **Test samples**: {len(y_test)}
- **Features**: {X_test.shape[1]}

## Model Details
- **Algorithm**: {best_model_name}
- **Training date**: {{ ds }}
- **Preprocessing**: StandardScaler + Polynomial Features + PCA

## Usage
Load the model using:
```python
import joblib
model = joblib.load('{work_dir}/models/{best_model_name}.pkl')
```
"""

with open(f'{work_dir}/results/model_card.md', 'w') as f:
    f.write(model_card)

print("Model evaluation completed!")
EOF
    ''',
    executor_config={
        "slurm": {
            "partition": "normal",
            "cpus_per_task": 2,
            "mem": "2G",
            "time_limit": "00:10:00",
        }
    },
    dag=dag,
)

# Model deployment preparation
prepare_deployment = BashOperator(
    task_id="prepare_deployment",
    bash_command="""
    echo "=== Preparing Model for Deployment ==="
    
    WORK_DIR="/tmp/ml_pipeline_{{ ds_nodash }}"
    DEPLOY_DIR="$WORK_DIR/deployment"
    mkdir -p "$DEPLOY_DIR"
    
    # Get best model name
    BEST_MODEL=$(cat "$WORK_DIR/models/best_model.txt")
    echo "Best model: $BEST_MODEL"
    
    # Copy model artifacts
    cp "$WORK_DIR/models/$BEST_MODEL.pkl" "$DEPLOY_DIR/model.pkl"
    cp "$WORK_DIR/models/scaler.pkl" "$DEPLOY_DIR/"
    cp "$WORK_DIR/models/poly_features.pkl" "$DEPLOY_DIR/"
    cp "$WORK_DIR/models/pca.pkl" "$DEPLOY_DIR/"
    
    # Create deployment manifest
    cat > "$DEPLOY_DIR/manifest.json" << EOF
{
    "model_name": "$BEST_MODEL",
    "model_version": "{{ ds_nodash }}",
    "created_date": "{{ ds }}",
    "artifacts": [
        "model.pkl",
        "scaler.pkl", 
        "poly_features.pkl",
        "pca.pkl"
    ],
    "input_features": 50,
    "preprocessing_required": true
}
EOF
    
    # Create prediction script
    cat > "$DEPLOY_DIR/predict.py" << 'EOF'
import joblib
import numpy as np
import pandas as pd

class ModelPredictor:
    def __init__(self, model_dir):
        self.model = joblib.load(f"{model_dir}/model.pkl")
        self.scaler = joblib.load(f"{model_dir}/scaler.pkl")
        self.poly = joblib.load(f"{model_dir}/poly_features.pkl")
        self.pca = joblib.load(f"{model_dir}/pca.pkl")
    
    def predict(self, X):
        # Apply preprocessing pipeline
        X_scaled = self.scaler.transform(X)
        X_poly = self.poly.transform(X_scaled)
        X_pca = self.pca.transform(X_poly)
        
        # Make prediction
        predictions = self.model.predict(X_pca)
        probabilities = self.model.predict_proba(X_pca)
        
        return predictions, probabilities

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Usage: python predict.py <model_dir>")
        sys.exit(1)
    
    model_dir = sys.argv[1]
    predictor = ModelPredictor(model_dir)
    print(f"Model loaded successfully from {model_dir}")
EOF
    
    echo "Deployment package prepared:"
    ls -la "$DEPLOY_DIR"
    
    echo "Deployment ready at: $DEPLOY_DIR"
    """,
    dag=dag,
)

# Cleanup (with trigger rule to run regardless of upstream success)
cleanup = BashOperator(
    task_id="cleanup",
    bash_command="""
    echo "=== Cleanup ==="
    WORK_DIR="/tmp/ml_pipeline_{{ ds_nodash }}"
    
    if [ -d "$WORK_DIR" ]; then
        echo "Workspace size before cleanup:"
        du -sh "$WORK_DIR"
        
        # Keep results and deployment, remove large temporary files
        find "$WORK_DIR/data" -name "*.npy" -delete
        find "$WORK_DIR/data" -name "*.csv" -delete
        
        echo "Workspace size after cleanup:"
        du -sh "$WORK_DIR"
        
        echo "Remaining files:"
        find "$WORK_DIR" -type f | head -20
    fi
    """,
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    dag=dag,
)

# End
end = DummyOperator(
    task_id="end", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS, dag=dag
)

# Define dependencies
start >> check_data

# Main pipeline path
(
    check_data
    >> prepare_training_data
    >> feature_engineering
    >> train_models
    >> evaluate_models
    >> prepare_deployment
)

# Skip path
check_data >> skip_training

# Both paths converge at cleanup
[prepare_deployment, skip_training] >> cleanup >> end

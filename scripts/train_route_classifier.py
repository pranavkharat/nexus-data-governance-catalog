"""
Train XGBoost classifier for query routing
Predicts optimal route given query features
"""

import json
import sys
import os
import joblib
import pandas as pd
from sklearn.model_selection import cross_val_score, StratifiedKFold
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import classification_report, confusion_matrix
import xgboost as xgb

# Path setup
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
sys.path.insert(0, PROJECT_ROOT)

TRAINING_DATA_PATH = os.path.join(PROJECT_ROOT, 'data', 'training', 'performance_based_labels.json')
MODEL_DIR = os.path.join(PROJECT_ROOT, 'models')

def train_classifier():
    """Train XGBoost route classifier"""
    
    print("🤖 Training XGBoost Route Classifier")
    print("=" * 70)
    
    # Load training data
    with open(TRAINING_DATA_PATH, 'r') as f:
        training_data = json.load(f)
    
    print(f"✅ Loaded {len(training_data)} training examples")
    
    # Convert to DataFrame
    data = []
    for ex in training_data:
        row = ex['features'].copy()
        row['best_route'] = ex['best_route']
        row['question'] = ex['question']
        data.append(row)
    
    df = pd.DataFrame(data)
    
    # Separate features and labels
    X = df.drop(['best_route', 'question'], axis=1)
    y = df['best_route']
    
    print(f"\n📊 Dataset Info:")
    print(f"   Features: {X.shape[1]}")
    print(f"   Samples: {X.shape[0]}")
    print(f"   Classes: {y.nunique()}")
    print(f"\n   Label distribution:")
    print(y.value_counts())
    
    # Encode labels
    le = LabelEncoder()
    y_encoded = le.fit_transform(y)
    
    print(f"\n   Encoded labels: {list(le.classes_)}")
    
    # Train XGBoost with regularization for small dataset
    print(f"\n🎓 Training XGBoost Classifier...")
    
    model = xgb.XGBClassifier(
        n_estimators=50,        # Small to prevent overfitting
        max_depth=3,            # Shallow trees
        learning_rate=0.1,
        min_child_weight=2,     # Regularization
        subsample=0.8,          # Row sampling
        colsample_bytree=0.8,   # Feature sampling
        reg_alpha=0.1,          # L1 regularization
        reg_lambda=1.0,         # L2 regularization
        random_state=42,
        objective='multi:softmax',
        num_class=len(le.classes_)
    )
    
    # Cross-validation (important for small datasets!)
    print(f"\n🔄 Running 5-fold cross-validation...")
    cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    cv_scores = cross_val_score(model, X, y_encoded, cv=cv, scoring='accuracy')
    
    print(f"\n📊 Cross-Validation Results:")
    print(f"   Accuracy: {cv_scores.mean():.3f} ± {cv_scores.std():.3f}")
    print(f"   Individual folds: {[f'{s:.3f}' for s in cv_scores]}")
    
    # Train on full dataset
    print(f"\n🎯 Training on full dataset...")
    model.fit(X, y_encoded)
    
    # Feature importance analysis
    feature_importance = pd.DataFrame({
        'feature': X.columns,
        'importance': model.feature_importances_
    }).sort_values('importance', ascending=False)
    
    print(f"\n🔝 Top 10 Most Important Features:")
    print(feature_importance.head(10).to_string(index=False))
    
    # Test predictions on training data
    y_pred = model.predict(X)
    y_pred_labels = le.inverse_transform(y_pred)
    
    print(f"\n📈 Training Accuracy: {(y_pred == y_encoded).mean():.3f}")
    
    # Classification report
    print(f"\n📋 Classification Report:")
    print(classification_report(y, y_pred_labels, zero_division=0))
    
    # Confusion matrix
    print(f"\n🔢 Confusion Matrix:")
    cm = confusion_matrix(y, y_pred_labels, labels=le.classes_)
    cm_df = pd.DataFrame(cm, index=le.classes_, columns=le.classes_)
    print(cm_df)
    
    # Save model
    os.makedirs(MODEL_DIR, exist_ok=True)
    
    model_path = os.path.join(MODEL_DIR, 'route_classifier.pkl')
    encoder_path = os.path.join(MODEL_DIR, 'label_encoder.pkl')
    features_path = os.path.join(MODEL_DIR, 'feature_names.json')
    
    joblib.dump(model, model_path)
    joblib.dump(le, encoder_path)
    
    # Save feature names for consistency
    with open(features_path, 'w') as f:
        json.dump(list(X.columns), f, indent=2)
    
    print(f"\n💾 Models saved:")
    print(f"   Classifier: {model_path}")
    print(f"   Encoder: {encoder_path}")
    print(f"   Features: {features_path}")
    
    print(f"\n✅ Training complete!")
    
    return model, le, X.columns.tolist()


if __name__ == "__main__":
    train_classifier()
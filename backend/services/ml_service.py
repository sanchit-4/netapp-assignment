import logging
import pandas as pd
import numpy as np
import joblib
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split

from database import SessionLocal
from models import ObjectMetadata
from services.feature_engineering import get_features_for_object

logger = logging.getLogger(__name__)
MODEL_PATH = "ml_models/model.joblib"

class MLService:
    def __init__(self):
        self.model = None
        self.load_model()

    def load_model(self):
        try:
            self.model = joblib.load(MODEL_PATH)
            logger.info("ML model loaded successfully.")
        except FileNotFoundError:
            logger.warning("Model file not found. Please train the model first.")
            self.model = None

    def generate_dummy_data(self, num_samples=1000):
        logger.info(f"Generating {num_samples} samples of dummy data...")
        data = {
            "size_bytes": np.random.randint(100, 1000000, num_samples),
            "access_count": np.random.randint(0, 500, num_samples),
            "creation_age_hours": np.random.uniform(0, 720, num_samples),
            "last_accessed_age_hours": np.random.uniform(0, 100, num_samples),
            "version": np.random.randint(1, 5, num_samples),
        }
        df = pd.DataFrame(data)
        
        # Create a plausible-looking target variable
        df['next_24h_access_count'] = (
            df['access_count'] * np.random.uniform(0.05, 0.2) +
            df['size_bytes'] / 10000 * np.random.uniform(0, 0.1) +
            np.random.randint(0, 5, num_samples)
        )
        df['next_24h_access_count'] = df['next_24h_access_count'].astype(int).clip(lower=0)
        
        logger.info("Dummy data generated.")
        return df

    def train_model(self):
        logger.info("Starting model training...")
        dummy_data = self.generate_dummy_data()
        
        X = dummy_data.drop('next_24h_access_count', axis=1)
        y = dummy_data['next_24h_access_count']
        
        X_train, _, y_train, _ = train_test_split(X, y, test_size=0.2, random_state=42)
        
        model = RandomForestRegressor(n_estimators=100, random_state=42, n_jobs=-1)
        model.fit(X_train, y_train)
        
        joblib.dump(model, MODEL_PATH)
        self.model = model
        logger.info(f"Model trained and saved to {MODEL_PATH}")

    def predict_access(self, object_id: str) -> float:
        if self.model is None:
            raise RuntimeError("ML model is not available. Please train the model first.")

        db = SessionLocal()
        try:
            metadata = db.query(ObjectMetadata).filter(ObjectMetadata.object_id == object_id).first()
            if not metadata:
                raise ValueError("Object not found")
            
            features = get_features_for_object(metadata)
            prediction = self.model.predict(features)
            return prediction[0]
        finally:
            db.close()

ml_service = MLService()

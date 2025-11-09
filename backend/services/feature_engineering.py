import datetime
import pandas as pd
from models import ObjectMetadata

def get_features_for_object(metadata: ObjectMetadata) -> pd.DataFrame:
    """
    Generates a feature DataFrame for a single object.
    """
    now = datetime.datetime.utcnow()
    
    creation_age_hours = (now - metadata.created_at).total_seconds() / 3600
    last_accessed_age_hours = (now - metadata.last_accessed_at).total_seconds() / 3600

    feature_dict = {
        "size_bytes": [metadata.size_bytes],
        "access_count": [metadata.access_count],
        "creation_age_hours": [creation_age_hours],
        "last_accessed_age_hours": [last_accessed_age_hours],
        "version": [metadata.version]
    }
    
    return pd.DataFrame(feature_dict)

from models import ObjectMetadata

# Reactive thresholds (based on actual access)
REACTIVE_HOT_TIER_THRESHOLD = 100
REACTIVE_WARM_TIER_THRESHOLD = 20

# Proactive thresholds (based on predicted access)
PROACTIVE_HOT_TIER_THRESHOLD = 50
PROACTIVE_WARM_TIER_THRESHOLD = 10


def get_tier_for_object(metadata: ObjectMetadata) -> str:
    """
    Determines the appropriate storage tier for an object based on its RECENT access count.
    This is the reactive placement engine.
    """
    if metadata.access_count > REACTIVE_HOT_TIER_THRESHOLD:
        return "onprem"
    elif REACTIVE_WARM_TIER_THRESHOLD < metadata.access_count <= REACTIVE_HOT_TIER_THRESHOLD:
        return "private"
    else:
        return "public"

def get_tier_for_predicted_access(predicted_access: float) -> str:
    """
    Determines the appropriate storage tier based on a PREDICTED access count.
    This is the proactive placement engine. Thresholds are lower to preemptively move data.
    """
    if predicted_access > PROACTIVE_HOT_TIER_THRESHOLD:
        return "onprem"
    elif PROACTIVE_WARM_TIER_THRESHOLD < predicted_access <= PROACTIVE_HOT_TIER_THRESHOLD:
        return "private"
    else:
        return "public"

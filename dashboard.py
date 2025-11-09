import streamlit as st
import requests
import pandas as pd
import plotly.express as px
import time

# --- Configuration ---
API_BASE_URL = "http://backend:8000" # Use 'backend' as hostname for Docker internal network
REFRESH_INTERVAL_SECONDS = 5

st.set_page_config(layout="wide", page_title="Data in Motion Dashboard")

# --- Helper Functions ---
@st.cache_data(ttl=REFRESH_INTERVAL_SECONDS)
def fetch_data(endpoint):
    try:
        response = requests.get(f"{API_BASE_URL}{endpoint}")
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        st.error(f"Error fetching data from {endpoint}: {e}")
        return None

def simulate_access(row):
    object_id = row["object_id"]
    try:
        response = requests.post(f"{API_BASE_URL}/api/objects/{object_id}/access")
        response.raise_for_status()
        st.success(f"Access simulated for {object_id}")
    except requests.exceptions.RequestException as e:
        st.error(f"Error simulating access for {object_id}: {e}")

def train_model():
    try:
        response = requests.post(f"{API_BASE_URL}/api/ml/train")
        response.raise_for_status()
        st.success("ML model training initiated.")
    except requests.exceptions.RequestException as e:
        st.error(f"Error initiating model training: {e}")

# --- Dashboard Layout ---
st.title("📊 Data in Motion: Intelligent Cloud Storage Dashboard")

# --- Auto-refresh mechanism ---
# This creates an empty slot that we can update to force a rerun
placeholder = st.empty()

while True:
    with placeholder.container():
        st.subheader("🚀 Key Metrics")
        metrics = fetch_data("/api/metrics")
        if metrics:
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Objects", metrics.get("total_objects", 0))
            with col2:
                st.metric("Estimated Monthly Cost", f"${metrics.get('estimated_monthly_cost', 0.0):.2f}")
            with col3:
                # Placeholder for objects migrated, as we don't track this metric yet
                st.metric("Objects Migrated (last 24h)", "N/A") 

            st.subheader("Distribution by Tier")
            tier_dist_data = metrics.get("tier_distribution", {})
            if tier_dist_data:
                df_tier = pd.DataFrame(list(tier_dist_data.items()), columns=['Tier', 'Count'])
                fig = px.pie(df_tier, values='Count', names='Tier', title='Object Distribution Across Tiers')
                st.plotly_chart(fig, use_container_width=True)

        st.subheader("📦 All Objects")
        objects_data = fetch_data("/api/objects")
        if objects_data:
            df_objects = pd.DataFrame(objects_data)
            if not df_objects.empty:
                # Convert timestamps to datetime objects for better display
                df_objects['last_accessed_at'] = pd.to_datetime(df_objects['last_accessed_at'])
                df_objects['created_at'] = pd.to_datetime(df_objects['created_at'])
                
                # Display predicted_next_24h with 2 decimal places
                df_objects['predicted_next_24h'] = df_objects['predicted_next_24h'].round(2)

                # Add action buttons
                st.dataframe(
                    df_objects.set_index('object_id'),
                    column_config={
                        "object_id": st.column_config.TextColumn("Object ID"),
                        "current_tier": st.column_config.TextColumn("Current Tier"),
                        "size_bytes": st.column_config.NumberColumn("Size (Bytes)"),
                        "access_count": st.column_config.NumberColumn("Access Count"),
                        "predicted_next_24h": st.column_config.NumberColumn("Predicted Access (24h)"),
                        "last_accessed_at": st.column_config.DatetimeColumn("Last Accessed"),
                        "created_at": st.column_config.DatetimeColumn("Created At"),
                        "simulate_access": st.column_config.ButtonColumn("Simulate Access", help="Click to simulate an access event", on_click=simulate_access),
                    },
                    hide_index=False,
                    use_container_width=True
                )
            else:
                st.info("No objects ingested yet.")
        
        st.subheader("🔄 Recent Migrations")
        migrations_data = fetch_data("/api/migrations")
        if migrations_data:
            df_migrations = pd.DataFrame(migrations_data)
            if not df_migrations.empty:
                df_migrations['timestamp'] = pd.to_datetime(df_migrations['timestamp'])
                st.dataframe(df_migrations.set_index('id'), use_container_width=True)
            else:
                st.info("No migrations recorded yet.")

        st.subheader("⚙️ ML Model Actions")
        if st.button("Train ML Model (Generates Dummy Data)"):
            train_model()

    time.sleep(REFRESH_INTERVAL_SECONDS)

import streamlit as st
import pandas as pd
import joblib

st.set_page_config(page_title="Bus Travel Time Prediction", page_icon="🚌", layout="centered")

# Custom CSS for improved look
st.markdown("""
<style>
    .main {
        background-color: #f5f7fa;
    }
    .stButton > button {
        background-color: #0072ff;
        color: white;
        font-size: 18px;
        border-radius: 8px;
        padding: 10px 24px;
        margin-top: 16px;
    }
    .stTextInput > div > input, .stNumberInput > div > input {
        border-radius: 8px;
        border: 1px solid #0072ff;
        padding: 8px;
        font-size: 16px;
    }
    .stRadio > div {
        border-radius: 8px;
        border: 1px solid #0072ff;
        padding: 8px;
        font-size: 16px;
        background-color: #eaf4ff;
    }
    .stMarkdown {
        font-size: 16px;
    }
</style>
""", unsafe_allow_html=True)

st.title("🚌 Bus Travel Time Prediction")

st.markdown("""
#### Enter segment details below:
""")

col1, col2 = st.columns([1, 1])

with col1:
    line_name = st.text_input("Route (line_name)", "DIAM_14", key="line_name_input")
    segment_distance_km = st.number_input("Segment Distance (km)", min_value=0.0, max_value=10.0, value=1.0, key="segment_distance_input")
    lat_diff = st.number_input("Latitude Difference", value=0.01, key="lat_diff_input")
    heading_ns = st.number_input("Heading NS", value=0.0, key="heading_ns_input")

with col2:
    is_timing_point = st.radio("Is Timing Point?", options=["No", "Yes"], key="timing_point_input")
    is_pickup = st.radio("Is Pickup?", options=["No", "Yes"], key="pickup_input")
    lon_diff = st.number_input("Longitude Difference", value=0.01, key="lon_diff_input")
    heading_ew = st.number_input("Heading EW", value=0.0, key="heading_ew_input")

st.markdown("---")

# Load model
try:
    model = joblib.load('travel_time_model.pkl')
except Exception as e:
    model = None
    st.error("Model file 'travel_time_model.pkl' not found or could not be loaded.")

if st.button("Predict Travel Time", key="predict_button"):
    if model:
        # Prepare input for model
        input_df = pd.DataFrame([{
            'line_name': line_name,
            'segment_distance_km': segment_distance_km,
            'is_timing_point': 1 if is_timing_point == "Yes" else 0,
            'is_pickup': 1 if is_pickup == "Yes" else 0,
            'lat_diff': lat_diff,
            'lon_diff': lon_diff,
            'heading_ns': heading_ns,
            'heading_ew': heading_ew
        }])
        try:
            prediction = model.predict(input_df)[0]
            st.success(f"Predicted Travel Time: {prediction:.2f} seconds")
        except Exception as e:
            st.error(f"Prediction failed: {e}")
    else:
        st.warning("Prediction unavailable: Model not loaded.")

st.markdown("<div style='text-align:center; color:#0072ff; font-size:16px;'>Adjust the inputs above and click <b>Predict</b> to estimate bus segment travel time.</div>", unsafe_allow_html=True)
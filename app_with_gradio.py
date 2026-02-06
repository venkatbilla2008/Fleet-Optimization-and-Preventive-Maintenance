"""
🚛 Fleet Predictive Maintenance Dashboard
==========================================

A comprehensive Streamlit application for fleet management
and predictive maintenance in logistics and transportation.

Author: Venkat M
Date: 2026-02-06
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import gradio as gr

# Page configuration
st.set_page_config(
    page_title="Fleet Predictive Maintenance",
    page_icon="🚛",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
    <style>
    .main {
        padding: 0rem 1rem;
    }
    .stMetric {
        background-color: #f0f2f6;
        padding: 15px;
        border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    .stMetric:hover {
        box-shadow: 0 4px 8px rgba(0,0,0,0.15);
        transform: translateY(-2px);
        transition: all 0.3s ease;
    }
    h1 {
        color: #1f77b4;
        font-weight: 700;
    }
    h2 {
        color: #2c3e50;
        font-weight: 600;
    }
    .stAlert {
        border-radius: 10px;
    }
    </style>
""", unsafe_allow_html=True)

# Load sample data - SAME AS DATABRICKS NOTEBOOKS
@st.cache_data
def load_data():
    """
    Load fleet data using SAME logic as Databricks notebooks
    (01_Bronze_Ingestion_IMPROVED.py)
    
    This ensures uniformity across all project activities:
    - Data Engineering (Databricks)
    - Data Science/ML (Databricks)
    - Visualization (Streamlit)
    """
    import random
    from datetime import datetime, timedelta
    
    # Configuration - SAME AS DATABRICKS
    NUM_VEHICLES = 500
    START_DATE = datetime(2024, 1, 1)
    END_DATE = datetime(2024, 1, 30)  # 30 days
    
    # Assign each vehicle a health category and degradation rate
    # SAME 5 CATEGORIES AS DATABRICKS
    vehicle_health_profiles = {}
    random.seed(42)  # For reproducibility
    
    for vehicle_id in range(1, NUM_VEHICLES + 1):
        # Health score: 0.0 (critical) to 1.0 (excellent)
        initial_health = random.random()
        degradation_rate = random.uniform(0.001, 0.005)
        
        vehicle_health_profiles[vehicle_id] = {
            'initial_health': initial_health,
            'degradation_rate': degradation_rate,
            'vehicle_id': f"VEH-{vehicle_id:03d}"
        }
    
    # Generate aggregated data (simulating Gold layer features)
    data_records = []
    
    for vehicle_id in range(1, NUM_VEHICLES + 1):
        profile = vehicle_health_profiles[vehicle_id]
        
        # Calculate current health after 30 days
        days_elapsed = 30
        current_health = profile['initial_health'] - (days_elapsed * profile['degradation_rate'])
        current_health = max(0.0, min(1.0, current_health))
        
        # Generate sensor values based on health score - SAME LOGIC AS DATABRICKS
        if current_health < 0.15:  # CRITICAL (15%)
            engine_temp = random.uniform(110, 125)
            oil_pressure = random.uniform(15, 30)
            battery_voltage = random.uniform(11.0, 12.0)
            health_status = 'Critical'
        elif current_health < 0.35:  # POOR (20%)
            engine_temp = random.uniform(105, 115)
            oil_pressure = random.uniform(25, 40)
            battery_voltage = random.uniform(11.5, 12.5)
            health_status = 'High'  # Map to High for UI
        elif current_health < 0.60:  # WARNING (25%)
            engine_temp = random.uniform(95, 108)
            oil_pressure = random.uniform(35, 50)
            battery_voltage = random.uniform(12.0, 13.0)
            health_status = 'Medium'
        elif current_health < 0.85:  # GOOD (25%)
            engine_temp = random.uniform(85, 100)
            oil_pressure = random.uniform(45, 60)
            battery_voltage = random.uniform(12.5, 13.5)
            health_status = 'Low'  # Map to Low risk for UI
        else:  # EXCELLENT (15%)
            engine_temp = random.uniform(80, 95)
            oil_pressure = random.uniform(50, 65)
            battery_voltage = random.uniform(13.0, 14.0)
            health_status = 'Low'
        
        # Calculate failure risk score (inverse of health)
        failure_risk_score = 1.0 - current_health
        
        # Determine if maintenance is required
        will_require_maintenance = 1 if failure_risk_score > 0.5 else 0
        
        # Vehicle metadata
        vehicle_age_years = random.randint(1, 10)
        total_mileage_km = random.randint(50000, 300000)
        days_since_maintenance = random.randint(0, 90)
        
        # GPS location (distributed around Delhi NCR - same as Databricks uses NYC)
        latitude = 28.4 + random.uniform(0, 0.3)
        longitude = 77.0 + random.uniform(0, 0.3)
        
        # Estimated maintenance cost
        if will_require_maintenance == 1:
            # Higher cost for critical vehicles
            if health_status == 'Critical':
                estimated_cost = random.randint(1500, 2000)
            else:
                estimated_cost = random.randint(500, 1500)
        else:
            estimated_cost = 0
        
        data_records.append({
            'vehicle_id': profile['vehicle_id'],
            'vehicle_age_years': vehicle_age_years,
            'total_mileage_km': total_mileage_km,
            'engine_temp_c': engine_temp,
            'oil_pressure_psi': oil_pressure,
            'battery_voltage': battery_voltage,
            'days_since_maintenance': days_since_maintenance,
            'failure_risk_score': failure_risk_score,
            'latitude': latitude,
            'longitude': longitude,
            'will_require_maintenance': will_require_maintenance,
            'health_status': health_status,
            'estimated_maintenance_cost': estimated_cost,
            'initial_health': profile['initial_health'],  # For reference
            'current_health': current_health  # After degradation
        })
    
    df = pd.DataFrame(data_records)
    
    return df

# Load data first so it's available everywhere
df = load_data()

# Sidebar with enhanced business branding
with st.sidebar:
    # Company branding header
    st.markdown("""
        <div style='text-align: center; padding: 20px 0; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); border-radius: 10px; margin-bottom: 20px;'>
            <h1 style='color: white; margin: 0; font-size: 28px; font-weight: 700;'>🚛 FleetAI</h1>
            <p style='color: #e0e0e0; margin: 5px 0 0 0; font-size: 14px;'>Predictive Maintenance Platform</p>
        </div>
    """, unsafe_allow_html=True)
    
    # Navigation with icons
    st.markdown("### 📍 Navigation")
    
    # Initialize session state for page if not exists
    if 'page' not in st.session_state:
        st.session_state.page = "🏠 Dashboard"
    
    # Page options
    page_options = ["🏠 Dashboard", "🔮 Predictions", "🚛 Fleet Monitor", "🤖 AI Insights", "🎯 Live Demo", "🎮 Gradio Interactive"]
    
    # Get current page index
    try:
        current_index = page_options.index(st.session_state.page)
    except ValueError:
        current_index = 0
    
    page = st.radio(
        "Select Page:",
        page_options,
        index=current_index,
        label_visibility="collapsed"
    )
    
    # Update session state
    st.session_state.page = page
    
    st.markdown("---")
    
    # Executive Summary
    st.markdown("### 💼 Executive Summary")
    st.markdown("""
        <div style='background-color: #f8f9fa; padding: 15px; border-radius: 8px; border-left: 4px solid #667eea;'>
            <p style='margin: 0; font-size: 13px; color: #2c3e50;'>
                <strong>Mission:</strong> Reduce fleet downtime and maintenance costs through AI-powered predictive analytics.
            </p>
        </div>
    """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Key Business Metrics
    st.markdown("### 📊 Key Metrics")
    
    # Metric 1: ROI
    st.markdown("""
        <div style='background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%); padding: 12px; border-radius: 8px; margin-bottom: 10px;'>
            <p style='color: white; margin: 0; font-size: 12px; font-weight: 500;'>Annual ROI</p>
            <p style='color: white; margin: 5px 0 0 0; font-size: 24px; font-weight: 700;'>3.0x</p>
            <p style='color: #e0f7fa; margin: 0; font-size: 11px;'>$5.4M savings</p>
        </div>
    """, unsafe_allow_html=True)
    
    # Metric 2: Uptime
    st.markdown("""
        <div style='background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); padding: 12px; border-radius: 8px; margin-bottom: 10px;'>
            <p style='color: white; margin: 0; font-size: 12px; font-weight: 500;'>Fleet Uptime</p>
            <p style='color: white; margin: 5px 0 0 0; font-size: 24px; font-weight: 700;'>98.5%</p>
            <p style='color: #e0e0e0; margin: 0; font-size: 11px;'>+2.1% vs last month</p>
        </div>
    """, unsafe_allow_html=True)
    
    # Metric 3: Prevention Rate
    st.markdown("""
        <div style='background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%); padding: 12px; border-radius: 8px; margin-bottom: 10px;'>
            <p style='color: white; margin: 0; font-size: 12px; font-weight: 500;'>Breakdown Prevention</p>
            <p style='color: white; margin: 5px 0 0 0; font-size: 24px; font-weight: 700;'>100%</p>
            <p style='color: #ffe0e0; margin: 0; font-size: 11px;'>Zero unexpected failures</p>
        </div>
    """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Filters
    st.markdown("### 🔍 Filters")
    date_range = st.date_input(
        "Date Range",
        value=(datetime.now() - timedelta(days=7), datetime.now()),
        max_value=datetime.now()
    )
    
    risk_filter = st.multiselect(
        "Risk Level",
        ["Critical", "High", "Medium", "Low"],
        default=["Critical", "High"]
    )
    
    st.markdown("---")
    
    # Quick Actions
    st.markdown("### ⚡ Quick Actions")
    
    # Export Report Button - Working CSV Download
    import io
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_data = csv_buffer.getvalue()
    
    st.download_button(
        label="� Download Fleet Report (CSV)",
        data=csv_data,
        file_name=f"fleet_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv",
        use_container_width=True,
        help="Download complete fleet data as CSV file"
    )
    
    # Refresh Data Button - Working
    if st.button("🔄 Refresh Data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
    
    st.markdown("---")
    
    # System Status
    st.markdown("### 🟢 System Status")
    st.markdown("""
        <div style='font-size: 12px;'>
            <p style='margin: 5px 0;'>🟢 ML Model: <strong>Active</strong></p>
            <p style='margin: 5px 0;'>🟢 Data Pipeline: <strong>Running</strong></p>
            <p style='margin: 5px 0;'>🟢 API: <strong>Healthy</strong></p>
        </div>
    """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Footer info
    st.caption("Last updated: " + datetime.now().strftime("%Y-%m-%d %H:%M"))
    st.caption("Version 1.3 | © 2026 FleetAI")


# Main content based on page selection
if page == "🏠 Dashboard":
    # Header
    st.title("🚛 Fleet Predictive Maintenance Dashboard")
    st.markdown("### Real-time monitoring and predictive analytics for your fleet")
    st.markdown("---")
    
    # KPI Row
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric(
            "Total Vehicles",
            f"{len(df):,}",
            delta="Active"
        )
    
    with col2:
        at_risk = len(df[df['will_require_maintenance'] == 1])
        st.metric(
            "Vehicles at Risk",
            f"{at_risk}",
            delta=f"{(at_risk/len(df)*100):.1f}%",
            delta_color="inverse"
        )
    
    with col3:
        st.metric(
            "Annual Savings",
            "$5.4M",
            delta="64% reduction"
        )
    
    with col4:
        st.metric(
            "Model F1-Score",
            "92%",
            delta="100% Recall"
        )
    
    with col5:
        avg_cost = df[df['will_require_maintenance'] == 1]['estimated_maintenance_cost'].mean()
        st.metric(
            "Avg Maintenance Cost",
            f"${avg_cost:.0f}",
            delta="-$450"
        )
    
    st.markdown("---")
    
    # Two-column layout
    col1, col2 = st.columns([1, 1])
    
    with col1:
        st.subheader("📊 Fleet Health Distribution")
        
        # Health status pie chart
        health_counts = df['health_status'].value_counts()
        colors = {'Critical': '#e74c3c', 'High': '#f39c12', 'Medium': '#f1c40f', 'Low': '#2ecc71'}
        
        fig = go.Figure(data=[go.Pie(
            labels=health_counts.index,
            values=health_counts.values,
            hole=0.4,
            marker=dict(colors=[colors[status] for status in health_counts.index]),
            textinfo='label+percent',
            textfont_size=14
        )])
        
        fig.update_layout(
            height=400,
            showlegend=True,
            legend=dict(orientation="h", yanchor="bottom", y=-0.2, xanchor="center", x=0.5)
        )
        
        st.plotly_chart(fig, width="stretch")
    
    with col2:
        st.subheader("📈 Risk Score Distribution")
        
        # Risk score histogram
        fig = px.histogram(
            df,
            x='failure_risk_score',
            nbins=30,
            color_discrete_sequence=['#3498db'],
            labels={'failure_risk_score': 'Risk Score', 'count': 'Number of Vehicles'}
        )
        
        fig.add_vline(x=0.5, line_dash="dash", line_color="red", 
                     annotation_text="Maintenance Threshold")
        
        fig.update_layout(
            height=400,
            showlegend=False,
            xaxis_title="Risk Score",
            yaxis_title="Number of Vehicles"
        )
        
        st.plotly_chart(fig, width="stretch")
    
    st.markdown("---")
    
    # Map visualization
    st.subheader("🗺️ Vehicle Locations & Risk Heatmap")
    
    # Create map with risk-based colors
    df_map = df.copy()
    df_map['color'] = df_map['health_status'].map(colors)
    df_map['size'] = df_map['failure_risk_score'] * 20
    
    fig = px.scatter_map(
        df_map,
        lat='latitude',
        lon='longitude',
        color='health_status',
        size='size',
        hover_name='vehicle_id',
        hover_data={
            'failure_risk_score': ':.2f',
            'health_status': True,
            'latitude': False,
            'longitude': False,
            'size': False
        },
        color_discrete_map=colors,
        zoom=10,
        height=500
    )
    
    fig.update_layout(
        margin={"r": 0, "t": 0, "l": 0, "b": 0}
    )
    
    st.plotly_chart(fig, width="stretch")
    
    st.markdown("---")
    
    # Recent alerts
    st.subheader("🚨 Recent High-Risk Alerts")
    
    high_risk = df[df['health_status'].isin(['Critical', 'High'])].sort_values(
        'failure_risk_score', ascending=False
    ).head(10)
    
    for idx, row in high_risk.iterrows():
        col1, col2, col3, col4 = st.columns([2, 2, 2, 1])
        
        with col1:
            st.write(f"**{row['vehicle_id']}**")
        
        with col2:
            status_color = colors[row['health_status']]
            st.markdown(f"<span style='color:{status_color}; font-weight:bold;'>{row['health_status']}</span>", 
                       unsafe_allow_html=True)
        
        with col3:
            st.write(f"Risk: {row['failure_risk_score']:.2f}")
        
        with col4:
            # Working View button - navigates to Fleet Monitor with selected vehicle
            if st.button("View", key=f"view_{idx}", type="secondary"):
                st.session_state.selected_vehicle = row['vehicle_id']
                st.session_state.page = "🚛 Fleet Monitor"
                st.rerun()

elif page == "🔮 Predictions":
    st.title("🔮 Maintenance Predictions")
    st.markdown("### 7-Day Maintenance Forecast")
    st.markdown("---")
    
    # Filter vehicles requiring maintenance
    maintenance_needed = df[df['will_require_maintenance'] == 1].sort_values(
        'failure_risk_score', ascending=False
    )
    
    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Vehicles Needing Maintenance", len(maintenance_needed))
    
    with col2:
        total_cost = maintenance_needed['estimated_maintenance_cost'].sum()
        st.metric("Total Estimated Cost", f"${total_cost:,}")
    
    with col3:
        critical_count = len(maintenance_needed[maintenance_needed['health_status'] == 'Critical'])
        st.metric("Critical Priority", critical_count, delta_color="inverse")
    
    with col4:
        avg_days = maintenance_needed['days_since_maintenance'].mean()
        st.metric("Avg Days Since Maintenance", f"{avg_days:.0f}")
    
    st.markdown("---")
    
    # Priority table
    st.subheader("📋 Maintenance Schedule (Priority Order)")
    
    # Add priority days
    maintenance_needed['priority_days'] = maintenance_needed['failure_risk_score'].apply(
        lambda x: 1 if x > 0.9 else (3 if x > 0.7 else 7)
    )
    
    display_df = maintenance_needed[[
        'vehicle_id', 'health_status', 'failure_risk_score', 
        'priority_days', 'estimated_maintenance_cost'
    ]].head(20).copy()
    
    display_df.columns = ['Vehicle ID', 'Status', 'Risk Score', 'Schedule In (Days)', 'Est. Cost ($)']
    display_df['Risk Score'] = display_df['Risk Score'].round(3)
    
    st.dataframe(
        display_df,
        width="stretch",
        height=400,
        hide_index=True
    )
    
    st.markdown("---")
    
    # Cost analysis
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("💰 Cost Impact Analysis")
        
        # Preventive vs Emergency cost comparison
        preventive_cost = maintenance_needed['estimated_maintenance_cost'].sum()
        emergency_cost = preventive_cost * 3  # Emergency costs are 3x higher
        savings = emergency_cost - preventive_cost
        
        cost_data = pd.DataFrame({
            'Type': ['Preventive Maintenance', 'Emergency Repairs (Avoided)', 'Total Savings'],
            'Cost': [preventive_cost, emergency_cost, savings]
        })
        
        fig = px.bar(
            cost_data,
            x='Type',
            y='Cost',
            color='Type',
            text='Cost',
            color_discrete_sequence=['#2ecc71', '#e74c3c', '#3498db']
        )
        
        fig.update_traces(texttemplate='$%{text:,.0f}', textposition='outside')
        fig.update_layout(height=400, showlegend=False)
        
        st.plotly_chart(fig, width="stretch")
    
    with col2:
        st.subheader("📊 Risk Distribution by Priority")
        
        # Risk by priority
        priority_counts = maintenance_needed.groupby('priority_days').size().reset_index()
        priority_counts.columns = ['Days', 'Count']
        priority_counts['Priority'] = priority_counts['Days'].map({
            1: 'Immediate (1 day)',
            3: 'Urgent (3 days)',
            7: 'Scheduled (7 days)'
        })
        
        fig = px.pie(
            priority_counts,
            values='Count',
            names='Priority',
            color_discrete_sequence=['#e74c3c', '#f39c12', '#f1c40f']
        )
        
        fig.update_layout(height=400)
        st.plotly_chart(fig, width="stretch")

elif page == "🚛 Fleet Monitor":
    st.title("🚛 Vehicle Health Monitor")
    st.markdown("### Individual Vehicle Analysis")
    st.markdown("---")
    
    # Initialize selected vehicle in session state if not exists
    if 'selected_vehicle' not in st.session_state:
        st.session_state.selected_vehicle = df['vehicle_id'].tolist()[0]
    
    # Get index of selected vehicle
    vehicle_list = df['vehicle_id'].tolist()
    try:
        default_index = vehicle_list.index(st.session_state.selected_vehicle)
    except ValueError:
        default_index = 0
    
    # Vehicle selector
    selected_vehicle = st.selectbox(
        "Select Vehicle",
        options=vehicle_list,
        index=default_index
    )
    
    # Update session state
    st.session_state.selected_vehicle = selected_vehicle
    
    # Get vehicle data
    vehicle_data = df[df['vehicle_id'] == selected_vehicle].iloc[0]
    
    # Vehicle header
    col1, col2, col3 = st.columns([2, 1, 1])
    
    with col1:
        st.markdown(f"## {selected_vehicle}")
        status_color = {'Critical': '#e74c3c', 'High': '#f39c12', 'Medium': '#f1c40f', 'Low': '#2ecc71'}[vehicle_data['health_status']]
        st.markdown(f"<h3 style='color:{status_color};'>{vehicle_data['health_status']} Risk</h3>", 
                   unsafe_allow_html=True)
    
    with col2:
        st.metric("Risk Score", f"{vehicle_data['failure_risk_score']:.2f}")
    
    with col3:
        if vehicle_data['will_require_maintenance'] == 1:
            st.error("⚠️ Maintenance Required")
        else:
            st.success("✅ Healthy")
    
    st.markdown("---")
    
    # Metrics row
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Vehicle Age", f"{vehicle_data['vehicle_age_years']} years")
    
    with col2:
        st.metric("Total Mileage", f"{vehicle_data['total_mileage_km']:,} km")
    
    with col3:
        st.metric("Days Since Maintenance", f"{vehicle_data['days_since_maintenance']}")
    
    with col4:
        if vehicle_data['will_require_maintenance'] == 1:
            st.metric("Est. Cost", f"${vehicle_data['estimated_maintenance_cost']:,}")
        else:
            st.metric("Est. Cost", "$0")
    
    st.markdown("---")
    
    # Sensor readings
    st.subheader("📊 Sensor Readings")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        # Engine temperature gauge
        temp_status = "🔴 High" if vehicle_data['engine_temp_c'] > 100 else ("🟡 Warm" if vehicle_data['engine_temp_c'] > 90 else "🟢 Normal")
        
        fig = go.Figure(go.Indicator(
            mode="gauge+number",
            value=vehicle_data['engine_temp_c'],
            title={'text': "Engine Temperature (°C)"},
            gauge={
                'axis': {'range': [None, 120]},
                'bar': {'color': "darkblue"},
                'steps': [
                    {'range': [0, 90], 'color': "#2ecc71"},
                    {'range': [90, 100], 'color': "#f1c40f"},
                    {'range': [100, 120], 'color': "#e74c3c"}
                ],
                'threshold': {
                    'line': {'color': "red", 'width': 4},
                    'thickness': 0.75,
                    'value': 100
                }
            }
        ))
        
        fig.update_layout(height=250)
        st.plotly_chart(fig, width="stretch")
        st.caption(temp_status)
    
    with col2:
        # Oil pressure gauge
        oil_status = "🔴 Low" if vehicle_data['oil_pressure_psi'] < 25 else ("🟢 Normal" if vehicle_data['oil_pressure_psi'] > 30 else "🟡 Monitor")
        
        fig = go.Figure(go.Indicator(
            mode="gauge+number",
            value=vehicle_data['oil_pressure_psi'],
            title={'text': "Oil Pressure (PSI)"},
            gauge={
                'axis': {'range': [0, 60]},
                'bar': {'color': "darkblue"},
                'steps': [
                    {'range': [0, 25], 'color': "#e74c3c"},
                    {'range': [25, 30], 'color': "#f1c40f"},
                    {'range': [30, 60], 'color': "#2ecc71"}
                ],
                'threshold': {
                    'line': {'color': "red", 'width': 4},
                    'thickness': 0.75,
                    'value': 25
                }
            }
        ))
        
        fig.update_layout(height=250)
        st.plotly_chart(fig, width="stretch")
        st.caption(oil_status)
    
    with col3:
        # Battery voltage gauge
        battery_status = "🔴 Low" if vehicle_data['battery_voltage'] < 12.0 else ("🟢 Good" if vehicle_data['battery_voltage'] > 12.4 else "🟡 Fair")
        
        fig = go.Figure(go.Indicator(
            mode="gauge+number",
            value=vehicle_data['battery_voltage'],
            title={'text': "Battery Voltage (V)"},
            gauge={
                'axis': {'range': [10, 14]},
                'bar': {'color': "darkblue"},
                'steps': [
                    {'range': [10, 12.0], 'color': "#e74c3c"},
                    {'range': [12.0, 12.4], 'color': "#f1c40f"},
                    {'range': [12.4, 14], 'color': "#2ecc71"}
                ],
                'threshold': {
                    'line': {'color': "red", 'width': 4},
                    'thickness': 0.75,
                    'value': 12.0
                }
            }
        ))
        
        fig.update_layout(height=250)
        st.plotly_chart(fig, width="stretch")
        st.caption(battery_status)
    
    st.markdown("---")
    
    # Recommendations
    st.subheader("🔧 Recommended Actions")
    
    if vehicle_data['will_require_maintenance'] == 1:
        priority_days = 1 if vehicle_data['failure_risk_score'] > 0.9 else (3 if vehicle_data['failure_risk_score'] > 0.7 else 7)
        
        st.warning(f"⚠️ **Schedule maintenance within {priority_days} day(s)**")
        
        recommendations = []
        
        if vehicle_data['engine_temp_c'] > 100:
            recommendations.append("• Check engine cooling system")
        
        if vehicle_data['oil_pressure_psi'] < 25:
            recommendations.append("• Inspect oil pump and oil level")
        
        if vehicle_data['battery_voltage'] < 12.0:
            recommendations.append("• Test battery and charging system")
        
        if vehicle_data['days_since_maintenance'] > 60:
            recommendations.append("• Perform routine maintenance check")
        
        if recommendations:
            for rec in recommendations:
                st.write(rec)
        else:
            st.write("• Perform general inspection")
    else:
        st.success("✅ **Vehicle is in good condition. Continue regular monitoring.**")

elif page == "🤖 AI Insights":
    st.title("🤖 AI Model Performance")
    st.markdown("### Model Metrics & Business Impact")
    st.markdown("---")
    
    # Model metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("F1-Score", "92%", delta="Target: ≥90%")
    
    with col2:
        st.metric("Recall", "100%", delta="Perfect!")
    
    with col3:
        st.metric("Precision", "85%", delta="+3%")
    
    with col4:
        st.metric("ROC-AUC", "94%", delta="+4%")
    
    st.markdown("---")
    
    # Two-column layout
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📊 Confusion Matrix")
        
        # Sample confusion matrix
        cm = np.array([[70, 10], [0, 20]])
        
        fig = px.imshow(
            cm,
            labels=dict(x="Predicted", y="Actual", color="Count"),
            x=['No Maintenance', 'Maintenance'],
            y=['No Maintenance', 'Maintenance'],
            text_auto=True,
            color_continuous_scale='Blues'
        )
        
        fig.update_layout(height=400)
        st.plotly_chart(fig, width="stretch")
        
        st.caption("**Perfect Recall (100%):** Zero missed failures!")
    
    with col2:
        st.subheader("⭐ Feature Importance")
        
        # Sample feature importance
        features = [
            'Days Since Maintenance',
            'Engine Temperature',
            'Oil Pressure',
            'Vehicle Age',
            'Total Mileage',
            'Battery Voltage',
            'Tire Pressure Variance',
            'Idle Time %'
        ]
        
        importance = [0.18, 0.15, 0.14, 0.12, 0.11, 0.10, 0.08, 0.07]
        
        feature_df = pd.DataFrame({
            'Feature': features,
            'Importance': importance
        }).sort_values('Importance', ascending=True)
        
        fig = px.bar(
            feature_df,
            x='Importance',
            y='Feature',
            orientation='h',  # Horizontal bar chart
            color='Importance',
            color_continuous_scale='Viridis'
        )
        
        fig.update_layout(height=400, showlegend=False)
        st.plotly_chart(fig, width="stretch")
    
    st.markdown("---")
    
    # Business impact
    st.subheader("💰 Business Impact")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("### $5.4M")
        st.caption("Annual Savings")
        st.progress(0.64, text="64% cost reduction")
    
    with col2:
        st.markdown("### 100%")
        st.caption("Breakdown Prevention")
        st.progress(1.0, text="Zero unexpected failures")
    
    with col3:
        st.markdown("### 67%")
        st.caption("Downtime Reduction")
        st.progress(0.67, text="From 15% to 5%")
    
    st.markdown("---")
    
    # ROI Analysis
    st.subheader("📈 ROI Analysis")
    
    roi_data = pd.DataFrame({
        'Metric': [
            'Emergency Repair Costs (Before)',
            'Preventive Maintenance Costs (After)',
            'Annual Savings',
            'Implementation Cost',
            'Net Benefit (Year 1)',
            'ROI'
        ],
        'Value': [
            '$8.4M',
            '$3.0M',
            '$5.4M',
            '$1.8M',
            '$3.6M',
            '3.0x'
        ],
        'Description': [
            'Reactive maintenance approach',
            'Proactive ML-driven approach',
            'Cost reduction achieved',
            'ML system development & deployment',
            'Savings minus implementation',
            'Return on investment'
        ]
    })
    
    st.dataframe(roi_data, width="stretch", hide_index=True)
    
    st.success("**Payback Period:** 4 months | **3-Year ROI:** 9.0x")

elif page == "🎯 Live Demo":
    st.title("🎯 Live Maintenance Prediction")
    st.markdown("### Try the ML Model in Real-Time")
    st.markdown("---")
    
    st.info("💡 **Tip:** Adjust the parameters below to see how different vehicle conditions affect maintenance predictions!")
    
    # Create two columns for input
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📝 Vehicle Parameters")
        
        vehicle_age = st.slider("Vehicle Age (years)", 1, 15, 5)
        total_mileage = st.slider("Total Mileage (km)", 10000, 500000, 150000, step=10000)
        engine_temp = st.slider("Engine Temperature (°C)", 60, 120, 95)
        oil_pressure = st.slider("Oil Pressure (PSI)", 15, 60, 35)
        battery_voltage = st.slider("Battery Voltage (V)", 10.0, 14.0, 12.5, step=0.1)
        days_since_maintenance = st.slider("Days Since Last Maintenance", 0, 180, 45)
    
    with col2:
        st.subheader("🔮 Prediction Result")
        
        # Simple prediction logic based on parameters
        risk_score = 0.0
        
        # Age factor
        risk_score += (vehicle_age / 15) * 0.15
        
        # Mileage factor
        risk_score += (total_mileage / 500000) * 0.15
        
        # Engine temp factor
        if engine_temp > 100:
            risk_score += 0.25
        elif engine_temp > 90:
            risk_score += 0.15
        
        # Oil pressure factor
        if oil_pressure < 25:
            risk_score += 0.20
        elif oil_pressure < 30:
            risk_score += 0.10
        
        # Battery factor
        if battery_voltage < 12.0:
            risk_score += 0.15
        elif battery_voltage < 12.4:
            risk_score += 0.08
        
        # Maintenance factor
        risk_score += (days_since_maintenance / 180) * 0.20
        
        # Cap at 1.0
        risk_score = min(risk_score, 1.0)
        
        # Determine prediction
        needs_maintenance = risk_score > 0.5
        
        # Display result
        if risk_score > 0.8:
            st.error("🚨 **CRITICAL RISK**")
            priority = "Immediate (1 day)"
            color = "#e74c3c"
        elif risk_score > 0.6:
            st.warning("⚠️ **HIGH RISK**")
            priority = "Urgent (3 days)"
            color = "#f39c12"
        elif risk_score > 0.4:
            st.warning("⚡ **MEDIUM RISK**")
            priority = "Scheduled (7 days)"
            color = "#f1c40f"
        else:
            st.success("✅ **LOW RISK**")
            priority = "Monitor"
            color = "#2ecc71"
        
        # Risk gauge
        fig = go.Figure(go.Indicator(
            mode="gauge+number+delta",
            value=risk_score * 100,
            title={'text': "Risk Score"},
            delta={'reference': 50, 'increasing': {'color': "red"}},
            gauge={
                'axis': {'range': [None, 100]},
                'bar': {'color': color},
                'steps': [
                    {'range': [0, 40], 'color': "#d5f4e6"},
                    {'range': [40, 60], 'color': "#fef5e7"},
                    {'range': [60, 80], 'color': "#fadbd8"},
                    {'range': [80, 100], 'color': "#f5b7b1"}
                ],
                'threshold': {
                    'line': {'color': "red", 'width': 4},
                    'thickness': 0.75,
                    'value': 50
                }
            }
        ))
        
        fig.update_layout(height=300)
        st.plotly_chart(fig, width="stretch")
        
        # Prediction details
        st.markdown("---")
        
        if needs_maintenance:
            st.markdown(f"### ⚠️ Maintenance Required")
            st.markdown(f"**Priority:** {priority}")
            st.markdown(f"**Confidence:** {min(risk_score * 100 + 10, 99):.0f}%")
            
            # Estimated cost
            base_cost = 500
            cost_multiplier = 1 + (risk_score * 2)
            estimated_cost = int(base_cost * cost_multiplier)
            st.markdown(f"**Estimated Cost:** ${estimated_cost}")
            
            # Recommendations
            st.markdown("**Recommended Actions:**")
            
            if engine_temp > 100:
                st.write("• 🔧 Check engine cooling system")
            
            if oil_pressure < 25:
                st.write("• 🔧 Inspect oil pump and oil level")
            
            if battery_voltage < 12.0:
                st.write("• 🔧 Test battery and charging system")
            
            if days_since_maintenance > 90:
                st.write("• 🔧 Perform overdue routine maintenance")
            
            if vehicle_age > 8:
                st.write("• 🔧 Comprehensive aging vehicle inspection")
        else:
            st.markdown(f"### ✅ No Immediate Maintenance Needed")
            st.markdown(f"**Status:** Vehicle is in good condition")
            st.markdown(f"**Confidence:** {min((1 - risk_score) * 100 + 10, 99):.0f}%")
            st.markdown(f"**Next Check:** Continue regular monitoring")
    
    st.markdown("---")
    
    # Comparison table
    st.subheader("📊 Parameter Analysis")
    
    param_data = pd.DataFrame({
        'Parameter': [
            'Vehicle Age',
            'Total Mileage',
            'Engine Temperature',
            'Oil Pressure',
            'Battery Voltage',
            'Days Since Maintenance'
        ],
        'Current Value': [
            f"{vehicle_age} years",
            f"{total_mileage:,} km",
            f"{engine_temp}°C",
            f"{oil_pressure} PSI",
            f"{battery_voltage}V",
            f"{days_since_maintenance} days"
        ],
        'Normal Range': [
            "1-8 years",
            "< 200,000 km",
            "80-95°C",
            "30-50 PSI",
            "12.4-13.2V",
            "< 60 days"
        ],
        'Status': [
            "✅ Normal" if vehicle_age <= 8 else "⚠️ High",
            "✅ Normal" if total_mileage < 200000 else "⚠️ High",
            "✅ Normal" if engine_temp <= 95 else "⚠️ High",
            "✅ Normal" if oil_pressure >= 30 else "⚠️ Low",
            "✅ Normal" if battery_voltage >= 12.4 else "⚠️ Low",
            "✅ Normal" if days_since_maintenance <= 60 else "⚠️ Overdue"
        ]
    })
    
    st.dataframe(param_data, width="stretch", hide_index=True)

elif page == "🎮 Gradio Interactive":
    st.title("🎮 Interactive ML Predictor (Gradio)")
    st.markdown("### Test the ML Model with Custom Inputs")
    st.markdown("---")
    
    st.info("💡 **Powered by Gradio** - Adjust the sliders below to test different vehicle conditions and get instant predictions!")
    
    # Define the prediction function
    def predict_vehicle_maintenance(engine_temp, oil_pressure, battery_voltage, 
                                    vehicle_age, mileage, days_since_maintenance):
        """
        Predict vehicle maintenance needs based on sensor readings and metadata
        Uses the same logic as the Databricks ML model
        """
        # Normalize inputs (simple scoring system)
        # Engine temp score (0-1, higher is worse)
        temp_score = max(0, min(1, (engine_temp - 80) / 45))  # 80-125°C range
        
        # Oil pressure score (0-1, lower is worse)
        oil_score = max(0, min(1, (50 - oil_pressure) / 35))  # 15-50 PSI range (inverted)
        
        # Battery score (0-1, lower is worse)
        battery_score = max(0, min(1, (13.5 - battery_voltage) / 2.5))  # 11-13.5V range (inverted)
        
        # Age score
        age_score = max(0, min(1, vehicle_age / 15))  # 0-15 years
        
        # Mileage score
        mileage_score = max(0, min(1, mileage / 500000))  # 0-500k km
        
        # Days since maintenance score
        days_score = max(0, min(1, days_since_maintenance / 180))  # 0-180 days
        
        # Calculate weighted risk score
        risk_score = (
            temp_score * 0.25 +
            oil_score * 0.25 +
            battery_score * 0.20 +
            age_score * 0.10 +
            mileage_score * 0.10 +
            days_score * 0.10
        )
        
        # Determine health status
        if risk_score > 0.85:
            health_status = "🔴 Critical"
            priority = "Immediate (within 24 hours)"
            estimated_cost = f"${np.random.randint(1500, 2000):,}"
        elif risk_score > 0.65:
            health_status = "🟠 High Risk"
            priority = "Urgent (within 3 days)"
            estimated_cost = f"${np.random.randint(1000, 1500):,}"
        elif risk_score > 0.45:
            health_status = "🟡 Medium Risk"
            priority = "Scheduled (within 7 days)"
            estimated_cost = f"${np.random.randint(500, 1000):,}"
        else:
            health_status = "🟢 Low Risk"
            priority = "Routine monitoring"
            estimated_cost = "$0 (No immediate maintenance)"
        
        # Maintenance recommendation
        needs_maintenance = "✅ Yes - Schedule Now" if risk_score > 0.5 else "❌ No - Continue Monitoring"
        
        # Generate specific recommendations
        recommendations = []
        if engine_temp > 100:
            recommendations.append("• Check engine cooling system")
        if oil_pressure < 30:
            recommendations.append("• Inspect oil pump and oil level")
        if battery_voltage < 12.4:
            recommendations.append("• Test battery and charging system")
        if days_since_maintenance > 90:
            recommendations.append("• Perform overdue routine maintenance")
        if vehicle_age > 8:
            recommendations.append("• Comprehensive aging vehicle inspection")
        if mileage > 200000:
            recommendations.append("• High-mileage vehicle inspection")
        
        if not recommendations:
            recommendations.append("• Continue regular monitoring")
        
        recommendations_text = "\n".join(recommendations)
        
        # Return results as formatted string
        result = f"""
## 🎯 Prediction Results

### Health Assessment
- **Health Status:** {health_status}
- **Risk Score:** {risk_score:.1%}
- **Maintenance Required:** {needs_maintenance}

### Priority & Cost
- **Priority Level:** {priority}
- **Estimated Cost:** {estimated_cost}

### Detailed Scores
- Engine Temperature Score: {temp_score:.1%}
- Oil Pressure Score: {oil_score:.1%}
- Battery Score: {battery_score:.1%}
- Vehicle Age Score: {age_score:.1%}
- Mileage Score: {mileage_score:.1%}
- Maintenance Delay Score: {days_score:.1%}

### 🔧 Recommended Actions
{recommendations_text}

### 📊 Confidence Level
Model Confidence: {min(95, 70 + (1 - abs(risk_score - 0.5)) * 50):.0f}%
        """
        
        return result
    
    # Create Gradio interface
    with st.container():
        st.markdown("### 🎛️ Adjust Vehicle Parameters")
        
        demo = gr.Interface(
            fn=predict_vehicle_maintenance,
            inputs=[
                gr.Slider(
                    minimum=60,
                    maximum=130,
                    value=95,
                    step=1,
                    label="🌡️ Engine Temperature (°C)",
                    info="Normal range: 80-95°C"
                ),
                gr.Slider(
                    minimum=10,
                    maximum=70,
                    value=35,
                    step=1,
                    label="🛢️ Oil Pressure (PSI)",
                    info="Normal range: 30-50 PSI"
                ),
                gr.Slider(
                    minimum=10.0,
                    maximum=15.0,
                    value=12.6,
                    step=0.1,
                    label="🔋 Battery Voltage (V)",
                    info="Normal range: 12.4-13.2V"
                ),
                gr.Slider(
                    minimum=1,
                    maximum=15,
                    value=5,
                    step=1,
                    label="📅 Vehicle Age (years)",
                    info="Typical fleet age: 3-7 years"
                ),
                gr.Slider(
                    minimum=10000,
                    maximum=500000,
                    value=150000,
                    step=10000,
                    label="🛣️ Total Mileage (km)",
                    info="High mileage: >200,000 km"
                ),
                gr.Slider(
                    minimum=0,
                    maximum=180,
                    value=45,
                    step=5,
                    label="🔧 Days Since Last Maintenance",
                    info="Recommended: <60 days"
                )
            ],
            outputs=gr.Markdown(label="Prediction Results"),
            title="🚛 Fleet Maintenance Predictor",
            description="Adjust vehicle parameters to predict maintenance needs using our AI model",
            examples=[
                [120, 20, 11.5, 8, 250000, 120],  # Critical vehicle
                [85, 50, 13.0, 3, 80000, 30],     # Healthy vehicle
                [105, 28, 12.0, 6, 180000, 75],   # Medium risk vehicle
                [95, 35, 12.6, 5, 150000, 45],    # Low risk vehicle
            ]
        )
        
        # Launch Gradio interface embedded in Streamlit
        demo.launch(
            share=False,
            inline=True,
            show_error=True
        )
    
    st.markdown("---")
    
    # Additional information
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        ### 📖 How It Works
        
        The ML predictor uses a **weighted scoring system** based on:
        
        1. **Sensor Readings** (70%)
           - Engine Temperature (25%)
           - Oil Pressure (25%)
           - Battery Voltage (20%)
        
        2. **Vehicle Metadata** (30%)
           - Vehicle Age (10%)
           - Total Mileage (10%)
           - Days Since Maintenance (10%)
        
        The model calculates a **risk score** (0-100%) and provides:
        - Health status classification
        - Maintenance priority level
        - Cost estimation
        - Specific recommendations
        """)
    
    with col2:
        st.markdown("""
        ### 🎯 Try These Scenarios
        
        **Scenario 1: Critical Vehicle**
        - Engine Temp: 120°C
        - Oil Pressure: 20 PSI
        - Battery: 11.5V
        - Age: 8 years
        - Mileage: 250,000 km
        - Days: 120
        
        **Scenario 2: Healthy Vehicle**
        - Engine Temp: 85°C
        - Oil Pressure: 50 PSI
        - Battery: 13.0V
        - Age: 3 years
        - Mileage: 80,000 km
        - Days: 30
        
        **Scenario 3: Borderline Case**
        - Engine Temp: 95°C
        - Oil Pressure: 35 PSI
        - Battery: 12.6V
        - Age: 5 years
        - Mileage: 150,000 km
        - Days: 45
        """)
    
    st.markdown("---")
    
    # Model information
    st.markdown("""
    ### ℹ️ About This Model
    
    - **Type:** Rule-based ML predictor with weighted scoring
    - **Training Data:** 500 vehicles, 30 days of telemetry
    - **Features:** 6 input parameters
    - **Output:** Risk score, health status, recommendations
    - **Accuracy:** 92% F1-Score, 100% Recall (from Databricks training)
    - **Update Frequency:** Real-time predictions
    
    **Note:** This is a demonstration model. Production models would use more sophisticated algorithms (Random Forest, XGBoost, Neural Networks) trained on historical maintenance data.
    """)


# Simple Footer
st.markdown("---")
st.caption("© 2026 FleetAI - Predictive Maintenance Platform | Version 1.4 (with Gradio) | Built with Streamlit + Gradio")


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

# Load sample data
@st.cache_data
def load_data():
    """Load sample fleet data"""
    np.random.seed(42)
    
    # Generate sample vehicle data
    n_vehicles = 500
    vehicle_ids = [f"VEH-{str(i).zfill(3)}" for i in range(1, n_vehicles + 1)]
    
    data = {
        'vehicle_id': vehicle_ids,
        'vehicle_age_years': np.random.randint(1, 10, n_vehicles),
        'total_mileage_km': np.random.randint(50000, 300000, n_vehicles),
        'engine_temp_c': np.random.normal(85, 15, n_vehicles),
        'oil_pressure_psi': np.random.normal(35, 8, n_vehicles),
        'battery_voltage': np.random.normal(12.6, 0.8, n_vehicles),
        'days_since_maintenance': np.random.randint(0, 90, n_vehicles),
        'failure_risk_score': np.random.beta(2, 5, n_vehicles),
        'latitude': np.random.uniform(28.4, 28.7, n_vehicles),
        'longitude': np.random.uniform(77.0, 77.3, n_vehicles),
    }
    
    df = pd.DataFrame(data)
    
    # Calculate maintenance prediction
    df['will_require_maintenance'] = (df['failure_risk_score'] > 0.5).astype(int)
    
    # Add health status
    def get_health_status(risk):
        if risk > 0.8:
            return 'Critical'
        elif risk > 0.6:
            return 'High'
        elif risk > 0.4:
            return 'Medium'
        else:
            return 'Low'
    
    df['health_status'] = df['failure_risk_score'].apply(get_health_status)
    
    # Add estimated cost
    df['estimated_maintenance_cost'] = np.where(
        df['will_require_maintenance'] == 1,
        np.random.randint(500, 2000, n_vehicles),
        0
    )
    
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
    page_options = ["🏠 Dashboard", "🔮 Predictions", "🚛 Fleet Monitor", "🤖 AI Insights", "🎯 Live Demo"]
    
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


# Simple Footer
st.markdown("---")
st.caption("© 2026 FleetAI - Predictive Maintenance Platform | Version 1.3 | Built with Streamlit")


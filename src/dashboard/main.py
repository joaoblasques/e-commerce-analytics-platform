"""
Main Streamlit application for E-Commerce Analytics Platform Dashboard.

This module provides the main entry point for the Streamlit dashboard with
multi-page navigation, authentication, and real-time data refresh capabilities.
"""

import asyncio
import sys
from pathlib import Path

import streamlit as st

# Add the src directory to the Python path
src_path = Path(__file__).parent.parent
sys.path.insert(0, str(src_path))

from dashboard.components.sidebar import render_sidebar
from dashboard.config.settings import DashboardSettings
from dashboard.pages import (
    customer_analytics,
    executive_dashboard,
    fraud_detection,
    real_time_monitoring,
    revenue_analytics,
)
from dashboard.utils.api_client import APIClient
from dashboard.utils.auth import check_authentication

# Configure Streamlit page
st.set_page_config(
    page_title="E-Commerce Analytics Platform",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        "Get Help": "https://github.com/your-repo/ecap",
        "Report a bug": "https://github.com/your-repo/ecap/issues",
        "About": "E-Commerce Analytics Platform - Comprehensive business intelligence dashboard",
    },
)


# Load custom CSS
def load_css():
    """Load custom CSS styling."""
    css_path = Path(__file__).parent / "assets" / "styles.css"
    if css_path.exists():
        with open(css_path) as f:
            st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)


def initialize_session_state():
    """Initialize Streamlit session state variables."""
    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False
    if "api_client" not in st.session_state:
        st.session_state.api_client = None
    if "current_page" not in st.session_state:
        st.session_state.current_page = "Executive Dashboard"
    if "auto_refresh" not in st.session_state:
        st.session_state.auto_refresh = False
    if "refresh_interval" not in st.session_state:
        st.session_state.refresh_interval = 30
    if "last_refresh" not in st.session_state:
        st.session_state.last_refresh = None


def main():
    """Main application function."""
    # Load settings
    settings = DashboardSettings()

    # Initialize session state
    initialize_session_state()

    # Load custom CSS
    load_css()

    # Check authentication
    if not check_authentication():
        return

    # Initialize API client if not already done
    if st.session_state.api_client is None:
        st.session_state.api_client = APIClient(
            base_url=settings.api_base_url, timeout=settings.api_timeout
        )

    # Render sidebar navigation
    selected_page = render_sidebar()
    st.session_state.current_page = selected_page

    # Main content area
    st.title("📊 E-Commerce Analytics Platform")

    # Add refresh controls in the header
    col1, col2, col3, col4 = st.columns([3, 1, 1, 1])

    with col2:
        if st.button("🔄 Refresh", help="Refresh current page data"):
            st.rerun()

    with col3:
        auto_refresh = st.checkbox(
            "Auto-refresh",
            value=st.session_state.auto_refresh,
            help="Automatically refresh data",
        )
        st.session_state.auto_refresh = auto_refresh

    with col4:
        if st.session_state.auto_refresh:
            refresh_interval = st.selectbox(
                "Interval (s)",
                options=[10, 30, 60, 300],
                index=1,
                help="Auto-refresh interval in seconds",
            )
            st.session_state.refresh_interval = refresh_interval

    # Auto-refresh logic
    if st.session_state.auto_refresh:
        import time

        if (
            st.session_state.last_refresh is None
            or time.time() - st.session_state.last_refresh
            > st.session_state.refresh_interval
        ):
            st.session_state.last_refresh = time.time()
            st.rerun()

    # Route to selected page
    try:
        if selected_page == "Executive Dashboard":
            executive_dashboard.render()
        elif selected_page == "Customer Analytics":
            customer_analytics.render()
        elif selected_page == "Revenue Analytics":
            revenue_analytics.render()
        elif selected_page == "Fraud Detection":
            fraud_detection.render()
        elif selected_page == "Real-time Monitoring":
            real_time_monitoring.render()
        else:
            st.error(f"Unknown page: {selected_page}")

    except Exception as e:
        st.error(f"Error loading page: {str(e)}")
        st.exception(e)

    # Footer
    st.markdown("---")
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        st.markdown(
            "<div style='text-align: center; color: #666;'>"
            "E-Commerce Analytics Platform v1.0.0 | "
            f"API: {settings.api_base_url} | "
            f"Last updated: {st.session_state.last_refresh or 'Never'}"
            "</div>",
            unsafe_allow_html=True,
        )


if __name__ == "__main__":
    main()

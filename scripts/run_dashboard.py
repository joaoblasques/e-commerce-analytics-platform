#!/usr/bin/env python3
"""
Dashboard startup script for E-Commerce Analytics Platform.

This script provides a convenient way to start the Streamlit dashboard
with proper configuration and environment setup.
"""

import os
import subprocess
import sys
from pathlib import Path


def setup_environment():
    """Setup the Python path and environment variables."""
    # Add src directory to Python path
    project_root = Path(__file__).parent.parent
    src_path = project_root / "src"

    if str(src_path) not in sys.path:
        sys.path.insert(0, str(src_path))

    # Set environment variables for dashboard
    os.environ.setdefault("DASHBOARD_API_BASE_URL", "http://localhost:8000")
    os.environ.setdefault("DASHBOARD_AUTH_ENABLED", "true")
    os.environ.setdefault("DASHBOARD_USERNAME", "admin")
    os.environ.setdefault("DASHBOARD_PASSWORD", "admin123")


def check_api_server():
    """Check if the API server is running."""
    import requests

    api_url = os.getenv("DASHBOARD_API_BASE_URL", "http://localhost:8000")

    try:
        response = requests.get(f"{api_url}/health", timeout=5)
        if response.status_code == 200:
            print(f"✅ API server is running at {api_url}")
            return True
        else:
            print(f"⚠️  API server returned status {response.status_code}")
            return False
    except requests.exceptions.ConnectionError:
        print(f"❌ API server is not reachable at {api_url}")
        print("   Please ensure the FastAPI server is running:")
        print(
            "   poetry run uvicorn src.api.main:app --reload --host 0.0.0.0 --port 8000"
        )
        return False
    except Exception as e:
        print(f"❌ Error checking API server: {e}")
        return False


def main():
    """Main function to start the dashboard."""
    print("🚀 Starting E-Commerce Analytics Platform Dashboard...")

    # Setup environment
    setup_environment()

    # Check API server (optional - dashboard can run without it for demo)
    api_running = check_api_server()
    if not api_running:
        print("\n📝 Note: Dashboard will run in demo mode with mock data.")
        print("   Start the API server for full functionality.\n")

    # Build streamlit command
    project_root = Path(__file__).parent.parent
    dashboard_main = project_root / "src" / "dashboard" / "main.py"

    cmd = [
        "streamlit",
        "run",
        str(dashboard_main),
        "--server.port",
        "8501",
        "--server.address",
        "0.0.0.0",
        "--browser.gatherUsageStats",
        "false",
        "--theme.base",
        "light",
        "--theme.primaryColor",
        "#FF6B35",
        "--theme.backgroundColor",
        "#FFFFFF",
        "--theme.secondaryBackgroundColor",
        "#F0F2F6",
        "--theme.textColor",
        "#262730",
    ]

    print("📊 Starting Streamlit dashboard at http://localhost:8501")
    print(f"🎯 Dashboard file: {dashboard_main}")
    print("⏹️  Press Ctrl+C to stop the dashboard\n")

    try:
        # Run streamlit
        subprocess.run(cmd, check=True)
    except KeyboardInterrupt:
        print("\n👋 Dashboard stopped by user")
    except subprocess.CalledProcessError as e:
        print(f"❌ Error starting dashboard: {e}")
        sys.exit(1)
    except FileNotFoundError:
        print("❌ Streamlit not found. Please install dependencies:")
        print("   poetry install")
        sys.exit(1)


if __name__ == "__main__":
    main()

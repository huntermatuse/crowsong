"""Test script for the crowsong Python bindings -- mirrors main.rs."""

import os
from crowsong import CanaryView

# Load from .env file (same vars as the Rust binary uses)
def load_dotenv():
    try:
        with open(".env") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, value = line.split("=", 1)
                    os.environ.setdefault(key.strip(), value.strip())
    except FileNotFoundError:
        pass

load_dotenv()

endpoint = os.environ["ENDPOINT"]
api_key = os.environ["API_KEY"]
user_id = os.environ.get("USER_ID", "crowsong")

print(f"Connecting to Views service at {endpoint}...")

with CanaryView(endpoint, api_key, "crowsong-test", user_id) as view:
    print(f"Connected! CCI = {view.cci()}")

    print("Testing gRPC connection...")
    view.test()
    print("Test passed.")

    print("Getting service version...")
    version = view.get_version()
    print(f"Service version: {version!r}")

    print("Getting views...")
    views = view.get_views()
    print(f"Views: {views!r}")

    if not views:
        print("No views found; skipping tag list.")
        print("Done.")
        exit()

    first_view = views[0]

    print(f"Getting datasets for view {first_view}...")
    datasets = view.get_dataset_list(first_view)
    print(f"Datasets: {datasets!r}")

    if not datasets:
        print(f"No datasets found in view {first_view}; skipping tag list.")
        print("Done.")
        exit()

    dataset_name = datasets[0]

    print(f"Getting tags for {first_view} / {dataset_name}...")
    tags = view.get_tag_list(first_view, dataset_name, max_count=100)
    print(f"Tags: {tags!r}")

print("Disconnected.")
print("Done.")

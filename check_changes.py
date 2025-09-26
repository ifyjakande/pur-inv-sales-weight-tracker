#!/usr/bin/env python3
"""
Check if source data worksheets have been modified since last update.
Uses content hash of source worksheets instead of Drive API timestamp.

Environment-aware: Always run in local development, smart detection in production.
"""

import json
import os
import sys
import hashlib
from google.oauth2.service_account import Credentials
import gspread

def load_env_file():
    """Load environment variables from .env file if it exists."""
    env_file = os.path.join(os.path.dirname(__file__), '.env')
    if os.path.exists(env_file):
        with open(env_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    value = value.strip().strip('"\'')
                    os.environ[key] = value

def get_credentials():
    """Get Google API credentials from environment."""
    load_env_file()

    service_account_info = os.getenv('SERVICE_ACCOUNT_JSON')
    if not service_account_info:
        raise ValueError("SERVICE_ACCOUNT_JSON environment variable not set")

    try:
        # It's JSON content - for GitHub Actions and local with JSON string
        credentials_dict = json.loads(service_account_info)
        credentials = Credentials.from_service_account_info(
            credentials_dict,
            scopes=[
                'https://www.googleapis.com/auth/spreadsheets.readonly',
                'https://www.googleapis.com/auth/drive.metadata.readonly'
            ]
        )
        print("🔑 Using service account from environment variable")
        return credentials

    except json.JSONDecodeError as e:
        print(f"❌ Error parsing service account JSON: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error creating credentials: {e}")
        sys.exit(1)

def get_worksheet_hash(spreadsheet_id, worksheet_name, credentials):
    """Get content hash of a specific worksheet."""
    try:
        # Use gspread for easier worksheet access
        gc = gspread.authorize(credentials)
        spreadsheet = gc.open_by_key(spreadsheet_id)

        # Get the specific worksheet
        worksheet = spreadsheet.worksheet(worksheet_name)

        # Get all values from the worksheet
        all_values = worksheet.get_all_values()

        # Create hash of the content
        content_str = str(all_values)
        content_hash = hashlib.md5(content_str.encode('utf-8')).hexdigest()

        print(f"📊 Sheet: {worksheet_name}")
        print(f"📝 Rows with data: {len([row for row in all_values if any(cell.strip() for cell in row)])} ")
        print(f"🔗 Content hash: {content_hash}")

        return content_hash

    except gspread.WorksheetNotFound:
        print(f"❌ Worksheet '{worksheet_name}' not found in spreadsheet {spreadsheet_id}")
        print("Available worksheets:")
        try:
            gc = gspread.authorize(credentials)
            spreadsheet = gc.open_by_key(spreadsheet_id)
            for ws in spreadsheet.worksheets():
                print(f"  - {ws.title}")
        except Exception:
            pass
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error getting worksheet hash for {worksheet_name}: {e}")
        sys.exit(1)

def get_combined_source_data_hash(credentials):
    """Get combined content hash of all source data worksheets."""
    try:
        # Get all required environment variables
        source_sheets = [
            {
                'id': os.getenv('PURCHASE_SHEET_ID'),
                'name': os.getenv('PURCHASE_SHEET_NAME'),
                'label': 'Purchase Data'
            },
            {
                'id': os.getenv('INVENTORY_SHEET_ID'),
                'name': os.getenv('INVENTORY_SHEET_NAME'),
                'label': 'Inventory Data'
            },
            {
                'id': os.getenv('STOCK_RELEASE_SHEET_ID'),
                'name': os.getenv('STOCK_INFLOW_SHEET_NAME'),
                'label': 'Stock Inflow Data'
            },
            {
                'id': os.getenv('STOCK_RELEASE_SHEET_ID'),
                'name': os.getenv('RELEASE_SHEET_NAME'),
                'label': 'Release Data'
            }
        ]

        # Validate all required environment variables are present
        missing_vars = []
        for sheet in source_sheets:
            if not sheet['id']:
                missing_vars.append(f"Sheet ID for {sheet['label']}")
            if not sheet['name']:
                missing_vars.append(f"Sheet name for {sheet['label']}")

        if missing_vars:
            print("❌ Missing required environment variables:")
            for var in missing_vars:
                print(f"  - {var}")
            sys.exit(1)

        print(f"🔍 Checking {len(source_sheets)} source data worksheets...")

        # Get hash for each source worksheet
        individual_hashes = []
        for sheet in source_sheets:
            print(f"\n📋 Processing {sheet['label']}:")
            sheet_hash = get_worksheet_hash(sheet['id'], sheet['name'], credentials)
            individual_hashes.append(f"{sheet['label']}:{sheet_hash}")

        # Create combined hash from all individual hashes
        combined_content = '|'.join(individual_hashes)
        combined_hash = hashlib.md5(combined_content.encode('utf-8')).hexdigest()

        print(f"\n🔗 Combined source data hash: {combined_hash}")
        return combined_hash

    except Exception as e:
        print(f"❌ Error getting combined source data hash: {e}")
        sys.exit(1)

def load_last_hash():
    """Load the last processed content hash from file."""
    hash_file = 'last_source_hash.json'
    try:
        if os.path.exists(hash_file):
            with open(hash_file, 'r') as f:
                data = json.load(f)
                return data.get('content_hash')
        return None
    except Exception as e:
        print(f"⚠️  Warning: Could not load last hash: {e}")
        return None

def save_hash(content_hash):
    """Save the current content hash to file."""
    hash_file = 'last_source_hash.json'
    try:
        data = {
            'content_hash': content_hash,
            'updated_at': hashlib.md5(str(content_hash).encode()).hexdigest()
        }
        with open(hash_file, 'w') as f:
            json.dump(data, f, indent=2)
        print(f"💾 Saved new content hash: {content_hash}")
    except Exception as e:
        print(f"❌ Error saving hash: {e}")
        sys.exit(1)

def main():
    """Main function to check for changes in source data."""
    try:
        # Load .env file first
        load_env_file()

        # Environment-aware behavior
        is_production = os.getenv('CI') == 'true'

        if not is_production:
            # Local development - always update
            print("🔧 Local development mode detected")
            print("✅ Always updating in local development - bypassing change detection")
            print("NEEDS_UPDATE=true")
            return True

        # Production mode - check for actual changes
        print("🏭 Production mode detected - checking for source data changes...")

        # Get credentials
        credentials = get_credentials()

        # Get current combined source data hash
        current_hash = get_combined_source_data_hash(credentials)

        # Load last processed hash
        last_hash = load_last_hash()
        print(f"📅 Last processed hash: {last_hash or 'Never'}")

        # Compare hashes
        if current_hash != last_hash:
            print("✅ Source data changes detected! Update needed.")
            save_hash(current_hash)
            print("NEEDS_UPDATE=true")
            return True
        else:
            print("⏭️  No changes in source data detected. Skipping update.")
            print("NEEDS_UPDATE=false")
            return False

    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
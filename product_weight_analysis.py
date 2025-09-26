#!/usr/bin/env python3
"""
Pullus Africa Gizzard Weight Reconciliation System
Analyzes gizzard weight flow from purchase to sales with monthly reporting
"""

import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple, Optional
import logging
import time
import random
import os
import json
from dotenv import load_dotenv
from gspread.exceptions import APIError

# Load environment variables (only if .env file exists - for local development)
if os.path.exists('.env'):
    load_dotenv()

# Configure minimal logging for errors only
logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class Constants:
    """Configuration constants for analysis thresholds"""
    INVENTORY_VARIANCE_THRESHOLD = 5  # kg threshold for inventory discrepancies

class RateLimitManager:
    """Manages API rate limiting with exponential backoff"""

    def __init__(self, max_retries=5, base_delay=1, max_delay=64):
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.last_request_time = 0
        self.min_request_interval = 0.1  # Minimum 100ms between requests

    def execute_with_backoff(self, func, *args, **kwargs):
        """Execute a function with exponential backoff retry logic"""
        for attempt in range(self.max_retries):
            try:
                # Rate limiting: ensure minimum interval between requests
                current_time = time.time()
                time_since_last = current_time - self.last_request_time
                if time_since_last < self.min_request_interval:
                    sleep_time = self.min_request_interval - time_since_last
                    time.sleep(sleep_time)

                result = func(*args, **kwargs)
                self.last_request_time = time.time()
                return result

            except APIError as e:
                if '429' in str(e) or 'quota' in str(e).lower() or 'rate' in str(e).lower():
                    if attempt < self.max_retries - 1:
                        # Calculate exponential backoff delay
                        delay = min(self.base_delay * (2 ** attempt) + random.uniform(0, 1), self.max_delay)
                        pass  # Silent retry for rate limits
                        time.sleep(delay)
                        continue
                    else:
                        logger.error("Rate limit exceeded after maximum retries")
                        raise
                else:
                    # Non-rate limit API error
                    logger.error("Google Sheets API error occurred")
                    raise
            except Exception as e:
                logger.error("Request execution failed")
                raise

        raise Exception(f"Failed after {self.max_retries} attempts")

class BatchRequestManager:
    """Manages batch requests to optimize API usage"""

    def __init__(self, gc, rate_manager):
        self.gc = gc
        self.rate_manager = rate_manager
        self.cached_spreadsheets = {}

    def get_spreadsheet(self, sheet_id):
        """Get spreadsheet with caching"""
        if sheet_id not in self.cached_spreadsheets:
            self.cached_spreadsheets[sheet_id] = self.rate_manager.execute_with_backoff(
                self.gc.open_by_key, sheet_id
            )
        return self.cached_spreadsheets[sheet_id]

    def get_worksheet_data(self, sheet_id, sheet_name, use_batch=True):
        """Get worksheet data with batch optimization"""
        spreadsheet = self.get_spreadsheet(sheet_id)
        worksheet = spreadsheet.worksheet(sheet_name)

        if use_batch:
            # Use batch get to retrieve all data in one call
            return self.rate_manager.execute_with_backoff(
                worksheet.get_all_values
            )
        else:
            return self.rate_manager.execute_with_backoff(
                worksheet.get_all_records
            )

    def batch_update_spreadsheet(self, sheet_id, updates):
        """Perform batch updates to minimize API calls"""
        spreadsheet = self.get_spreadsheet(sheet_id)
        return self.rate_manager.execute_with_backoff(
            spreadsheet.batch_update, updates
        )

    def get_sheet_id(self, spreadsheet, sheet_name):
        """Get the actual sheet ID for a given sheet name"""
        try:
            for sheet in spreadsheet.worksheets():
                if sheet.title == sheet_name:
                    return sheet.id
            return 0  # Fallback to 0 if not found
        except Exception as e:
            logger.warning("Could not get sheet ID")
            return 0

    def write_data_optimized(self, sheet_id, sheet_name, data, clear_first=True):
        """Write data with optimized batch operations"""
        spreadsheet = self.get_spreadsheet(sheet_id)
        worksheet = spreadsheet.worksheet(sheet_name)
        actual_sheet_id = self.get_sheet_id(spreadsheet, sheet_name)

        # Clear ALL content and formatting
        if clear_first:
            # First unmerge all cells
            unmerge_request = {
                'requests': [{
                    'unmergeCells': {
                        'range': {'sheetId': actual_sheet_id}
                    }
                }]
            }

            try:
                self.rate_manager.execute_with_backoff(
                    spreadsheet.batch_update, unmerge_request
                )
                pass  # Cells unmerged
            except Exception:
                pass  # Could not unmerge cells

            # Clear all content first
            self.rate_manager.execute_with_backoff(worksheet.clear)

            # Clear all formatting for entire sheet
            clear_format_request = {
                'requests': [{
                    'repeatCell': {
                        'range': {
                            'sheetId': actual_sheet_id
                            # No start/end indices = entire sheet
                        },
                        'cell': {
                            'userEnteredFormat': {}
                        },
                        'fields': 'userEnteredFormat'
                    }
                }]
            }

            # Apply format clearing
            try:
                self.rate_manager.execute_with_backoff(
                    spreadsheet.batch_update, clear_format_request
                )
                pass  # Formatting cleared
            except Exception:
                pass  # Could not clear formatting

        if data:
            # Calculate range for batch update
            num_rows = len(data)
            num_cols = max(len(row) for row in data) if data else 1

            # Handle columns beyond Z (26 columns)
            if num_cols <= 26:
                end_col = chr(ord('A') + num_cols - 1)
            else:
                # For columns beyond Z, use AA, AB, AC, etc.
                first_char = chr(ord('A') + (num_cols - 27) // 26)
                second_char = chr(ord('A') + (num_cols - 1) % 26)
                end_col = first_char + second_char

            range_name = f'A1:{end_col}{num_rows}'

            # Use batch update for better performance
            self.rate_manager.execute_with_backoff(
                worksheet.update, values=data, range_name=range_name
            )

            return range_name
        return None

class GizzardWeightAnalyzer:
    """Main class for analyzing gizzard weight reconciliation"""

    def __init__(self, service_account_path: str = None):
        """Initialize with service account credentials"""
        self.service_account_path = service_account_path or os.getenv('SERVICE_ACCOUNT_PATH')
        self.gc = None
        self.rate_manager = RateLimitManager()
        self.batch_manager = None
        self.constants = Constants()

        # Sheet configurations from environment variables
        self.sheet_configs = {
            'purchase': {
                'id': os.getenv('PURCHASE_SHEET_ID'),
                'sheet_name': os.getenv('PURCHASE_SHEET_NAME')
            },
            'inventory': {
                'id': os.getenv('INVENTORY_SHEET_ID'),
                'sheet_name': os.getenv('INVENTORY_SHEET_NAME')
            },
            'stock_inflow': {
                'id': os.getenv('STOCK_RELEASE_SHEET_ID'),
                'sheet_name': os.getenv('STOCK_INFLOW_SHEET_NAME')
            },
            'release': {
                'id': os.getenv('STOCK_RELEASE_SHEET_ID'),
                'sheet_name': os.getenv('RELEASE_SHEET_NAME')
            },
            'output': {
                'sheet_name': os.getenv('OUTPUT_SHEET_NAME')
            }
        }

    def _log_debug_error(self, error: Exception, context: str = ""):
        """Log sanitized error information for debugging"""
        if os.getenv('DEBUG_MODE') == 'true':
            logger.debug(f"Error in {context}: {type(error).__name__}")

    def authenticate_sheets(self):
        """Authenticate with Google Sheets API - supports both file and JSON string credentials"""
        try:
            scope = [
                'https://spreadsheets.google.com/feeds',
                'https://www.googleapis.com/auth/drive'
            ]

            # Check if we have SERVICE_ACCOUNT_JSON (for GitHub Actions and local with JSON)
            service_account_json = os.getenv('SERVICE_ACCOUNT_JSON')

            if service_account_json:
                # Use JSON string from environment variable
                try:
                    service_account_info = json.loads(service_account_json)
                    credentials = Credentials.from_service_account_info(
                        service_account_info,
                        scopes=scope
                    )
                    print("✅ Using service account from SERVICE_ACCOUNT_JSON")
                except json.JSONDecodeError:
                    # If JSON parsing fails, treat as file path for backward compatibility
                    if os.path.exists(service_account_json):
                        credentials = Credentials.from_service_account_file(
                            service_account_json,
                            scopes=scope
                        )
                        print(f"✅ Using service account file: {service_account_json}")
                    else:
                        raise Exception(f"Invalid JSON content and file not found: {service_account_json}")

            elif self.service_account_path and os.path.exists(self.service_account_path):
                # Local development mode: Use file path from SERVICE_ACCOUNT_PATH
                credentials = Credentials.from_service_account_file(
                    self.service_account_path,
                    scopes=scope
                )
                print(f"✅ Using service account file: {self.service_account_path}")
            else:
                raise Exception("No valid service account credentials found. Set SERVICE_ACCOUNT_JSON environment variable or SERVICE_ACCOUNT_PATH file.")

            self.gc = gspread.authorize(credentials)
            self.batch_manager = BatchRequestManager(self.gc, self.rate_manager)
            print("✅ Authentication successful")
            return True

        except Exception as e:
            logger.error("Authentication failed: Invalid credentials or configuration")
            self._log_debug_error(e, "authentication")
            return False


    def get_sheet_data(self, sheet_type: str, date_filter: Optional[str] = None) -> pd.DataFrame:
        """Get data from a specific sheet with optional date filtering"""
        try:
            config = self.sheet_configs[sheet_type]

            # Use batch manager for optimized data retrieval
            all_values = self.batch_manager.get_worksheet_data(config['id'], config['sheet_name'], use_batch=True)

            if not all_values:
                pass  # No data found
                return pd.DataFrame()

            # Handle different sheet structures
            if sheet_type in ['purchase', 'inventory']:
                # These sheets have headers in row 4 and data starts from row 5
                if len(all_values) < 4:
                    pass  # Insufficient data
                    return pd.DataFrame()

                headers = all_values[3]  # Row 4 (0-indexed as 3)
                data_rows = all_values[4:]  # Skip first 4 rows

                # Filter out empty headers and corresponding columns
                valid_headers = []
                valid_indices = []
                for i, header in enumerate(headers):
                    if header and str(header).strip():
                        valid_headers.append(str(header).strip())
                        valid_indices.append(i)

                if not valid_headers:
                    pass  # No valid headers
                    return pd.DataFrame()

                # Extract only valid columns from data rows
                filtered_data_rows = []
                for row in data_rows:
                    if any(str(cell).strip() for cell in row if cell):  # Skip completely empty rows
                        filtered_row = [row[i] if i < len(row) else '' for i in valid_indices]
                        filtered_data_rows.append(filtered_row)

                # Create DataFrame
                df = pd.DataFrame(filtered_data_rows, columns=valid_headers)

            else:
                # For stock_inflow and release sheets, use first row as headers
                if len(all_values) < 2:
                    pass  # Insufficient data
                    return pd.DataFrame()

                headers = all_values[0]
                data_rows = all_values[1:]

                # Create DataFrame
                df = pd.DataFrame(data_rows, columns=headers)

            # Remove empty rows
            df = df.dropna(how='all')

            # Apply date filter if provided
            if date_filter and 'DATE' in df.columns:
                # Convert date column to datetime
                df['DATE'] = pd.to_datetime(df['DATE'], errors='coerce')
                # Filter for July 2025 or later
                df = df[df['DATE'] >= date_filter]

            pass  # Data retrieved
            return df

        except Exception as e:
            logger.error("Error retrieving sheet data")
            self._log_debug_error(e, f"data retrieval {sheet_type}")
            return pd.DataFrame()

    def calculate_historical_reconciliation(self, product: str = 'gizzard') -> float:
        """Calculate historical month-by-month reconciliation using ALL data before July 2025 for specified product"""
        pass  # Calculating historical reconciliation

        # Get all historical data without date filter
        stock_inflow_df = self.get_sheet_data('stock_inflow')
        release_df = self.get_sheet_data('release')

        # Extract product data from historical records
        historical_data = {}

        # Stock inflow product data
        if not stock_inflow_df.empty and 'PRODUCT TYPE' in stock_inflow_df.columns:
            stock_inflow_df['DATE'] = pd.to_datetime(stock_inflow_df['DATE'], errors='coerce')

            if product == 'gizzard':
                product_stock = stock_inflow_df[stock_inflow_df['PRODUCT TYPE'].str.contains('Gizzard', case=False, na=False)].copy()
            else:  # whole_chicken
                product_stock = stock_inflow_df[stock_inflow_df['PRODUCT TYPE'].str.contains('Whole Chicken', case=False, na=False)].copy()

            if not product_stock.empty:
                product_stock['WEIGHT'] = pd.to_numeric(product_stock['WEIGHT'].astype(str).str.replace(',', ''), errors='coerce')
                historical_data['stock_inflow'] = product_stock[['DATE', 'WEIGHT']].copy()

        # Release product data
        if not release_df.empty and 'PRODUCT' in release_df.columns:
            release_df['DATE'] = pd.to_datetime(release_df['DATE'], errors='coerce')

            if product == 'gizzard':
                product_release = release_df[release_df['PRODUCT'].str.contains('GIZZARD', case=False, na=False)].copy()
            else:  # whole_chicken
                product_release = release_df[release_df['PRODUCT'].str.contains('WHOLE CHICKEN', case=False, na=False)].copy()

            if not product_release.empty:
                product_release['WEIGHT'] = pd.to_numeric(product_release['WEIGHT in KG'].astype(str).str.replace(',', ''), errors='coerce')
                historical_data['release'] = product_release[['DATE', 'WEIGHT']].copy()

        # Get all unique months from ALL historical data before July 2025
        all_months = set()
        july_cutoff = pd.to_datetime('2025-07-01')

        for df in historical_data.values():
            if not df.empty:
                # Use ALL historical data before July 2025
                df_filtered = df[df['DATE'] < july_cutoff]
                if not df_filtered.empty:
                    months = df_filtered['DATE'].dt.to_period('M').unique()
                    all_months.update(months)

        if not all_months:
            return 0.0  # No historical data

        all_months = sorted(list(all_months))

        # Calculate month-by-month reconciliation
        opening_balance = 0.0  # Start with zero balance

        for month in all_months:
            monthly_inflow = 0.0
            monthly_release = 0.0

            # Calculate stock inflow for this month
            if 'stock_inflow' in historical_data:
                df = historical_data['stock_inflow']
                df['MONTH'] = df['DATE'].dt.to_period('M')
                month_df = df[df['MONTH'] == month]
                if not month_df.empty:
                    monthly_inflow = month_df['WEIGHT'].sum()

            # Calculate release for this month
            if 'release' in historical_data:
                df = historical_data['release']
                df['MONTH'] = df['DATE'].dt.to_period('M')
                month_df = df[df['MONTH'] == month]
                if not month_df.empty:
                    monthly_release = month_df['WEIGHT'].sum()

            # Apply reconciliation formula: Opening + Inflow - Release = Closing
            closing_balance = opening_balance + monthly_inflow - monthly_release

            # This month's closing becomes next month's opening
            opening_balance = closing_balance

        pass  # Historical reconciliation calculated
        return opening_balance

    def calculate_opening_balance(self, product: str = 'gizzard') -> float:
        """Calculate opening balance for July 2025 using historical reconciliation"""
        pass  # Calculating opening balance using historical reconciliation

        # Use the new historical reconciliation method
        opening_balance = self.calculate_historical_reconciliation(product)

        pass  # Opening balance calculated

        return opening_balance

    def extract_product_data(self) -> Dict[str, Dict[str, pd.DataFrame]]:
        """Extract both gizzard and whole chicken data from all sheets"""
        pass  # Extracting product data

        # Date filter for July 2025 onwards
        date_filter = '2025-07-01'

        product_data = {
            'gizzard': {},
            'whole_chicken': {}
        }

        # Purchase sheet - both product weight columns
        purchase_df = self.get_sheet_data('purchase', date_filter)
        if not purchase_df.empty:
            # Gizzard weight column
            gizzard_col = 'PURCHASED GIZZARD WEIGHT'
            if gizzard_col in purchase_df.columns and 'FARMER NAME' in purchase_df.columns:
                product_data['gizzard']['purchase'] = purchase_df[['DATE', 'FARMER NAME', gizzard_col]].copy()
                product_data['gizzard']['purchase']['WEIGHT'] = pd.to_numeric(product_data['gizzard']['purchase'][gizzard_col].astype(str).str.replace(',', ''), errors='coerce')

            # Whole chicken weight column
            chicken_col = 'PURCHASED CHICKEN WEIGHT'
            if chicken_col in purchase_df.columns and 'FARMER NAME' in purchase_df.columns:
                product_data['whole_chicken']['purchase'] = purchase_df[['DATE', 'FARMER NAME', chicken_col]].copy()
                product_data['whole_chicken']['purchase']['WEIGHT'] = pd.to_numeric(product_data['whole_chicken']['purchase'][chicken_col].astype(str).str.replace(',', ''), errors='coerce')

        # Inventory sheet - both product columns
        inventory_df = self.get_sheet_data('inventory', date_filter)
        if not inventory_df.empty:
            # Gizzard inventory column
            gizzard_col = 'INVENTORY GIZZARD WEIGHT'
            if gizzard_col in inventory_df.columns:
                product_data['gizzard']['inventory'] = inventory_df[['DATE', gizzard_col]].copy()
                product_data['gizzard']['inventory']['WEIGHT'] = pd.to_numeric(product_data['gizzard']['inventory'][gizzard_col].astype(str).str.replace(',', ''), errors='coerce')

            # Whole chicken inventory column
            chicken_col = 'INVENTORY CHICKEN WEIGHT'
            if chicken_col in inventory_df.columns:
                product_data['whole_chicken']['inventory'] = inventory_df[['DATE', chicken_col]].copy()
                product_data['whole_chicken']['inventory']['WEIGHT'] = pd.to_numeric(product_data['whole_chicken']['inventory'][chicken_col].astype(str).str.replace(',', ''), errors='coerce')

        # Stock inflow sheet - filter by product type
        stock_inflow_df = self.get_sheet_data('stock_inflow', date_filter)
        if not stock_inflow_df.empty:
            if 'PRODUCT TYPE' in stock_inflow_df.columns and 'WEIGHT' in stock_inflow_df.columns:
                # Filter for gizzard products
                gizzard_stock = stock_inflow_df[stock_inflow_df['PRODUCT TYPE'].str.contains('Gizzard', case=False, na=False)]
                if not gizzard_stock.empty:
                    product_data['gizzard']['stock_inflow'] = gizzard_stock[['DATE', 'WEIGHT']].copy()
                    product_data['gizzard']['stock_inflow']['WEIGHT'] = pd.to_numeric(product_data['gizzard']['stock_inflow']['WEIGHT'].astype(str).str.replace(',', ''), errors='coerce')

                # Filter for whole chicken products
                chicken_stock = stock_inflow_df[stock_inflow_df['PRODUCT TYPE'].str.contains('Whole Chicken', case=False, na=False)]
                if not chicken_stock.empty:
                    product_data['whole_chicken']['stock_inflow'] = chicken_stock[['DATE', 'WEIGHT']].copy()
                    product_data['whole_chicken']['stock_inflow']['WEIGHT'] = pd.to_numeric(product_data['whole_chicken']['stock_inflow']['WEIGHT'].astype(str).str.replace(',', ''), errors='coerce')

        # Release sheet - filter by product name
        release_df = self.get_sheet_data('release', date_filter)
        if not release_df.empty:
            if 'PRODUCT' in release_df.columns and 'WEIGHT in KG' in release_df.columns:
                # Filter for gizzard sales
                gizzard_release = release_df[release_df['PRODUCT'].str.contains('GIZZARD', case=False, na=False)]
                if not gizzard_release.empty:
                    product_data['gizzard']['release'] = gizzard_release[['DATE', 'WEIGHT in KG']].copy()
                    product_data['gizzard']['release']['WEIGHT'] = pd.to_numeric(product_data['gizzard']['release']['WEIGHT in KG'].astype(str).str.replace(',', ''), errors='coerce')

                # Filter for whole chicken sales
                chicken_release = release_df[release_df['PRODUCT'].str.contains('WHOLE CHICKEN', case=False, na=False)]
                if not chicken_release.empty:
                    product_data['whole_chicken']['release'] = chicken_release[['DATE', 'WEIGHT in KG']].copy()
                    product_data['whole_chicken']['release']['WEIGHT'] = pd.to_numeric(product_data['whole_chicken']['release']['WEIGHT in KG'].astype(str).str.replace(',', ''), errors='coerce')

        pass  # Data extracted

        return product_data

    def calculate_comprehensive_monthly_analysis(self, product_data: Dict[str, Dict[str, pd.DataFrame]]) -> pd.DataFrame:
        """Calculate comprehensive month-by-month analysis with opening balances for both products"""
        pass  # Calculating monthly analysis

        # Get unique months from July 2025 onwards for both products
        all_months = set()
        for product, datasets in product_data.items():
            for df in datasets.values():
                if not df.empty:
                    df['DATE'] = pd.to_datetime(df['DATE'])
                    months = df['DATE'].dt.to_period('M').unique()
                    all_months.update(months)

        all_months = sorted(list(all_months))

        # Initialize analysis DataFrame with side-by-side structure
        analysis_data = []

        # Calculate opening balances for both products
        gizzard_opening = self.calculate_opening_balance('gizzard')
        chicken_opening = self.calculate_opening_balance('whole_chicken')

        for month in all_months:
            # Format month as "Jan-2025", "Feb-2025", etc.
            month_formatted = month.strftime('%b-%Y')
            month_data = {
                'MONTH': month_formatted,
                # Gizzard columns
                'G_OPENING_BALANCE': gizzard_opening,
                'G_PURCHASES': 0,
                'G_INVENTORY_RECEIVED': 0,
                'G_STOCK_INFLOW': 0,
                'G_SALES_RELEASE': 0,
                'G_CLOSING_BALANCE': 0,
                'G_PURCHASE_VS_INVENTORY_VAR': 0,
                'G_INVENTORY_VS_STOCK_VAR': 0,
                'G_WEIGHT_GAIN_RATE': 0,
                'G_AVAILABLE_STOCK': 0,
                'G_UTILIZATION_PCT': 0,
                'G_STORAGE_GAIN': 0,
                'G_STORAGE_GAIN_PCT': 0,
                # Whole Chicken columns
                'C_OPENING_BALANCE': chicken_opening,
                'C_PURCHASES': 0,
                'C_INVENTORY_RECEIVED': 0,
                'C_STOCK_INFLOW': 0,
                'C_SALES_RELEASE': 0,
                'C_CLOSING_BALANCE': 0,
                'C_PURCHASE_VS_INVENTORY_VAR': 0,
                'C_INVENTORY_VS_STOCK_VAR': 0,
                'C_WEIGHT_GAIN_RATE': 0,
                'C_AVAILABLE_STOCK': 0,
                'C_UTILIZATION_PCT': 0,
                'C_STORAGE_GAIN': 0,
                'C_STORAGE_GAIN_PCT': 0
            }

            # Extract monthly data for each product and source
            for product, datasets in product_data.items():
                prefix = 'G_' if product == 'gizzard' else 'C_'

                for source, df in datasets.items():
                    if not df.empty:
                        df['DATE'] = pd.to_datetime(df['DATE'])
                        df['MONTH'] = df['DATE'].dt.to_period('M')
                        month_df = df[df['MONTH'] == month]

                        if not month_df.empty:
                            monthly_total = month_df['WEIGHT'].sum()

                            if source == 'purchase':
                                month_data[f'{prefix}PURCHASES'] = monthly_total
                            elif source == 'inventory':
                                month_data[f'{prefix}INVENTORY_RECEIVED'] = monthly_total
                            elif source == 'stock_inflow':
                                month_data[f'{prefix}STOCK_INFLOW'] = monthly_total
                            elif source == 'release':
                                month_data[f'{prefix}SALES_RELEASE'] = monthly_total

            # Calculate derived metrics for both products
            for product_prefix, opening_key in [('G_', 'G_OPENING_BALANCE'), ('C_', 'C_OPENING_BALANCE')]:
                opening_balance = month_data[opening_key]
                purchases = month_data[f'{product_prefix}PURCHASES']
                sales = month_data[f'{product_prefix}SALES_RELEASE']

                # Basic calculations
                month_data[f'{product_prefix}CLOSING_BALANCE'] = opening_balance + month_data[f'{product_prefix}STOCK_INFLOW'] - sales
                month_data[f'{product_prefix}PURCHASE_VS_INVENTORY_VAR'] = purchases - month_data[f'{product_prefix}INVENTORY_RECEIVED']
                month_data[f'{product_prefix}INVENTORY_VS_STOCK_VAR'] = month_data[f'{product_prefix}INVENTORY_RECEIVED'] - month_data[f'{product_prefix}STOCK_INFLOW']

                # New improved metrics with correct business logic
                # Available = what's physically available in cold storage
                month_data[f'{product_prefix}AVAILABLE_STOCK'] = opening_balance + month_data[f'{product_prefix}STOCK_INFLOW']
                available_stock = month_data[f'{product_prefix}AVAILABLE_STOCK']

                # Utilization_% = sales efficiency from available stock
                month_data[f'{product_prefix}UTILIZATION_PCT'] = (sales / available_stock * 100) if available_stock > 0 else 0

                # Storage_Gain = storage impact on sellable weight: Sales - Available_Stock
                month_data[f'{product_prefix}STORAGE_GAIN'] = sales - available_stock

                # Weight_% = storage efficiency relative to purchases
                month_data[f'{product_prefix}WEIGHT_GAIN_RATE'] = (month_data[f'{product_prefix}STORAGE_GAIN'] / purchases * 100) if purchases > 0 else 0

                # Storage_% = storage impact as % of available stock (negative = loss, positive = gain)
                month_data[f'{product_prefix}STORAGE_GAIN_PCT'] = (month_data[f'{product_prefix}STORAGE_GAIN'] / available_stock * 100) if available_stock > 0 else 0

                # Update opening balance for next month
                if product_prefix == 'G_':
                    gizzard_opening = month_data[f'{product_prefix}CLOSING_BALANCE']
                else:
                    chicken_opening = month_data[f'{product_prefix}CLOSING_BALANCE']

            analysis_data.append(month_data)

        return pd.DataFrame(analysis_data)

    def create_dashboard(self, monthly_analysis: pd.DataFrame, product_data: Dict[str, Dict[str, pd.DataFrame]]):
        """Create professional dashboard in target spreadsheet with dual product analysis"""
        pass  # Creating dashboard

        try:

            # Title and headers - each row must have 25 columns for proper merging
            title_data = [
                ['PULLUS AFRICA GIZZARD & WHOLE CHICKEN WEIGHT RECONCILIATION ANALYSIS'] + [''] * 24,
                ['Monthly Analysis Report - Updated at ' + datetime.now(timezone(timedelta(hours=1))).strftime('%Y-%m-%d %I:%M %p WAT')] + [''] * 24,
                [''] + [''] * 24
            ]

            # Business definitions section - keep ALL new metrics with head commit formatting style
            definitions_data = [
                ['BUSINESS DEFINITIONS & TERMS:'] + [''] * 24,
                ['TERM', 'DEFINITION'] + [''] * 23,
                ['Opening', 'Product inventory carried from previous month (kg)'] + [''] * 23,
                ['Purchases', 'Product bought from farmers by purchase team (kg)'] + [''] * 23,
                ['Inventory', 'Product recorded by inventory team after offtake (kg)'] + [''] * 23,
                ['Inflow', 'Product logged into inventory record (kg)'] + [''] * 23,
                ['Available', 'Total stock available for sale: Opening + Inflow (kg)'] + [''] * 23,
                ['Sales', 'Product sold and released from cold storage (kg)'] + [''] * 23,
                ['Closing', 'Opening + Inflow - Sales (kg)'] + [''] * 23,
                ['Pur_Var', 'Difference between purchase and inventory records (offtake)'] + [''] * 23,
                ['Inv_Var', 'Difference between inventory team records (should be 0)'] + [''] * 23,
                ['Utiliz_%', 'Inventory efficiency: Sales ÷ Available Stock × 100 (higher = better)'] + [''] * 23,
                ['Storage_Gain', 'Storage impact on sellable weight: Sales - Available Stock (kg)'] + [''] * 23,
                ['Storage_%', 'Storage impact as % of available stock (negative = loss, positive = gain)'] + [''] * 23
            ]

            # Monthly analysis section
            analysis_headers = [
                # Empty row for spacing (row 19) - must have 25 columns
                [''] + [''] * 24,
                # Header on row 20 - must have 25 columns for proper merging
                ['MONTH-BY-MONTH DUAL PRODUCT WEIGHT ANALYSIS (All weights in KG)'] + [''] * 24,
                # Column headers on row 21
                [
                    'MONTH',
                    # Gizzard columns
                    'GIZZARD\nOPENING', 'GIZZARD\nPURCHASES', 'GIZZARD\nINVENTORY', 'GIZZARD\nINFLOW', 'GIZZARD\nAVAILABLE', 'GIZZARD\nSALES', 'GIZZARD\nCLOSING', 'GIZZARD\nPUR_VAR', 'GIZZARD\nINV_VAR', 'GIZZARD\nUTILIZ_%', 'GIZZARD\nSTOR_GAIN', 'GIZZARD\nSTORAGE_%',
                    # Whole Chicken columns
                    'CHICKEN\nOPENING', 'CHICKEN\nPURCHASES', 'CHICKEN\nINVENTORY', 'CHICKEN\nINFLOW', 'CHICKEN\nAVAILABLE', 'CHICKEN\nSALES', 'CHICKEN\nCLOSING', 'CHICKEN\nPUR_VAR', 'CHICKEN\nINV_VAR', 'CHICKEN\nUTILIZ_%', 'CHICKEN\nSTOR_GAIN', 'CHICKEN\nSTORAGE_%'
                ]
            ]

            # Prepare analysis data (only data rows, no headers)
            analysis_data = []
            if not monthly_analysis.empty:

                # Data rows
                for _, row in monthly_analysis.iterrows():
                    # Gizzard data processing
                    g_purchase_var = row['G_PURCHASE_VS_INVENTORY_VAR']
                    g_inventory_var = row['G_INVENTORY_VS_STOCK_VAR']
                    if abs(g_purchase_var) < 0.005:
                        g_purchase_var = 0.0
                    if abs(g_inventory_var) < 0.005:
                        g_inventory_var = 0.0

                    # Whole Chicken data processing
                    c_purchase_var = row['C_PURCHASE_VS_INVENTORY_VAR']
                    c_inventory_var = row['C_INVENTORY_VS_STOCK_VAR']
                    if abs(c_purchase_var) < 0.005:
                        c_purchase_var = 0.0
                    if abs(c_inventory_var) < 0.005:
                        c_inventory_var = 0.0

                    data_row = [
                        row['MONTH'],
                        # Gizzard data - store as numbers for conditional formatting
                        round(row['G_OPENING_BALANCE'], 2),
                        round(row['G_PURCHASES'], 2),
                        round(row['G_INVENTORY_RECEIVED'], 2),
                        round(row['G_STOCK_INFLOW'], 2),
                        round(row['G_AVAILABLE_STOCK'], 2),
                        round(row['G_SALES_RELEASE'], 2),
                        round(row['G_CLOSING_BALANCE'], 2),
                        round(g_purchase_var, 2),
                        round(g_inventory_var, 2),
                        round(row['G_UTILIZATION_PCT'] / 100, 3),  # Convert to decimal for percentage formatting
                        round(row['G_STORAGE_GAIN'], 2),
                        round(row['G_STORAGE_GAIN_PCT'] / 100, 3),  # Convert to decimal for percentage formatting
                        # Whole Chicken data - store as numbers for conditional formatting
                        round(row['C_OPENING_BALANCE'], 2),
                        round(row['C_PURCHASES'], 2),
                        round(row['C_INVENTORY_RECEIVED'], 2),
                        round(row['C_STOCK_INFLOW'], 2),
                        round(row['C_AVAILABLE_STOCK'], 2),
                        round(row['C_SALES_RELEASE'], 2),
                        round(row['C_CLOSING_BALANCE'], 2),
                        round(c_purchase_var, 2),
                        round(c_inventory_var, 2),
                        round(row['C_UTILIZATION_PCT'] / 100, 3),  # Convert to decimal for percentage formatting
                        round(row['C_STORAGE_GAIN'], 2),
                        round(row['C_STORAGE_GAIN_PCT'] / 100, 3)  # Convert to decimal for percentage formatting
                    ]
                    analysis_data.append(data_row)

            # Key insights section - ensure 25 columns for proper merging
            insights_data = [
                [''] + [''] * 24,
                ['KEY BUSINESS INSIGHTS & CRITICAL FINDINGS:'] + [''] * 24,
                [''] + [''] * 24
            ]

            # Calculate totals and insights from monthly analysis with dual products
            if not monthly_analysis.empty:
                total_months = len(monthly_analysis)

                # Gizzard analysis with new metrics
                g_total_purchases = monthly_analysis['G_PURCHASES'].sum()
                g_total_sales = monthly_analysis['G_SALES_RELEASE'].sum()
                g_total_available = monthly_analysis['G_AVAILABLE_STOCK'].sum()
                g_total_storage_gain = monthly_analysis['G_STORAGE_GAIN'].sum()
                g_avg_utilization = monthly_analysis['G_UTILIZATION_PCT'].mean()
                g_avg_storage_gain_rate = monthly_analysis['G_STORAGE_GAIN_PCT'].mean()
                g_final_balance = monthly_analysis['G_CLOSING_BALANCE'].iloc[-1]

                # Whole Chicken analysis with new metrics
                c_total_purchases = monthly_analysis['C_PURCHASES'].sum()
                c_total_sales = monthly_analysis['C_SALES_RELEASE'].sum()
                c_total_available = monthly_analysis['C_AVAILABLE_STOCK'].sum()
                c_total_storage_gain = monthly_analysis['C_STORAGE_GAIN'].sum()
                c_avg_utilization = monthly_analysis['C_UTILIZATION_PCT'].mean()
                c_avg_storage_gain_rate = monthly_analysis['C_STORAGE_GAIN_PCT'].mean()
                c_final_balance = monthly_analysis['C_CLOSING_BALANCE'].iloc[-1]

                insights_data.extend([
                    [f'GIZZARD STORAGE & UTILIZATION ANALYSIS:', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', ''],
                    [f'• Total Months Analyzed: {total_months}', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', ''],
                    [f'• Total Purchased: {g_total_purchases:,.2f} kg', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', ''],
                    [f'• Total Sold: {g_total_sales:,.2f} kg', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', ''],
                    [f'• Net Storage Impact: {g_total_storage_gain:,.2f} kg', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', ''],
                    [f'• Average Inventory Utilization: {g_avg_utilization:.1f}%', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', ''],
                    [f'• Average Storage Effect: {g_avg_storage_gain_rate:.1f}%', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', ''],
                    [f'• Balance: {g_final_balance:,.2f} kg', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', ''],
                    ['', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', ''],
                    [f'CHICKEN STORAGE & UTILIZATION ANALYSIS:', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', ''],
                    [f'• Total Months Analyzed: {total_months}', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', ''],
                    [f'• Total Purchased: {c_total_purchases:,.2f} kg', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', ''],
                    [f'• Total Sold: {c_total_sales:,.2f} kg', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', ''],
                    [f'• Net Storage Impact: {c_total_storage_gain:,.2f} kg', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', ''],
                    [f'• Average Inventory Utilization: {c_avg_utilization:.1f}%', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', ''],
                    [f'• Average Storage Effect: {c_avg_storage_gain_rate:.1f}%', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', ''],
                    [f'• Balance: {c_final_balance:,.2f} kg', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', ''],
                    ['', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', ''],
                    [f'COMPARATIVE STORAGE INSIGHTS:', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', ''],
                    [f'• Storage Impact: Gizzard {g_total_storage_gain:,.2f} kg vs Chicken {c_total_storage_gain:,.2f} kg', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', ''],
                    [f'• Utilization Efficiency: Gizzard {g_avg_utilization:.1f}% vs Chicken {c_avg_utilization:.1f}%', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', ''],
                    ['', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', ''],
                    [f'CRITICAL FINDINGS:', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '']
                ])

                # Analyze critical issues with new storage-focused insights
                # Check for unusual storage gains (possible measurement errors)
                if g_total_storage_gain > 0:
                    insights_data.append([f'🚨 GIZZARD STORAGE GAIN: {g_total_storage_gain:,.2f} kg more sold than available stock (cold storage weight gain)', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', ''])
                if c_total_storage_gain > 0:
                    insights_data.append([f'🚨 CHICKEN STORAGE GAIN: {c_total_storage_gain:,.2f} kg more sold than available stock (cold storage weight gain)', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', ''])

                # Check for low utilization efficiency
                if g_avg_utilization < 70:
                    insights_data.append([f'⚠️  GIZZARD LOW UTILIZATION: {g_avg_utilization:.1f}% of available stock used (optimize inventory levels)', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', ''])
                if c_avg_utilization < 70:
                    insights_data.append([f'⚠️  CHICKEN LOW UTILIZATION: {c_avg_utilization:.1f}% of available stock used (optimize inventory levels)', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', ''])

                # Check for negative inventory
                if g_final_balance < 0:
                    insights_data.append([f'🚨 GIZZARD NEGATIVE INVENTORY: {g_final_balance:,.2f} kg deficit (stock reconciliation needed)', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', ''])
                if c_final_balance < 0:
                    insights_data.append([f'🚨 CHICKEN NEGATIVE INVENTORY: {c_final_balance:,.2f} kg deficit (stock reconciliation needed)', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', ''])

                # Check inventory recording discrepancies
                g_max_inventory_var = monthly_analysis['G_INVENTORY_VS_STOCK_VAR'].abs().max()
                c_max_inventory_var = monthly_analysis['C_INVENTORY_VS_STOCK_VAR'].abs().max()
                if g_max_inventory_var > self.constants.INVENTORY_VARIANCE_THRESHOLD:
                    insights_data.append([f'⚠️  GIZZARD RECORDING INCONSISTENCY: Up to {g_max_inventory_var:,.2f} kg difference between inventory team records', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', ''])
                if c_max_inventory_var > self.constants.INVENTORY_VARIANCE_THRESHOLD:
                    insights_data.append([f'⚠️  CHICKEN RECORDING INCONSISTENCY: Up to {c_max_inventory_var:,.2f} kg difference between inventory team records', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', ''])

                # Check for high storage gains (possible measurement errors)
                if g_avg_storage_gain_rate > 10:
                    insights_data.append([f'⚠️  GIZZARD MEASUREMENT ALERT: {g_avg_storage_gain_rate:.1f}% average storage gain may indicate weighing inconsistencies', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', ''])
                if c_avg_storage_gain_rate > 10:
                    insights_data.append([f'⚠️  CHICKEN MEASUREMENT ALERT: {c_avg_storage_gain_rate:.1f}% average storage gain may indicate weighing inconsistencies', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', ''])

            # Analysis complete

            # Combine all data
            all_data = title_data + definitions_data + analysis_headers + analysis_data + insights_data

            # Data ready for writing

            # Write data to multiple sheets using optimized batch operations
            if all_data:
                # Define target sheets: inventory and purchase sheets using output sheet name from .env
                output_sheet_name = self.sheet_configs['output']['sheet_name']
                target_sheets = [
                    {
                        'id': self.sheet_configs['inventory']['id'],
                        'sheet_name': output_sheet_name
                    },
                    {
                        'id': self.sheet_configs['purchase']['id'],
                        'sheet_name': output_sheet_name
                    }
                ]

                # Write to each target sheet
                for sheet_info in target_sheets:
                    # Use batch manager for optimized writing
                    range_name = self.batch_manager.write_data_optimized(
                        sheet_info['id'],
                        sheet_info['sheet_name'],
                        all_data,
                        clear_first=True
                    )

                    if range_name:
                        # Calculate dynamic row indices for proper formatting
                        insights_start_row = len(title_data) + len(definitions_data) + len(analysis_headers) + len(analysis_data)
                        insights_header_row = insights_start_row + 1  # The "KEY BUSINESS INSIGHTS..." header row (0-indexed)

                        # Apply formatting using batch manager
                        self.apply_dual_dashboard_formatting_optimized(sheet_info['id'], sheet_info['sheet_name'], len(all_data), insights_header_row)

        except Exception as e:
            logger.error(f"Dashboard generation error: {str(e)}")
            logger.error(f"Error type: {type(e).__name__}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            self._log_debug_error(e, "dashboard generation")

    def apply_dual_dashboard_formatting_optimized(self, sheet_id: str, sheet_name: str, total_rows: int, insights_header_row: int = 21):
        """Apply beautiful colorful formatting for dual product dashboard with extended column support"""
        try:
            spreadsheet = self.batch_manager.get_spreadsheet(sheet_id)
            actual_sheet_id = self.batch_manager.get_sheet_id(spreadsheet, sheet_name)

            format_requests = []

            # Set optimized column widths for dual product layout (25 columns total)
            column_widths = [
                100,  # MONTH
                # Gizzard columns (12 columns)
                90, 95, 95, 90, 90, 90, 90, 90, 95, 85, 95, 85,
                # Whole Chicken columns (12 columns)
                90, 95, 95, 90, 90, 90, 90, 90, 95, 85, 95, 85
            ]
            for col_idx, width in enumerate(column_widths):
                format_requests.append({
                    'updateDimensionProperties': {
                        'range': {
                            'sheetId': actual_sheet_id,
                            'dimension': 'COLUMNS',
                            'startIndex': col_idx,
                            'endIndex': col_idx + 1
                        },
                        'properties': {'pixelSize': width},
                        'fields': 'pixelSize'
                    }
                })

            # Beautiful borders for the entire sheet (25 columns total)
            format_requests.append({
                'updateBorders': {
                    'range': {
                        'sheetId': actual_sheet_id,
                        'startRowIndex': 0,
                        'endRowIndex': total_rows,
                        'startColumnIndex': 0,
                        'endColumnIndex': 25  # A to Y (25 columns)
                    },
                    'top': {'style': 'SOLID', 'width': 1, 'color': {'red': 0.3, 'green': 0.3, 'blue': 0.3}},
                    'bottom': {'style': 'SOLID', 'width': 1, 'color': {'red': 0.3, 'green': 0.3, 'blue': 0.3}},
                    'left': {'style': 'SOLID', 'width': 1, 'color': {'red': 0.3, 'green': 0.3, 'blue': 0.3}},
                    'right': {'style': 'SOLID', 'width': 1, 'color': {'red': 0.3, 'green': 0.3, 'blue': 0.3}},
                    'innerHorizontal': {'style': 'SOLID', 'width': 1, 'color': {'red': 0.6, 'green': 0.6, 'blue': 0.6}},
                    'innerVertical': {'style': 'SOLID', 'width': 1, 'color': {'red': 0.6, 'green': 0.6, 'blue': 0.6}}
                }
            })

            # Add thicker vertical separator between gizzard and chicken columns (between column M and N)
            format_requests.append({
                'updateBorders': {
                    'range': {
                        'sheetId': actual_sheet_id,
                        'startRowIndex': 19,  # Start from column headers row
                        'endRowIndex': insights_header_row,  # End before insights section
                        'startColumnIndex': 12,  # Column M (last gizzard column - STORAGE_%)
                        'endColumnIndex': 13   # Column N (first chicken column - OPENING)
                    },
                    'right': {'style': 'SOLID', 'width': 3, 'color': {'red': 0.2, 'green': 0.4, 'blue': 0.7}}  # Thicker blue line
                }
            })

            # 1. Main Title - Beautiful Gradient Green (Row 1)
            format_requests.append({
                'repeatCell': {
                    'range': {
                        'sheetId': actual_sheet_id,
                        'startRowIndex': 0,
                        'endRowIndex': 1,
                        'startColumnIndex': 0,
                        'endColumnIndex': 25
                    },
                    'cell': {
                        'userEnteredFormat': {
                            'backgroundColor': {'red': 0.1, 'green': 0.6, 'blue': 0.2},
                            'textFormat': {'bold': True, 'fontSize': 16, 'foregroundColor': {'red': 1, 'green': 1, 'blue': 1}},
                            'horizontalAlignment': 'CENTER',
                            'verticalAlignment': 'MIDDLE'
                        }
                    },
                    'fields': 'userEnteredFormat'
                }
            })

            # 2. Subtitle - Soft Lavender (Row 2)
            format_requests.append({
                'repeatCell': {
                    'range': {
                        'sheetId': actual_sheet_id,
                        'startRowIndex': 1,
                        'endRowIndex': 2,
                        'startColumnIndex': 0,
                        'endColumnIndex': 25
                    },
                    'cell': {
                        'userEnteredFormat': {
                            'backgroundColor': {'red': 0.9, 'green': 0.9, 'blue': 0.95},
                            'textFormat': {'italic': True, 'fontSize': 11, 'foregroundColor': {'red': 0.3, 'green': 0.3, 'blue': 0.5}},
                            'horizontalAlignment': 'CENTER'
                        }
                    },
                    'fields': 'userEnteredFormat'
                }
            })

            # 3. Section Headers - Beautiful Blue (Row 4, 21, dynamic)
            # Business definitions header
            format_requests.append({
                'repeatCell': {
                    'range': {
                        'sheetId': actual_sheet_id,
                        'startRowIndex': 3,
                        'endRowIndex': 4,
                        'startColumnIndex': 0,
                        'endColumnIndex': 25
                    },
                    'cell': {
                        'userEnteredFormat': {
                            'backgroundColor': {'red': 0.2, 'green': 0.5, 'blue': 0.8},
                            'textFormat': {'bold': True, 'fontSize': 12, 'foregroundColor': {'red': 1, 'green': 1, 'blue': 1}},
                            'horizontalAlignment': 'LEFT'
                        }
                    },
                    'fields': 'userEnteredFormat'
                }
            })

            # Monthly analysis header formatting will be applied after merging

            # Key insights header
            format_requests.append({
                'repeatCell': {
                    'range': {
                        'sheetId': actual_sheet_id,
                        'startRowIndex': insights_header_row,
                        'endRowIndex': insights_header_row + 1,
                        'startColumnIndex': 0,
                        'endColumnIndex': 25
                    },
                    'cell': {
                        'userEnteredFormat': {
                            'backgroundColor': {'red': 0.2, 'green': 0.5, 'blue': 0.8},
                            'textFormat': {'bold': True, 'fontSize': 12, 'foregroundColor': {'red': 1, 'green': 1, 'blue': 1}},
                            'horizontalAlignment': 'LEFT'
                        }
                    },
                    'fields': 'userEnteredFormat'
                }
            })

            # 4. Table Headers - Blue background for column headers
            # Business definitions table header
            format_requests.append({
                'repeatCell': {
                    'range': {
                        'sheetId': actual_sheet_id,
                        'startRowIndex': 4,
                        'endRowIndex': 5,
                        'startColumnIndex': 0,
                        'endColumnIndex': 25
                    },
                    'cell': {
                        'userEnteredFormat': {
                            'backgroundColor': {'red': 0.8, 'green': 0.95, 'blue': 1.0},
                            'textFormat': {'bold': True, 'fontSize': 10, 'foregroundColor': {'red': 0.1, 'green': 0.3, 'blue': 0.5}},
                            'horizontalAlignment': 'CENTER',
                            'wrapStrategy': 'WRAP'
                        }
                    },
                    'fields': 'userEnteredFormat'
                }
            })

            # Monthly table headers - Strong Blue background
            format_requests.append({
                'repeatCell': {
                    'range': {
                        'sheetId': actual_sheet_id,
                        'startRowIndex': 19,  # Column headers row
                        'endRowIndex': 20,
                        'startColumnIndex': 0,
                        'endColumnIndex': 25
                    },
                    'cell': {
                        'userEnteredFormat': {
                            'backgroundColor': {'red': 0.2, 'green': 0.5, 'blue': 0.8},
                            'textFormat': {'bold': True, 'fontSize': 11, 'foregroundColor': {'red': 1, 'green': 1, 'blue': 1}},
                            'horizontalAlignment': 'CENTER',
                            'wrapStrategy': 'WRAP'
                        }
                    },
                    'fields': 'userEnteredFormat'
                }
            })

            # 5. Definition rows - Soft Mint Green (Rows 6-18) - includes new metrics
            format_requests.append({
                'repeatCell': {
                    'range': {
                        'sheetId': actual_sheet_id,
                        'startRowIndex': 5,
                        'endRowIndex': 18,
                        'startColumnIndex': 0,
                        'endColumnIndex': 25
                    },
                    'cell': {
                        'userEnteredFormat': {
                            'backgroundColor': {'red': 0.95, 'green': 0.98, 'blue': 0.95},
                            'textFormat': {'fontSize': 9, 'foregroundColor': {'red': 0.2, 'green': 0.4, 'blue': 0.2}},
                            'verticalAlignment': 'TOP',
                            'wrapStrategy': 'WRAP'
                        }
                    },
                    'fields': 'userEnteredFormat'
                }
            })

            # 6. Monthly data rows - Base text formatting only (background will be set by alternating colors)
            # Apply base text formatting to all data rows at once
            format_requests.append({
                'repeatCell': {
                    'range': {
                        'sheetId': actual_sheet_id,
                        'startRowIndex': 20,  # Data starts from row 20 (after column headers on row 19)
                        'endRowIndex': insights_header_row,
                        'startColumnIndex': 0,
                        'endColumnIndex': 25
                    },
                    'cell': {
                        'userEnteredFormat': {
                            'textFormat': {'fontSize': 9, 'foregroundColor': {'red': 0.1, 'green': 0.1, 'blue': 0.4}},
                            'horizontalAlignment': 'CENTER'
                        }
                    },
                    'fields': 'userEnteredFormat.textFormat,userEnteredFormat.horizontalAlignment'
                }
            })

            # 7. Insights section - Light Peach (After insights header)
            format_requests.append({
                'repeatCell': {
                    'range': {
                        'sheetId': actual_sheet_id,
                        'startRowIndex': insights_header_row + 1,
                        'endRowIndex': total_rows,
                        'startColumnIndex': 0,
                        'endColumnIndex': 25
                    },
                    'cell': {
                        'userEnteredFormat': {
                            'backgroundColor': {'red': 1.0, 'green': 0.95, 'blue': 0.9},
                            'textFormat': {'fontSize': 10, 'foregroundColor': {'red': 0.4, 'green': 0.2, 'blue': 0.1}},
                            'verticalAlignment': 'TOP',
                            'wrapStrategy': 'WRAP',
                            'horizontalAlignment': 'LEFT'
                        }
                    },
                    'fields': 'userEnteredFormat'
                }
            })

            # 7b. Format insights labels (column A) with bold text
            format_requests.append({
                'repeatCell': {
                    'range': {
                        'sheetId': actual_sheet_id,
                        'startRowIndex': insights_header_row + 1,
                        'endRowIndex': total_rows,
                        'startColumnIndex': 0,
                        'endColumnIndex': 1
                    },
                    'cell': {
                        'userEnteredFormat': {
                            'backgroundColor': {'red': 1.0, 'green': 0.95, 'blue': 0.9},
                            'textFormat': {'fontSize': 10, 'bold': True, 'foregroundColor': {'red': 0.4, 'green': 0.2, 'blue': 0.1}},
                            'verticalAlignment': 'TOP',
                            'horizontalAlignment': 'LEFT'
                        }
                    },
                    'fields': 'userEnteredFormat'
                }
            })

            # 8. Merge header cells
            merge_ranges = [
                {'startRowIndex': 0, 'endRowIndex': 1, 'startColumnIndex': 0, 'endColumnIndex': 25},  # Title
                {'startRowIndex': 1, 'endRowIndex': 2, 'startColumnIndex': 0, 'endColumnIndex': 25},  # Subtitle
                {'startRowIndex': 3, 'endRowIndex': 4, 'startColumnIndex': 0, 'endColumnIndex': 25},  # Business definitions
                {'startRowIndex': 18, 'endRowIndex': 19, 'startColumnIndex': 0, 'endColumnIndex': 25},  # Monthly analysis header (row 19) - merge the main header
                {'startRowIndex': insights_header_row, 'endRowIndex': insights_header_row + 1, 'startColumnIndex': 0, 'endColumnIndex': 25}   # Key insights
            ]

            for merge_range in merge_ranges:
                format_requests.append({
                    'mergeCells': {
                        'range': {
                            'sheetId': actual_sheet_id,
                            **merge_range
                        },
                        'mergeType': 'MERGE_ALL'
                    }
                })

            # 9. Merge cells for definition columns (B-AA) and add italic formatting
            # Definition rows are from row 6 to 18 (0-indexed: 5 to 17) - includes new metrics
            for row_idx in range(5, 18):  # Rows 6-18 in the sheet
                # Merge columns B-S (1-18 in 0-indexed) for each definition
                format_requests.append({
                    'mergeCells': {
                        'range': {
                            'sheetId': actual_sheet_id,
                            'startRowIndex': row_idx,
                            'endRowIndex': row_idx + 1,
                            'startColumnIndex': 1,  # Column B
                            'endColumnIndex': 25    # Column S
                        },
                        'mergeType': 'MERGE_ALL'
                    }
                })

                # Add italic formatting specifically for the definition text
                format_requests.append({
                    'repeatCell': {
                        'range': {
                            'sheetId': actual_sheet_id,
                            'startRowIndex': row_idx,
                            'endRowIndex': row_idx + 1,
                            'startColumnIndex': 1,  # Column B
                            'endColumnIndex': 25    # Column S
                        },
                        'cell': {
                            'userEnteredFormat': {
                                'backgroundColor': {'red': 0.95, 'green': 0.98, 'blue': 0.95},
                                'textFormat': {
                                    'fontSize': 9,
                                    'italic': True,  # Make definitions italic
                                    'foregroundColor': {'red': 0.2, 'green': 0.4, 'blue': 0.2}
                                },
                                'verticalAlignment': 'TOP',
                                'wrapStrategy': 'WRAP',
                                'horizontalAlignment': 'LEFT'
                            }
                        },
                        'fields': 'userEnteredFormat'
                    }
                })

            # 9b. Merge cells for insights section (entire row A-S for each insight row)
            # Calculate insights section start row
            insights_start_row = insights_header_row + 1
            insights_end_row = total_rows

            for row_idx in range(insights_start_row, insights_end_row):
                # Merge ALL columns A-S (0-18 in 0-indexed) for each insight row
                format_requests.append({
                    'mergeCells': {
                        'range': {
                            'sheetId': actual_sheet_id,
                            'startRowIndex': row_idx,
                            'endRowIndex': row_idx + 1,
                            'startColumnIndex': 0,  # Column A
                            'endColumnIndex': 25    # Column S
                        },
                        'mergeType': 'MERGE_ALL'
                    }
                })

            # 9c. Apply monthly analysis header formatting AFTER merging
            format_requests.append({
                'repeatCell': {
                    'range': {
                        'sheetId': actual_sheet_id,
                        'startRowIndex': 18,
                        'endRowIndex': 19,
                        'startColumnIndex': 0,
                        'endColumnIndex': 25
                    },
                    'cell': {
                        'userEnteredFormat': {
                            'backgroundColor': {'red': 0.2, 'green': 0.5, 'blue': 0.8},
                            'textFormat': {'bold': True, 'fontSize': 12, 'foregroundColor': {'red': 1, 'green': 1, 'blue': 1}},
                            'horizontalAlignment': 'CENTER',
                            'verticalAlignment': 'MIDDLE'
                        }
                    },
                    'fields': 'userEnteredFormat'
                }
            })

            # Number formatting will be applied AFTER alternating colors to prevent being overwritten

            # 11. Add alternating row colors for data table (dynamic)
            # Apply alternating colors to data rows only
            data_start_row = 20  # First data row (after column headers on row 19)
            data_end_row = insights_header_row  # End before insights

            for i, row_idx in enumerate(range(data_start_row, data_end_row)):
                if i % 2 == 0:
                    # Even rows - Light blue
                    bg_color = {'red': 0.95, 'green': 0.97, 'blue': 1.0}
                else:
                    # Odd rows - Very light gray
                    bg_color = {'red': 0.98, 'green': 0.98, 'blue': 0.98}

                format_requests.append({
                    'repeatCell': {
                        'range': {
                            'sheetId': actual_sheet_id,
                            'startRowIndex': row_idx,
                            'endRowIndex': row_idx + 1,
                            'startColumnIndex': 0,
                            'endColumnIndex': 25
                        },
                        'cell': {
                            'userEnteredFormat': {
                                'backgroundColor': bg_color,
                                'textFormat': {'fontSize': 9, 'foregroundColor': {'red': 0.1, 'green': 0.1, 'blue': 0.4}},
                                'horizontalAlignment': 'CENTER'
                            }
                        },
                        'fields': 'userEnteredFormat.backgroundColor,userEnteredFormat.textFormat,userEnteredFormat.horizontalAlignment'
                    }
                })

            # 12. Add number formatting with commas for all numeric columns (applying AFTER alternating colors)
            # Apply comma formatting to all numeric data columns, excluding percentage columns
            # Columns with commas: All numeric except G_UTILIZ_% (10), G_STORAGE_% (12), C_UTILIZ_% (22), C_STORAGE_% (24)
            numeric_columns_with_commas = (
                list(range(1, 10)) +    # G_OPENING to G_INV_VAR (1-9)
                [11] +                  # G_STORAGE_GAIN (11)
                list(range(13, 22)) +   # C_OPENING to C_INV_VAR (13-21)
                [23]                    # C_STORAGE_GAIN (23)
            )

            for col_idx in numeric_columns_with_commas:
                format_requests.append({
                    'repeatCell': {
                        'range': {
                            'sheetId': actual_sheet_id,
                            'startRowIndex': 20,  # Start from data rows (after column headers at row 19)
                            'endRowIndex': insights_header_row,  # End before insights section
                            'startColumnIndex': col_idx,
                            'endColumnIndex': col_idx + 1
                        },
                        'cell': {
                            'userEnteredFormat': {
                                'numberFormat': {
                                    'type': 'NUMBER',
                                    'pattern': '#,##0.00'
                                }
                            }
                        },
                        'fields': 'userEnteredFormat.numberFormat'
                    }
                })

            # 13. Add percentage formatting for percentage columns (UTILIZ_% and STORAGE_% columns)
            # Percentage columns: G_UTILIZ_% (10), G_STORAGE_% (12), C_UTILIZ_% (22), C_STORAGE_% (24)
            percentage_columns = [10, 12, 22, 24]

            for col_idx in percentage_columns:
                format_requests.append({
                    'repeatCell': {
                        'range': {
                            'sheetId': actual_sheet_id,
                            'startRowIndex': 20,  # Start from data rows (after column headers at row 19)
                            'endRowIndex': insights_header_row,  # End before insights section
                            'startColumnIndex': col_idx,
                            'endColumnIndex': col_idx + 1
                        },
                        'cell': {
                            'userEnteredFormat': {
                                'numberFormat': {
                                    'type': 'PERCENT',
                                    'pattern': '0.0%'
                                }
                            }
                        },
                        'fields': 'userEnteredFormat.numberFormat'
                    }
                })

            # 14. Add conditional formatting for negative values (red background)
            # Apply to ALL data cells (both gizzard and chicken columns) in the monthly analysis table
            format_requests.append({
                'addConditionalFormatRule': {
                    'rule': {
                        'ranges': [{
                            'sheetId': actual_sheet_id,
                            'startRowIndex': 20,  # Start from data rows (after column headers)
                            'endRowIndex': insights_header_row,  # End before insights section
                            'startColumnIndex': 1,  # Start from column B (after MONTH column)
                            'endColumnIndex': 25   # All data columns including both products
                        }],
                        'booleanRule': {
                            'condition': {
                                'type': 'NUMBER_LESS',
                                'values': [{'userEnteredValue': '-0.01'}]
                            },
                            'format': {
                                'backgroundColor': {'red': 1.0, 'green': 0.8, 'blue': 0.8},  # Light red background
                                'textFormat': {'foregroundColor': {'red': 0.8, 'green': 0.0, 'blue': 0.0}, 'bold': True}  # Dark red text, bold
                            }
                        }
                    },
                    'index': 0
                }
            })

            # Apply batch formatting
            batch_update_request = {'requests': format_requests}
            self.batch_manager.batch_update_spreadsheet(sheet_id, batch_update_request)

        except Exception:
            # Fallback to basic formatting
            self.apply_basic_formatting(sheet_id, sheet_name)

    def apply_basic_formatting(self, sheet_id: str, sheet_name: str):
        """Apply basic colorful formatting as fallback"""
        try:
            spreadsheet = self.batch_manager.get_spreadsheet(sheet_id)
            worksheet = spreadsheet.worksheet(sheet_name)

            # Title formatting - Green background
            self.rate_manager.execute_with_backoff(
                worksheet.format, 'A1:AA1', {
                    'backgroundColor': {'red': 0.1, 'green': 0.6, 'blue': 0.2},
                    'textFormat': {'bold': True, 'fontSize': 16, 'foregroundColor': {'red': 1, 'green': 1, 'blue': 1}},
                    'horizontalAlignment': 'CENTER'
                }
            )

            # Subtitle formatting - Light purple
            self.rate_manager.execute_with_backoff(
                worksheet.format, 'A2:AA2', {
                    'backgroundColor': {'red': 0.9, 'green': 0.9, 'blue': 0.95},
                    'textFormat': {'italic': True, 'fontSize': 11},
                    'horizontalAlignment': 'CENTER'
                }
            )

            # Section headers - Blue background
            self.rate_manager.execute_with_backoff(
                worksheet.format, 'A4:AA4', {
                    'backgroundColor': {'red': 0.2, 'green': 0.5, 'blue': 0.8},
                    'textFormat': {'bold': True, 'fontSize': 12, 'foregroundColor': {'red': 1, 'green': 1, 'blue': 1}},
                    'horizontalAlignment': 'LEFT'
                }
            )

            # Table headers - Light cyan
            self.rate_manager.execute_with_backoff(
                worksheet.format, 'A5:AA5', {
                    'backgroundColor': {'red': 0.8, 'green': 0.95, 'blue': 1.0},
                    'textFormat': {'bold': True, 'fontSize': 10},
                    'horizontalAlignment': 'CENTER'
                }
            )

            pass  # Basic formatting applied

        except Exception:
            pass  # Basic formatting failed

    def run_complete_analysis(self):
        """Run the complete dual product weight analysis"""
        print("Starting dual product weight analysis...")

        # Step 1: Authenticate
        if not self.authenticate_sheets():
            return False

        # Step 2: Extract product data for both gizzard and whole chicken
        product_data = self.extract_product_data()

        # Check if we have data for either product
        has_data = False
        for product, datasets in product_data.items():
            if any(not df.empty for df in datasets.values()):
                has_data = True
                break

        if not has_data:
            print("No product data found")
            return False

        # Step 3: Calculate comprehensive monthly analysis
        monthly_analysis = self.calculate_comprehensive_monthly_analysis(product_data)

        # Step 4: Create dashboard
        self.create_dashboard(monthly_analysis, product_data)

        print("Analysis completed successfully!")
        return True

def main():
    """Main execution function"""
    analyzer = GizzardWeightAnalyzer()

    success = analyzer.run_complete_analysis()

    if success:
        print("\n" + "="*60)
        print("ANALYSIS COMPLETED SUCCESSFULLY!")
        print("="*60)
        print("Dashboard updated successfully in target spreadsheets")
    else:
        print("\n" + "="*60)
        print("ANALYSIS FAILED!")
        print("="*60)
        print("Check the logs above for error details.")

if __name__ == "__main__":
    main()
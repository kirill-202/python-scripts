import logging
import os
import time
import zlib
from dataclasses import dataclass
from typing import TypedDict

import requests
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Constants
PAUSE_DURATION = 20  # Duration to wait between synchronization cycles

# Environment variables for configuration
PLAYRIX_SPREAD_SHEET_ID = os.environ["PLAYRIX_SPREAD_SHEET_ID"]
GOOGLE_CRED_PATH = os.environ["GOOGLE_CRED_PATH"]
GRIDLY_API_KEY = os.environ["GRIDLY_API_KEY"]
GRIDLY_DATABASE_ID = os.environ["GRIDLY_DATABASE_ID"]
SHEET_NAMES = os.environ["SHEET_NAMES"]

# Type definitions for Gridly API responses
class Cell(TypedDict):
    columnId: str
    value: str

class Record(TypedDict):
    id: str
    cells: list[Cell]

class Grid(TypedDict):
    id: str
    name: str

class View(TypedDict):
    id: str
    name: str
    gridId: str

# Data classes to store checksum and row data for efficient change detection
@dataclass
class RowChecksum:
    row_id: int
    row_checksum: int
    row_content: list[str]

@dataclass
class SheetChecksum:
    sheet_title: str
    reference: str | None = None  # Reference view ID for the Gridly sheet
    hashed_rows: list[RowChecksum] = None

    def __post_init__(self):
        if self.hashed_rows is None:
            self.hashed_rows = []

# Client to interact with the Gridly API
class GridlyClient:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.gridly.com/v1"
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"ApiKey {api_key}",
            "Content-Type": "application/json"
        })

    # Execute API requests with method and endpoint
    def execute_api_request(self, method: str, endpoint: str, body: dict | None = None) -> dict:
        url = f"{self.base_url}{endpoint}"
        response = self.session.request(method, url, json=body)
        response.raise_for_status()
        return response.json() if response.content else {}

    # Add new rows to a specific Gridly view
    def add_rows_to_gridly(self, view_id: str, new_rows: list[RowChecksum]) -> None:
        endpoint = f"/views/{view_id}/records"
        records = []
        
        for row in new_rows:
            record_id = row.row_content[0]
            column_values = row.row_content[1:]
            
            cells = [
                {"columnId": f"column{i+1}", "value": value}
                for i, value in enumerate(column_values)
            ]
            
            records.append({"id": record_id, "cells": cells})

        self.execute_api_request("POST", endpoint, records)
        logging.info("New rows were added to Gridly.")

    # Update existing rows in Gridly if changes are detected
    def update_gridly_row(self, view_id: str, rows: list[RowChecksum]) -> None:
        for row in rows:
            record_id = row.row_content[0]
            column_values = row.row_content[1:]
            
            cells = [
                {"columnId": f"column{i+1}", "value": value}
                for i, value in enumerate(column_values)
            ]
            
            record = {"id": record_id, "cells": cells}
            endpoint = f"/views/{view_id}/records/{record_id}"
            
            self.execute_api_request("PATCH", endpoint, record)
            logging.info(f"Successfully updated record {record_id}")

    # Retrieve grids in the Gridly database
    def fetch_grids_by_database_id(self, database_id: str) -> list[Grid]:
        endpoint = f"/grids?dbId={database_id}"
        return self.execute_api_request("GET", endpoint)

    # Fetch views for a specific grid
    def fetch_grid_view(self, grid_id: str) -> View:
        endpoint = f"/views?gridId={grid_id}"
        views = self.execute_api_request("GET", endpoint)
        if not views:
            raise ValueError("No views found")
        return views[0]

    # Retrieve records in a specific view
    def fetch_records_for_view(self, view_id: str) -> list[Record]:
        endpoint = f"/views/{view_id}/records"
        return self.execute_api_request("GET", endpoint)

# Client to interact with Google Sheets API
class SheetsClient:
    def __init__(self):
        self.service = self._build_service()

    def _build_service(self):
        creds = Credentials.from_service_account_file(GOOGLE_CRED_PATH)
        return build("sheets", "v4", credentials=creds)

    # Retrieve data from a specific Google Sheet range
    def get_sheet_data(self, spreadsheet_id: str, range_name: str):
        try:
            result = self.service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=range_name
            ).execute()
            return result.get('values', [])
        except HttpError as error:
            logging.error(f"Error fetching sheet data: {error}")
            raise

# Compute a checksum for a row based on its contents for change detection
def compute_row_hash(row_data: list[str]) -> int:
    concatenated = "".join(row_data)
    return zlib.crc32(concatenated.encode())

# Process each Google Sheet and generate row checksums
def process_sheet(title: str, sheets_client: SheetsClient, 
                 sheet_hashes: dict[str, SheetChecksum]) -> None:
    try:
        data = sheets_client.get_sheet_data(PLAYRIX_SPREAD_SHEET_ID, title)
        if not data:
            return

        hashed_rows = []
        for i, row in enumerate(data[1:], 1):  # Skip header row
            row_checksum = compute_row_hash(row)
            hashed_rows.append(RowChecksum(i-1, row_checksum, row))

        sheet_hashes[title] = SheetChecksum(
            sheet_title=title,
            hashed_rows=hashed_rows
        )
        logging.info(f"All columns of sheet with title {title} were hashed")
    
    except Exception as e:
        logging.error(f"Error processing sheet {title}: {e}")

# Process each Gridly grid and generate row checksums
def process_gridly_grid(grid: Grid, gridly_sheets: dict[str, SheetChecksum], 
                       client: GridlyClient) -> None:
    try:
        gridly_sheet = SheetChecksum(sheet_title=grid['name'])
        view = client.fetch_grid_view(grid['id'])
        gridly_sheet.reference = view['id']
        
        records = client.fetch_records_for_view(view['id'])
        
        for i, record in enumerate(records):
            processed_rows = [record['id']] + [cell['value'] for cell in record['cells']]
            row_hash = compute_row_hash(processed_rows)
            gridly_sheet.hashed_rows.append(
                RowChecksum(i, row_hash, processed_rows)
            )
        
        gridly_sheets[grid['name']] = gridly_sheet
    
    except Exception as e:
        logging.error(f"Error processing Gridly grid {grid['name']}: {e}")

# Compare rows in Google Sheets and Gridly to identify differences
def sheets_equal(gridly_title: str, hashed_sheet: SheetChecksum, 
                google_sheets: dict[str, SheetChecksum]) -> list[RowChecksum] | None:
    if gridly_title not in google_sheets:
        return None

    rows_to_push = []
    google_sheet = google_sheets[gridly_title]
    
    # Create a dictionary for better performance
    hashed_sheet_dict = {row.row_id: row.row_checksum for row in hashed_sheet.hashed_rows}

    for row in google_sheet.hashed_rows:
        if (row.row_id in hashed_sheet_dict and hashed_sheet_dict[row.row_id] != row.row_checksum):
            rows_to_push.append(row)

    return rows_to_push

# Detect new rows in Google Sheets that don't exist in Gridly
def find_new_rows(current_google_hash: dict[str, SheetChecksum], 
                 gridly_sheet: SheetChecksum) -> list[RowChecksum] | None:
    if gridly_sheet.sheet_title not in current_google_hash:
        return None
        
    google_sheet = current_google_hash[gridly_sheet.sheet_title]
    diff = len(google_sheet.hashed_rows) - len(gridly_sheet.hashed_rows)
    
    if diff <= 0:
        return None
    
    return google_sheet.hashed_rows[-diff:]

def main():
    # Set up logging with a specified format and level to capture INFO and ERROR messages
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    logging.info("Program started...") 
    

    sheet_names = SHEET_NAMES.split(',')
    logging.info(f"Sheet titles to sync in GoogleSheet and Gridly: {sheet_names}")
    
    # Initialize Google Sheets and Gridly clients
    sheets_client = SheetsClient()
    gridly_client = GridlyClient(GRIDLY_API_KEY)
    
    try:
        # Initial processing: create a hash map of Google Sheet data for initial comparison
        init_sheet_hash = {}
        for title in sheet_names:
            process_sheet(title, sheets_client, init_sheet_hash)  # Store checksums for each sheet

        # Retrieve and process existing grids from Gridly for synchronization
        gridly_sheets = {}
        grids = gridly_client.fetch_grids_by_database_id(GRIDLY_DATABASE_ID)
        
        for grid in grids:
            process_gridly_grid(grid, gridly_sheets, gridly_client)  # Populate grid data from Gridly

        logging.info("All initial sheets processed and checksums calculated.")
        
        # Initial comparison between Google Sheets and Gridly grids to check synchronization status
        for title, sheet in gridly_sheets.items():
            rows = sheets_equal(title, sheet, init_sheet_hash)
            status = "fully matches" if rows else "does not match"
            logging.info(f"GRIDLY Sheet <{title}> <{status}> the initial Google Sheet")
        
        # Start a continuous loop to regularly check for updates in Google Sheets
        while True:
            logging.info("Starting a new Gridly synchronization check...")
            
            # Create a fresh hash map for the latest Google Sheet data
            current_google_hash = {}
            for title in sheet_names:
                process_sheet(title, sheets_client, current_google_hash)  # Update checksums

            # Check and synchronize each Gridly sheet with the updated Google Sheet data
            for title, sheet in gridly_sheets.items():
                current_google_hash[title].reference = sheet.reference  # Maintain Gridly references
                rows_to_push = sheets_equal(title, sheet, current_google_hash)

                # Update Gridly sheet if there are changes detected in the Google Sheet
                if rows_to_push:
                    logging.info(f"GRIDLY Sheet <{title}> needs update to match Google Sheet")
                    gridly_client.update_gridly_row(sheet.reference, rows_to_push)

                # Identify new rows in Google Sheets to add to Gridly
                new_rows = find_new_rows(current_google_hash, sheet)
                logging.info("new rows that were added to Google: %s", new_rows)

                # Add any new rows from Google Sheets into the corresponding Gridly grid
                if new_rows:
                    logging.info(f"Adding new rows to Gridly Sheet {title}")
                    gridly_client.add_rows_to_gridly(sheet.reference, new_rows)
                else:
                    logging.info(f"GRIDLY Sheet <{title}> fully matches the updated Google Sheet")

                # Update the Gridly sheet data to reflect the latest Google Sheet state
                gridly_sheets = current_google_hash
                
            
            time.sleep(PAUSE_DURATION)
            
    except Exception as e:
        logging.error(f"Fatal error in main loop: {e}")
        raise

if __name__ == "__main__":
    main()

from fastapi import FastAPI, Query
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import os

app = FastAPI()

# Define the base directory dynamically using __file__
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "static")

# Columns to keep
COLUMNS_TO_KEEP = [
    "latitude", "longitude", "name", "population", "price_per_m2",
    "province", "rooms", "year", "GST", "average_price",
    "total_tax_rate", "facility", "population_price_interaction", "rooms_latitude_interaction"
]

# Mount the static folder
app.mount("/static", StaticFiles(directory=DATA_DIR), name="static")

# Serve index.html at root
@app.get("/")
def serve_index():
    file_path = os.path.join(DATA_DIR, "index.html")
    if os.path.exists(file_path):
        return FileResponse(file_path)
    else:
        return {"error": "index.html not found"}

# Endpoint to fetch images based on province
@app.get("/images/{province}")
def get_images(province: str):
    province_images = []

    # Iterate over files in the static directory
    for file in os.listdir(DATA_DIR):
        if province.lower() in file.lower() and file.endswith(('.png', '.jpg', '.jpeg')):
            province_images.append(f"/static/{file}")

    if not province_images:
        return JSONResponse({"error": f"No images found for province: {province}"}, status_code=404)

    return province_images

# Function to process flattened housing data
@app.get("/data/flattened_housing")
def get_flattened_housing_data(page: int = Query(1, ge=1), limit: int = Query(100, ge=1, le=1000)):
    return fetch_data("flatteneddata.csv", page, limit)

# Function to process advanced predictions data
@app.get("/data/advanced_predictions")
def get_advanced_predictions(page: int = Query(1, ge=1), limit: int = Query(100, ge=1, le=1000)):
    return fetch_data("advancesd_predictions.csv", page, limit)

# Function to process affordability by region data
@app.get("/data/affordability_by_region")
def get_affordability_by_region(page: int = Query(1, ge=1), limit: int = Query(100, ge=1, le=1000)):
    return fetch_data("affordabilitybyregion.csv", page, limit)

# Function to process regional housing characteristics data
@app.get("/data/regional_housing_characteristics")
def get_regional_housing_characteristics(page: int = Query(1, ge=1), limit: int = Query(100, ge=1, le=1000)):
    return fetch_data("regionalhousecharacteristics.csv", page, limit)

# Helper function to fetch data from local CSV
def fetch_data(file_name: str, page: int, limit: int):
    try:
        file_path = os.path.join(DATA_DIR, file_name)
        if not os.path.exists(file_path):
            return {"error": f"File {file_path} not found"}

        # Read the CSV file
        df = pd.read_csv(file_path)

        # Keep only specified columns if applicable
        df = df[df.columns.intersection(COLUMNS_TO_KEEP)]

        # Paginate the DataFrame
        start_idx = (page - 1) * limit
        end_idx = start_idx + limit
        paginated_data = df.iloc[start_idx:end_idx]

        return paginated_data.to_dict(orient="records")
    except Exception as e:
        return {"error": str(e)}

# Add CORS Middleware to handle cross-origin requests
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow requests from any origin
    allow_credentials=True,
    allow_methods=["*"],  # Allow all HTTP methods
    allow_headers=["*"],  # Allow all HTTP headers
)
# Function to search data by facility
@app.get("/data/search_by_facility")
def search_by_facility(facility: str, page: int = Query(1, ge=1), limit: int = Query(100, ge=1, le=1000)):
    try:
        file_path = os.path.join(DATA_DIR, "flatteneddata.csv")  # Change file if needed
        if not os.path.exists(file_path):
            return {"error": f"File {file_path} not found"}

        # Read the CSV file
        df = pd.read_csv(file_path)

        # Filter rows by facility
        filtered_df = df[df["facility"].str.contains(facility, case=False, na=False)]

        # Drop unwanted columns
        excluded_columns = ["features_normalized_value", "features_normalized_type"]
        filtered_df = filtered_df.drop(columns=excluded_columns, errors="ignore")

        # Paginate the DataFrame
        start_idx = (page - 1) * limit
        end_idx = start_idx + limit
        paginated_data = filtered_df.iloc[start_idx:end_idx]

        if filtered_df.empty:
            return {"message": f"No data found for facility: {facility}"}

        return paginated_data.to_dict(orient="records")
    except Exception as e:
        return {"error": str(e)}

# import random
# import json

# # Constants for latitude and longitude ranges for Canada
# LATITUDE_RANGE = (42.0, 83.0)
# LONGITUDE_RANGE = (-141.0, -53.0)

# # Define some random job categories and facilities
# job_categories = ["Engineer", "Doctor", "Software Developer", "Teacher", "Mechanic", "Nurse", "Accountant", "Construction Worker", "Retail Worker", "Tour Guide"]
# house_facilities = ["garage", "garden", "central heating", "solar panels", "smart home systems", "swimming pool", "gym", "home office", "balcony", "fireplace"]
# area_facilities = ["hospitals", "schools", "shopping malls", "parks", "universities", "public transit", "museums", "entertainment centers", "sports complexes", "libraries"]

# # Sales tax data from the image
# tax_data = {
#     "Alberta": {"GST": 5, "PST": 0, "HST": 0, "total_tax_rate": 5},
#     "British Columbia": {"GST": 5, "PST": 7, "HST": 0, "total_tax_rate": 12},
#     "Manitoba": {"GST": 5, "PST": 7, "HST": 0, "total_tax_rate": 12},
#     "New Brunswick": {"GST": 0, "PST": 0, "HST": 15, "total_tax_rate": 15},
#     "Newfoundland and Labrador": {"GST": 0, "PST": 0, "HST": 15, "total_tax_rate": 15},
#     "Northwest Territories": {"GST": 5, "PST": 0, "HST": 0, "total_tax_rate": 5},
#     "Nova Scotia": {"GST": 0, "PST": 0, "HST": 15, "total_tax_rate": 15},
#     "Nunavut": {"GST": 5, "PST": 0, "HST": 0, "total_tax_rate": 5},
#     "Ontario": {"GST": 0, "PST": 0, "HST": 13, "total_tax_rate": 13},
#     "Prince Edward Island": {"GST": 0, "PST": 0, "HST": 15, "total_tax_rate": 15},
#     "Quebec": {"GST": 5, "PST": 9.975, "HST": 0, "total_tax_rate": 14.975},
#     "Saskatchewan": {"GST": 5, "PST": 6, "HST": 0, "total_tax_rate": 11},
#     "Yukon": {"GST": 5, "PST": 0, "HST": 0, "total_tax_rate": 5}
# }

# # Provinces and their populations
# province_data = {
#     "Alberta": 4452000,
#     "British Columbia": 5147712,
#     "Manitoba": 1377517,
#     "New Brunswick": 789225,
#     "Newfoundland and Labrador": 522875,
#     "Northwest Territories": 45024,
#     "Nova Scotia": 979351,
#     "Nunavut": 38708,
#     "Ontario": 14734000,
#     "Prince Edward Island": 166584,
#     "Quebec": 8508000,
#     "Saskatchewan": 1177884,
#     "Yukon": 40220
# }

# # Function to generate random coordinates within Canada
# def generate_coordinates():
#     latitude = round(random.uniform(LATITUDE_RANGE[0], LATITUDE_RANGE[1]), 6)
#     longitude = round(random.uniform(LONGITUDE_RANGE[0], LONGITUDE_RANGE[1]), 6)
#     return latitude, longitude

# # Function to generate random job opportunities
# def generate_job_opportunities():
#     return random.sample(job_categories, random.randint(3, 6))

# # Function to generate random house data
# def generate_housing_data():
#     housing_data = []
#     for year in range(2014, 2024):  # Years from 2014 to 2023
#         house = {
#             "year": year,
#             "average_price": random.randint(200000, 2000000),  # Increased range for larger variance
#             "rooms": random.randint(2, 8),
#             "area_size_m2": random.randint(60, 400),
#             "facilities": random.sample(house_facilities, random.randint(2, 6))
#         }
#         housing_data.append(house)
#     return housing_data

# # Function to generate data for each area in the province
# def generate_area_data(area_name, province_population):
#     area_population = random.randint(1000, province_population // 2)  # Ensure area population is less than province population
#     latitude, longitude = generate_coordinates()
#     area_data = {
#         "name": area_name,
#         "population": area_population,
#         "latitude": latitude,
#         "longitude": longitude,
#         "area_size_km2": random.uniform(100, 5000),
#         "housing_data": generate_housing_data(),
#         "jobs_opportunities": generate_job_opportunities(),
#         "area_facilities": random.sample(area_facilities, random.randint(3, 7))
#     }
#     return area_data

# # Function to generate the full dataset
# def generate_province_data():
#     data = []
#     for province, population in province_data.items():
#         province_entry = {
#             "province": province,
#             "population": population,
#             "areas": [],
#             "tax_rate": tax_data[province]
#         }
#         # Generate more areas for larger data size
#         for _ in range(random.randint(50, 100)):  # Increased area count to generate more data
#             area_name = f"Area_{random.randint(1, 10000)}_{province[:3]}"
#             province_entry["areas"].append(generate_area_data(area_name, population))
        
#         data.append(province_entry)
#     return data

# # Generate the data
# canadian_housing_data = generate_province_data()

# # Convert to JSON and pretty print
# canadian_housing_data_json = json.dumps(canadian_housing_data, indent=4)

# # Save to file or display
# with open("canadian_housing_data_10MB.json", "w") as f:
#     f.write(canadian_housing_data_json)

# print("Data generated and saved to 'canadian_housing_data_10MB.json'.")

import random
import json

# Constants for latitude and longitude ranges for Canada
LATITUDE_RANGE = (42.0, 83.0)
LONGITUDE_RANGE = (-141.0, -53.0)

# Define some random job categories and facilities
job_categories = ["Engineer", "Doctor", "Software Developer", "Teacher", "Mechanic", "Nurse", "Accountant", "Construction Worker", "Retail Worker", "Tour Guide"]
house_facilities = ["garage", "garden", "central heating", "solar panels", "smart home systems", "swimming pool", "gym", "home office", "balcony", "fireplace"]
area_facilities = ["hospitals", "schools", "shopping malls", "parks", "universities", "public transit", "museums", "entertainment centers", "sports complexes", "libraries"]

# Sales tax data from the image
tax_data = {
    "Alberta": {"GST": 5, "PST": 0, "HST": 0, "total_tax_rate": 5},
    "British Columbia": {"GST": 5, "PST": 7, "HST": 0, "total_tax_rate": 12},
    "Manitoba": {"GST": 5, "PST": 7, "HST": 0, "total_tax_rate": 12},
    "New Brunswick": {"GST": 0, "PST": 0, "HST": 15, "total_tax_rate": 15},
    "Newfoundland and Labrador": {"GST": 0, "PST": 0, "HST": 15, "total_tax_rate": 15},
    "Northwest Territories": {"GST": 5, "PST": 0, "HST": 0, "total_tax_rate": 5},
    "Nova Scotia": {"GST": 0, "PST": 0, "HST": 15, "total_tax_rate": 15},
    "Nunavut": {"GST": 5, "PST": 0, "HST": 0, "total_tax_rate": 5},
    "Ontario": {"GST": 0, "PST": 0, "HST": 13, "total_tax_rate": 13},
    "Prince Edward Island": {"GST": 0, "PST": 0, "HST": 15, "total_tax_rate": 15},
    "Quebec": {"GST": 5, "PST": 9.975, "HST": 0, "total_tax_rate": 14.975},
    "Saskatchewan": {"GST": 5, "PST": 6, "HST": 0, "total_tax_rate": 11},
    "Yukon": {"GST": 5, "PST": 0, "HST": 0, "total_tax_rate": 5}
}

# Provinces and their populations
province_data = {
    "Alberta": 4452000,
    "British Columbia": 5147712,
    "Manitoba": 1377517,
    "New Brunswick": 789225,
    "Newfoundland and Labrador": 522875,
    "Northwest Territories": 45024,
    "Nova Scotia": 979351,
    "Nunavut": 38708,
    "Ontario": 14734000,
    "Prince Edward Island": 166584,
    "Quebec": 8508000,
    "Saskatchewan": 1177884,
    "Yukon": 40220
}

# Function to generate random coordinates within Canada
def generate_coordinates():
    latitude = round(random.uniform(LATITUDE_RANGE[0], LATITUDE_RANGE[1]), 6)
    longitude = round(random.uniform(LONGITUDE_RANGE[0], LONGITUDE_RANGE[1]), 6)
    return latitude, longitude

# Function to generate random job opportunities
def generate_job_opportunities():
    return random.sample(job_categories, random.randint(3, 6))

# Function to generate random house data
def generate_housing_data():
    housing_data = []
    for year in range(2014, 2025):  # Years from 2014 to 2024
        house = {
            "year": year,
            "average_price": random.randint(200000, 2000000),  # Increased range for larger variance
            "rooms": random.randint(2, 8),
            "area_size_m2": random.randint(60, 400),
            "facilities": random.sample(house_facilities, random.randint(2, 6))
        }
        housing_data.append(house)
    return housing_data

# Function to introduce 1% anomalies
def introduce_anomalies(data):
    total_items = len(data)
    anomalies_count = max(1, total_items // 100)  # 1% anomalies

    for _ in range(anomalies_count):
        random_province = random.choice(data)
        random_area = random.choice(random_province["areas"])

        # Choose a random type of anomaly to introduce
        anomaly_type = random.choice(["coordinates", "price", "population", "missing_jobs"])
        
        if anomaly_type == "coordinates":
            # Set invalid coordinates
            random_area["latitude"] = random.uniform(90, 100)  # Invalid latitude
            random_area["longitude"] = random.uniform(-180, -160)  # Invalid longitude

        elif anomaly_type == "price":
            # Set an unrealistic price for housing
            for house in random_area["housing_data"]:
                house["average_price"] = random.randint(5000000, 10000000)  # Extremely high price

        elif anomaly_type == "population":
            # Set invalid population (larger than province population)
            random_area["population"] = random_province["population"] * 2

        elif anomaly_type == "missing_jobs":
            # Remove job opportunities for an area
            random_area["jobs_opportunities"] = []
    
    return data

# Function to generate data for each area in the province
def generate_area_data(area_name, province_population):
    area_population = random.randint(1000, province_population // 2)  # Ensure area population is less than province population
    latitude, longitude = generate_coordinates()
    area_data = {
        "name": area_name,
        "population": area_population,
        "latitude": latitude,
        "longitude": longitude,
        "area_size_km2": random.uniform(100, 5000),
        "housing_data": generate_housing_data(),
        "jobs_opportunities": generate_job_opportunities(),
        "area_facilities": random.sample(area_facilities, random.randint(3, 7))
    }
    return area_data

# Function to generate the full dataset
def generate_province_data():
    data = []
    for province, population in province_data.items():
        province_entry = {
            "province": province,
            "population": population,
            "areas": [],
            "tax_rate": tax_data[province]
        }
        # Generate more areas for larger data size
        for _ in range(random.randint(50, 100)):  # Increased area count to generate more data
            area_name = f"Area_{random.randint(1, 10000)}_{province[:3]}"
            province_entry["areas"].append(generate_area_data(area_name, population))
        
        data.append(province_entry)
    return data

# Generate the data
canadian_housing_data = generate_province_data()

# Introduce 1% anomalies
canadian_housing_data_with_anomalies = introduce_anomalies(canadian_housing_data)

# Convert to JSON and pretty print
canadian_housing_data_json = json.dumps(canadian_housing_data_with_anomalies, indent=4)

# Save to file or display
with open("canadian_housing_data_with_anomalies_10MB.json", "w") as f:
    f.write(canadian_housing_data_json)

print("Data with anomalies generated and saved to 'canadian_housing_data_with_anomalies_10MB.json'.")

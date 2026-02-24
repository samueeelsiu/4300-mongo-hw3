import pandas as pd
import json
import numpy as np


df = pd.read_csv('fy2026-property-assessment-data_12_23_2025.csv', low_memory=False)
df.columns = df.columns.str.strip()

cols_to_keep = [
    'PID', 'ST_NUM', 'ST_NAME', 'ZIP_CODE', 'LU_DESC', 'OWN_OCC', 
    'TOTAL_VALUE', 'GROSS_TAX', 'YR_BUILT', 'LIVING_AREA', 'BED_RMS', 'FULL_BTH'
]
df = df[cols_to_keep].copy()

# clean
df = df.dropna(subset=['ZIP_CODE', 'ST_NAME'])
df['ZIP_CODE'] = df['ZIP_CODE'].astype(str).str.split('.').str[0].str.zfill(5)

# make str to numeric
df['TOTAL_VALUE'] = pd.to_numeric(df['TOTAL_VALUE'].astype(str).str.replace(r'[^\d.]', '', regex=True), errors='coerce')
df['GROSS_TAX'] = pd.to_numeric(df['GROSS_TAX'].astype(str).str.replace(r'[^\d.]', '', regex=True), errors='coerce')

# replace NaN with None so that threr won't be trouble in future steps
df = df.replace({np.nan: None})

hierarchical_data = []

for (zip_code, st_name), group in df.groupby(['ZIP_CODE', 'ST_NAME']):
    
    properties_list = []
    for _, row in group.iterrows():
        prop = {
            "pid": row['PID'],
            "street_number": row['ST_NUM'],
            "usage": row['LU_DESC'],
            "owner_occupied": row['OWN_OCC'],
            "financials": {
                "total_value": row['TOTAL_VALUE'],
                "gross_tax": row['GROSS_TAX']
            },
            "building_specs": {
                "year_built": row['YR_BUILT'],
                "living_area": row['LIVING_AREA'],
                "bedrooms": row['BED_RMS'],
                "full_baths": row['FULL_BTH']
            }
        }
        properties_list.append(prop)
    
    # aggregate
    valid_values = [p['financials']['total_value'] for p in properties_list if p['financials']['total_value'] is not None]
    avg_value = sum(valid_values) / len(valid_values) if valid_values else 0
    
    # parent doc
    street_doc = {
        "zip_code": zip_code,
        "street_name": st_name,
        "metrics": {
            "total_properties": len(properties_list),
            "average_property_value": round(avg_value, 2)
        },
        "properties": properties_list
    }
    
    hierarchical_data.append(street_doc)

with open('boston_streets_hierarchical.json', 'w', encoding='utf-8') as f:
    json.dump(hierarchical_data, f, indent=2)
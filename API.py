from pymongo import MongoClient
import json

class MongoAPI:
    def __init__(self):
        self.client = MongoClient('localhost', 27017)
        self.db = self.client['Boston_Housing']


    def create_collections(self):
        housing_collection = self.db.housings
        housing_collection.drop()
        return housing_collection

    def insert_housing_json_file(self, json_file, housing_collection):
        with open(json_file) as json_file:
            data = json.load(json_file)
        housing_collection.insert_many(data)


    def find_street(self, housing_collection, zip_code, street_name):
        return housing_collection.find(
            {"zip_code": zip_code, "street_name": street_name},
            {"_id": 0, "zip_code": 1, "street_name": 1, "metrics": 1}
        )

    def street_property_value(self, housing_collection, descending=True):
        if descending:
            return housing_collection.find(
                {},
                { "_id": 0, "zip_code": 1, "street_name": 1, "metrics.average_property_value": 1 }
            ).sort({ "metrics.average_property_value": -1 }).limit(5)
        else:
            return housing_collection.find(
                {"metrics.average_property_value": {"$gt": 0}},
                {"_id": 0, "zip_code": 1, "street_name": 1, "metrics.average_property_value": 1}
            ).sort({"metrics.average_property_value": 1}).limit(5)

    def find_st_before_year(self, housing_collection, year):
        return housing_collection.find(
            {"properties.building_specs.year_built": { "$lt": year}},
            {"_id": 0, "zip_code": 1, "street_name": 1}
        ).limit(5)

    def zipcode_total_access_property(self, housing_collection):
        return housing_collection.aggregate([
            { "$group": {"_id": "$zip_code", "total_assessed_properties": { "$sum": "$metrics.total_properties"}}},
            { "$sort": {"total_assessed_properties": -1}},
            { "$limit": 5}
        ])

    def top_expensive_occupy_pr(self, housing_collection):
        return housing_collection.aggregate([
            { "$unwind": "$properties" },
            { "$match": { "properties.owner_occupied": "Y", "properties.financials.total_value": { "$ne": "null" } } },
            { "$sort": { "properties.financials.total_value": -1 } },
            { "$project": { "_id": 0, "street_name": 1, "properties.street_number": 1, "properties.financials.total_value": 1 } },
            { "$limit": 5 }
        ])

    def living_area_price_filter(self, housing_collection, zip_code, area_filter):
        return housing_collection.find(
            {"zip_code": zip_code, "properties.building_specs.living_area": {"$gt": area_filter}},
            {"_id": 0,"properties": {"$elemMatch": {"building_specs.living_area": {"$gt": area_filter}}}}
        ).sort({"properties.financials.total_value": 1}).limit(5)

    def find_non_owner_properties(self, housing_collection, zip_code, num_bedroom, num_bathroom):
        filter_query = {
            "owner_occupied": "N",
            "building_specs.bedrooms": {"$gte": num_bedroom},
            "building_specs.full_baths": {"$gte": num_bathroom},
        }
        return housing_collection.find(
            {
                "zip_code": zip_code,
                "properties": {"$elemMatch": filter_query}
            },
            {
                "_id": 0,
                "properties": {"$elemMatch": filter_query}
            }
        ).sort("properties.financials.total_value", 1).limit(5)


    def find_most_bedrooms(self, housing_collection):
        return housing_collection.find(
            {"properties.building_specs.bedrooms": {"$gt": 0}},
            {
                "_id": 0,
                "zip_code": 1,
                "street_name": 1,
                "properties": {"$elemMatch": {"building_specs.bedrooms": {"$gt": 0}}}
            }
        ).sort("properties.building_specs.bedrooms", -1).limit(1)

    def top_zipcodes_by_avg_tax(self, housing_collection):
        return housing_collection.aggregate([
            {"$unwind": "$properties"},
            {"$group": {"_id": "$zip_code", "avg_tax": {"$avg": "$properties.financials.gross_tax"}}},
            {"$sort": {"avg_tax": -1}},
            {"$limit": 5},
            {"$project": {"_id": 0, "zip_code": "$_id", "avg_tax": {"$round": ["$avg_tax", 2]}}}
        ])
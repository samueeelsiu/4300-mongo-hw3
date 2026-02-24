from pymongo import MongoClient
from API import MongoAPI

def main():
    api = MongoAPI()

    housing_collection = api.create_collections()
    api.insert_housing_json_file('boston_streets_hierarchical.json', housing_collection)

    Huntington_ave_agg = api.find_street(housing_collection, zip_code='02115', street_name='HUNTINGTON AV')
    print(list(Huntington_ave_agg))

    top_5_st_highest_value = api.street_property_value(housing_collection)
    print(list(top_5_st_highest_value))

    historic_building_before_1800 = api.find_st_before_year(housing_collection, 1800)
    print(list(historic_building_before_1800))

    total_access_property_zipcode = api.zipcode_total_access_property(housing_collection)
    print(list(total_access_property_zipcode))

    top_expensive_property_occupied = api.top_expensive_occupy_pr(housing_collection)
    print(list(top_expensive_property_occupied))

    cheapest_living_area_2000 = api.living_area_price_filter(housing_collection, zip_code='02115', area_filter=2000)
    print(list(cheapest_living_area_2000))

    two_bedr_1_bath_property = api.find_non_owner_properties(housing_collection, zip_code='02115', num_bedroom=2, num_bathroom=1)
    print(list(two_bedr_1_bath_property))

    most_bedroom = api.find_most_bedrooms(housing_collection)
    print(list(most_bedroom))

    top5_zipcode_avg_tax = api.top_zipcodes_by_avg_tax(housing_collection)
    print(list(top5_zipcode_avg_tax))

    top_5_st_lowest_value = api.street_property_value(housing_collection, descending=False)
    print(list(top_5_st_lowest_value))


if __name__ == '__main__':
    main()
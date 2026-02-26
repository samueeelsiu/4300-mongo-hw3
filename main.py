from pymongo import MongoClient
from API import MongoAPI
import seaborn as sns
import matplotlib.pyplot as plt

def main():
    api = MongoAPI()

    housing_collection = api.create_collections()
    api.insert_housing_json_file('boston_streets_hierarchical.json', housing_collection)

    Huntington_ave_agg = list(api.find_street(housing_collection, zip_code='02115', street_name='HUNTINGTON AV'))
    print(Huntington_ave_agg)

    top_5_st_highest_value = list(api.street_property_value(housing_collection))
    print(top_5_st_highest_value)

    historic_building_before_1800 = list(api.find_st_before_year(housing_collection, 1800))
    print(historic_building_before_1800)

    total_access_property_zipcode = list(api.zipcode_total_access_property(housing_collection))
    print(total_access_property_zipcode)

    top_expensive_property_occupied = list(api.top_expensive_occupy_pr(housing_collection))
    print(top_expensive_property_occupied)

    cheapest_living_area_2000 = list(api.living_area_price_filter(housing_collection, zip_code='02115', area_filter=2000))
    print(cheapest_living_area_2000)

    two_bedr_1_bath_property = list(api.find_non_owner_properties(housing_collection, zip_code='02115', num_bedroom=2, num_bathroom=1))
    print(two_bedr_1_bath_property)

    most_bedroom = list(api.find_most_bedrooms(housing_collection))
    print(most_bedroom)

    top5_zipcode_avg_tax = list(api.top_zipcodes_by_avg_tax(housing_collection))
    print(top5_zipcode_avg_tax)

    top_5_st_lowest_value = list(api.street_property_value(housing_collection, descending=False))
    print(top_5_st_lowest_value)


    #visualization for top 5 most expensive street value
    street = [f"{i['street_name']}, {i['zip_code']}" for i in list(top_5_st_highest_value)]
    values = [i["metrics"]["average_property_value"] for i in list(top_5_st_highest_value)]
    sns.barplot(x = street, y = values)
    plt.title("Top Streets by Average Property Value")
    plt.xlabel("Street")
    plt.ylabel("Average Property Value ($)")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.show()



if __name__ == '__main__':
    main()
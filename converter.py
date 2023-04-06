from os import path, makedirs
import pandas as pd

dir = path.dirname(__file__)
complete_df = pd.read_csv(path.join(dir, "input.csv"), sep = ";", encoding = "utf-8")
complete_df["ownership_grp"].replace({"1": "same", "2": "same country", "3": "foreign", "4": "multiple countries"}, inplace = True)
complete_df["employees_p_factory_gt1000"].replace({0: "no", 1: "yes"}, inplace = True)
complete_df["factory_parent_different"].replace({0: "no", 1: "yes"}, inplace = True)
complete_df["EPZ"].replace({0: "no", 1: "yes"}, inplace = True)
complete_df["Product_Categories_grp"].replace({0: "rest", 1: "accessories", 2: "footwear"}, inplace = True)
complete_df["City_factory"] = complete_df["City_factory"].str.lower()
complete_df["Province_State_factory"] = complete_df["Province_State_factory"].str.lower()
# Minimum numbers
complete_df.replace({-9: 0, 0: float("nan"), 1: 1, 2: 1000, 3: 5000}, inplace = True)
# Middle numbers
# complete_df.replace({-9: 0, 0: float("nan"), 1: 500, 2: 2500, 3: 7500}, inplace = True)
# Maximum numbers
# complete_df.replace({-9: 0, 0: float("nan"), 1: 1000, 2: 5000, 3: 10000}, inplace = True)

df = complete_df.drop(columns = ["Parent_companies_name", "Country_parent", "Address_1_factory", "Address_2_factory", "Address_3_factory",
                       "Zip_Code_factory", "countrycode", "country_parent_num", "factory_parent_different", "ownership_grp", 
                       "TOTAL_BRANDS16", "TOTAL_BRANDS16_dich", "femployees_p_factory", "EPZ"])
for column in df.columns:
    if "DICH" in column:
        df.drop(columns = column, inplace = True)
cities_to_be_coded_list = list(set(df["City_factory"].tolist()))
cities_to_be_coded_list = [city for city in cities_to_be_coded_list]

geocode_df = pd.read_csv(path.join(dir, "geocode.csv"), sep = ";", encoding = "utf-8")
cities_names_list = geocode_df["Name"].tolist()
cities_names_list = [city.lower() for city in cities_names_list]
cities_alternate_names_list = geocode_df["Alternate Names"].tolist()
cities_alternate_names_list = [city.lower() for cities in cities_alternate_names_list for city in str(cities).split(",")]
cities_codable_list = cities_names_list + cities_alternate_names_list

# Get entry that is geocodable
cities_coded_dict = {}
cities_not_coded_list = []
for city in cities_to_be_coded_list:
    if city in cities_codable_list:
        cities_coded_dict[city] = city
    else:
        province = df.loc[df["City_factory"].str.contains(city)]["Province_State_factory"].values[0]
        if province in cities_codable_list:
            cities_coded_dict[city] = province
        else:
            cities_not_coded_list.append(city)

# Create column with geocodable entries converted to geocode info
globals()["city_counter"] = 0
def city_encoder(city):
    global city_counter
    city_counter += 1
    print(str(city_counter / len(df.index) * 100)[:5], "%")
    if city in cities_not_coded_list:
        return float("nan")
    geocodable_entry = cities_coded_dict[city]
    if geocodable_entry in cities_names_list:
        row = geocode_df.loc[geocode_df["Name"].str.lower() == geocodable_entry]
    elif geocodable_entry in cities_alternate_names_list:
        row = geocode_df.loc[geocode_df["Alternate Names"].str.lower().str.contains(geocodable_entry, na = False)]
    #geocode = row["Geoname ID"].values[0]
    coordinates = row["Coordinates"].values[0]
    return [geocodable_entry, coordinates]
df["geocode_info"] = df["City_factory"].apply(city_encoder)
# Split geocode_info into several columns because I don't know how to assign several new columns at once
df["geocode_name"] = df["geocode_info"].astype(list)[0]
coordinates = df["geocode_info"][1].str.split(", ")
df["lat"] = coordinates[0]
df["lon"] = coordinates[1]

# Create dataframes for each company
company_list = ["Adidas", "ASICS", "ASOS", "CandA", "Debenhams", "Esprit", "GAP", "HandM", "MandS", "NIKE",
                "Pentland", "Primark", "PUMA", "Under_Armour", "VF_Corp", "Amer_Sports", "Bestseller",
                "KappAhl", "New_Balance", "Levi_Strauss", "PVH", "Tesco", "UNIQLO"]
# Workers
if not path.exists(path.join(dir, "workers")):
    makedirs(path.join(dir, "workers"))
for company in company_list:
    for column in df.columns:
        if company in column:
            company_df = df[["geocode_name", column, "lat", "lon"]].rename(columns = {column: "workers"})
            # Including product categories
            #company_df = df[["geocode", "Product_Categories_grp", column]].rename(columns = {column: "workers"})
            if len(company_df.index) > 0:
                company_df = company_df.loc[company_df["workers"] != 0]
                company_df = company_df.groupby(by = ["geocode_name"], dropna = True).sum(numeric_only = True)
                company_df = company_df.sort_values("workers", ascending = False)
                company_df.to_csv(path.join(dir, "workers", company + ".csv"), sep = ";", encoding = "utf-8", index = False)
# Has factories
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
for column in complete_df.columns:
    if not "DICH" in column:
        # Minimum numbers
        #complete_df[column].replace({-9: 0, 0: float("nan"), 1: 1, 2: 1000, 3: 5000}, inplace = True)
        # Middle numbers
        complete_df[column].replace({-9: 0, 0: float("nan"), 1: 500, 2: 2500, 3: 7500}, inplace = True)
        # Maximum numbers
        #complete_df[column].replace({-9: 0, 0: float("nan"), 1: 1000, 2: 5000, 3: 10000}, inplace = True)

df = complete_df.drop(columns = ["Parent_companies_name", "Country_parent", "Address_1_factory", "Address_2_factory", "Address_3_factory",
                       "Zip_Code_factory", "countrycode", "country_parent_num", "factory_parent_different", "ownership_grp", 
                       "TOTAL_BRANDS16", "TOTAL_BRANDS16_dich", "femployees_p_factory", "EPZ"])
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
    return ", ".join([geocodable_entry, coordinates])
df[["geocode_name", "lat", "lon"]] = df["City_factory"].apply(city_encoder).str.split(", ", expand = True)

# Create dataframes for each company
company_list = ["Adidas", "ASICS", "ASOS", "CandA", "Debenhams", "Esprit", "GAP", "G_STAR", "HandM", "MandS", "NIKE",
                "Pentland", "Primark", "PUMA", "Under_Armour", "VF_Corp", "Amer_Sports", "Bestseller",
                "KappAhl", "New_Balance", "Levi_Strauss", "PVH", "Tesco", "UNIQLO"]

# Function to export final dataframes and export as CSV
def export_company(company_df, company, feature):
    for column in company_df.columns:
        if company in column:
            company_df = company_df[["geocode_name", column, "lat", "lon"]].rename(columns = {column: feature})
            # Including product categories
            #company_df = company_df[["geocode", "Product_Categories_grp", column]].rename(columns = {column: feature})
            if len(company_df.index) > 0:
                company_df = company_df.loc[company_df[feature] != 0]
                company_df = company_df.groupby(by = ["geocode_name", "lat", "lon"], dropna = True).sum(numeric_only = True)
                company_df = company_df.reset_index().rename(columns = {"index": "geocode_name"})
                company_df = company_df.sort_values(feature, ascending = False)
                company_df.to_csv(path.join(dir, feature, company + ".csv"), sep = ";", encoding = "utf-8", index = False)

# Workers
worker_df = df
feature = "workers"
for column in worker_df.columns:
    if "DICH" in column:
        worker_df = worker_df.drop(columns = column)
if not path.exists(path.join(dir, feature)):
    makedirs(path.join(dir, feature))
for company in company_list:
    export_company(worker_df, company, feature)
# Create combined dataframe out of all others and export that

# Has factories
factory_df = df
feature = "factories"
for column in factory_df.columns:
    for company in company_list:
        if company in column:
            if not "DICH" in column:
                factory_df = factory_df.drop(columns = column)
if not path.exists(path.join(dir, feature)):
    makedirs(path.join(dir, feature))
for company in company_list:
    export_company(factory_df, company, feature)
# Create combined dataframe out of all others and export that
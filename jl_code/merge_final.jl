# ===================================================================
# Main SCRIPT
# ===================================================================

using HTTP
using JSON
using DataFrames
using CSV
using Dates
using Impute

script_start_time = Dates.now()
println("SCRIPT STARTED at ", script_start_time)
println("-----------------------------------------\n")

# --- CONFIGURATION ---
dir_path = "C:/Users/Pinguin/Documents/BC/2025_2026/fall/labor_1/research_project_ev/"
data_in_path = joinpath(dir_path, "data/data_raw")
data_intermediate_path = joinpath(dir_path,"data/data_intermediate")
data_out_path = joinpath(dir_path, "data/data_final")

#2012 for now, will look at more years later potentially.
const analysis_years = 2012:2024

#raw data
ev_station_path = joinpath(data_in_path, "ev_stations_output.csv")
election_data_path = joinpath(data_in_path, "countypres_2000-2024.csv")
house_election_path = joinpath(data_in_path, "1976-2022-house.csv")
crosswalk_path = joinpath(data_in_path, "ZIP_COUNTY_032018.csv")
population_data_path = joinpath(data_in_path, "us.1990_2023.20ages.adjusted.txt")
bls_data = joinpath(data_intermediate_path, "BLS_wages_all_years_industries_COMBINED.csv")
migration_data_path = joinpath(dir_path, "data/data_raw", "irs_migration", "inflow")
tesla_ps_ratio_path = joinpath(dir_path, "data/data_raw", "tesla_ps_ratio.csv")
gdp_data_path_07_15 = joinpath(data_in_path,"bea_gdp_coarse","gdp_07_15.csv")
gdp_data_path_16_23 = joinpath(data_in_path,"bea_gdp_coarse","gdp_16_23.csv")


#intermediate data

intermediate_raw_pop_path = joinpath(data_intermediate_path, "intermediate_pop_data.csv")
panel_output_path = joinpath(data_out_path, "all_data_panel_analysis.csv")


pull_new_ev_data = false #part 1
recompile_population_data = true #part 2
run_BLS = false #part 3f, run file_b.jl to get BLS data
run_migration = false #run to recompile migration data from raw files


#load helper functions
include(joinpath(dir_path,"code/jl_code", "helper_functions.jl"))


# --- PART 1: ACQUIRE EV STATION DATA ---

if pull_new_ev_data
    println("--- Part 1: Downloading EV Station Data ---")
    api_key = "lK3vVoWo39cwOfBDpEh29Ma7xqiXV9YBEbNlq3bP"
    api_url = "https://developer.nrel.gov/api/alt-fuel-stations/v1.json?api_key=$(api_key)&fuel_type=ELEC&limit=all"
    
    try
        response = HTTP.get(api_url, readtimeout=9000)
        if response.status == 200
            data = JSON.parse(String(response.body))
            df_api = DataFrame(data["fuel_stations"])
            df_api = mapcols(col -> replace(col, nothing => missing), df_api)
            CSV.write(ev_station_path, df_api)
            println("Successfully downloaded and saved $(nrow(df_api)) stations to:\n", ev_station_path)
        else
            println("ERROR: API request failed with status $(response.status)")
        end
    catch e
        println("ERROR: An unexpected error occurred during API download: $e")
    end
else
    println("--- Part 1: Skipped API Download ---")
end

println("\n--- Part 3: Loading and Preparing IRS Migration Data ---")


if run_migration
    # Get a list of all CSV files in the directory
    migration_files = filter(x -> endswith(x, ".csv"), readdir(migration_data_path, join=true))

    # Create an empty DataFrame to hold all the data
    all_migration_df = DataFrame()

    # Loop through each file, read it, and append it to the main DataFrame
    for file in migration_files
        # Extract the year from the filename (e.g., "countyinflow1112" -> 2012)
        year_str = match(r"countyinflow(\d{2})(\d{2})", basename(file))
        if !isnothing(year_str)
            # Assuming the second capture group is the end year of the period
            year = parse(Int, "20" * year_str.captures[2])
            
            # Read the data using CSV.jl
            temp_df = CSV.read(file, DataFrame)
            temp_df.year = fill(year, nrow(temp_df))
            append!(all_migration_df, temp_df)
        end
    end

    println("Loaded $(nrow(all_migration_df)) total migration records.")

    # Filter for only the rows you need
    migration_us_total = filter(row -> 
        !ismissing(row.y1_countyname) && 
        contains(string(row.y1_countyname), "County Total Migration-US"), 
        all_migration_df
    )

    println("Filtered down to $(nrow(migration_us_total)) 'County Total Migration-US' records.")

    # Create the county_fips code by combining state and county codes
    migration_us_total.y2_statefips = lpad.(string.(migration_us_total.y2_statefips), 2, '0')
    migration_us_total.y2_countyfips = lpad.(string.(migration_us_total.y2_countyfips), 3, '0')
    migration_us_total.county_fips = migration_us_total.y2_statefips .* migration_us_total.y2_countyfips

    # Select and rename columns to be clear for merging
    migration_panel_df = select(migration_us_total, 
        :county_fips, 
        :year, 
        :n1 => :migration_returns, # Number of returns
        :n2 => :migration_individuals, # Number of individuals/exemptions
        :agi => :migration_total_agi # Adjusted Gross Income in thousands
    )
    CSV.write(joinpath(data_intermediate_path, "irs_migration_panel.csv"), migration_panel_df)
else
    migration_panel_df = CSV.read(joinpath(data_intermediate_path, "irs_migration_panel.csv"), DataFrame)
    println("Loaded pre-compiled IRS migration data with $(nrow(migration_panel_df)) records.")
end


# --- PART 2: LOAD ALL DATASETS FROM DISK ---

println("\n--- Part 2: Loading All Datasets ---")
stations_df = CSV.read(ev_station_path, DataFrame)
stations_df = select(stations_df,[:open_date, :latitude, :longitude, :city, :access_code, :street_address, :state, :facility_type,:zip])
dropmissing!(stations_df, :zip)
stations_df.zip = lpad.(string.(stations_df.zip), 5, '0')

election_df = CSV.read(election_data_path, DataFrame)

crosswalk_df = CSV.read(crosswalk_path, DataFrame)
house_df = CSV.read(house_election_path, DataFrame)
if "county_fip" in names(house_df)
    rename!(house_df, :county_fip => :county_fips)
end

println("CSV files loaded successfully.")

# --- PART 3: PREPARE AND CLEAN EACH DATASET ---

println("\n--- Part 3: Preparing and Cleaning Data ---")

# 3a: Prepare Crosswalk Data
# 3a: Prepare Crosswalk Data
println("Preparing crosswalk data from new HUD file...")

# The new HUD file uses 'COUNTY' for the FIPS code and 'ZIP' for the zip code.
# We will use these new names to clean the data before renaming them.

# 1. Drop rows where the FIPS code is missing.
dropmissing!(crosswalk_df, :county)

# 2. Ensure all FIPS codes are strings padded with leading zeros to 5 digits.
crosswalk_df.county = lpad.(string.(crosswalk_df.county), 5, '0')

# 3. Rename columns to the standard names used in the rest of the script.
rename!(crosswalk_df, :county => :county_fips)

# 4. Clean the zip codes (they are already strings, but good practice).
crosswalk_df.zip = lpad.(string.(crosswalk_df.zip), 5, '0')

# 5. Ensure there is only one county per zip code.
crosswalk_df = unique(select(crosswalk_df, :zip, :county_fips), [:zip])

println("Crosswalk data prepared successfully.")

# 3b: Prepare Population Data
if recompile_population_data
    println("Parsing fixed-width population data file...")
    raw_pop_df = DataFrame(Year=Int[], StateFIPS=String[], CountyFIPS_suffix=String[], Race=Int[], Origin=Int[], Sex=Int[], AgeGroup=Int[], Population=Int[])
    open(population_data_path, "r") do f
        for line in eachline(f)
            try
                push!(raw_pop_df, (
                    parse(Int, line[1:4]), 
                    line[7:8], 
                    line[9:11], 
                    parse(Int, line[14:14]), 
                    parse(Int, line[15:15]), 
                    parse(Int, line[16:16]), 
                    parse(Int, line[17:18]), 
                    parse(Int, line[19:26])
                ))
            catch
                # Skip malformed lines
            end
        end
    end
    raw_pop_df.county_fips = clean_fips(raw_pop_df.StateFIPS .* raw_pop_df.CountyFIPS_suffix)
    println("Successfully parsed $(nrow(raw_pop_df)) demographic records.")
    CSV.write(intermediate_raw_pop_path,raw_pop_df)
else
    raw_pop_df = CSV.read(intermediate_raw_pop_path, DataFrame)
    println("Successfully loaded $(nrow(raw_pop_df)) demographic records.")
end

#change race =5 if origin = 1 for hispanic people
raw_pop_df[raw_pop_df.Origin .== 1, :Race] .= 5

race_map = Dict(1 => "White", 2 => "Black", 3 => "American_Indian_Alaska_Native", 4 => "Asian_Pacific_Islander", 5 => "Hispanic")
sex_map = Dict(1 => "Male", 2 => "Female")
raw_pop_df.race_text = get.(Ref(race_map), raw_pop_df.Race, "Unknown")
raw_pop_df.sex_text = get.(Ref(sex_map), raw_pop_df.Sex, "Unknown")

raw_pop_df.age_category = categorize_age.(raw_pop_df.AgeGroup)


pop_race_panel = aggregate_and_widen(raw_pop_df, [:county_fips, :Year, :race_text], :race_text, "pop_race_")
pop_sex_panel = aggregate_and_widen(raw_pop_df, [:county_fips, :Year, :sex_text], :sex_text, "pop_sex_")
pop_age_panel = aggregate_and_widen(raw_pop_df, [:county_fips, :Year, :age_category], :age_category, "pop_")

total_pop_panel = combine(groupby(raw_pop_df, [:county_fips, :Year]), :Population => sum => :population)
rename!(total_pop_panel, :Year => :year)

rename!(pop_race_panel, :Year => :year)
rename!(pop_sex_panel, :Year => :year)
rename!(pop_age_panel, :Year => :year)

# Now, join everything on the consistent key names: [:county_fips, :year]
demographics_panel_df = innerjoin(total_pop_panel, pop_race_panel, on=[:county_fips, :year])
demographics_panel_df = innerjoin(demographics_panel_df, pop_sex_panel, on=[:county_fips, :year])
demographics_panel_df = innerjoin(demographics_panel_df, pop_age_panel, on=[:county_fips, :year])

for col in names(demographics_panel_df, r"^pop_")
    new_col_name = "pct_" * chopprefix(col, "pop_")
    demographics_panel_df[!, new_col_name] = demographics_panel_df[!, col] ./ demographics_panel_df.population
end
demographics_panel_df.county_fips = clean_fips(demographics_panel_df.county_fips)

println("Yearly demographic panel data prepared.")

# 3c: Prepare Election Data
dropmissing!(election_df, :county_fips)
election_df.county_fips = clean_fips(election_df.county_fips)
election_filtered = filter(row -> !ismissing(row.office) && row.office == "US PRESIDENT" && row.year in analysis_years && !ismissing(row.party) && lowercase(row.party) in ["democrat", "republican"], election_df)

# Add state and state_po to the groupby and unstack keys
election_agg = combine(groupby(election_filtered, [:county_fips, :year, :state, :state_po, :party]), 
    :candidatevotes => sum => :candidatevotes, 
    :totalvotes => first => :totalvotes
)
election_agg.vote_share = election_agg.candidatevotes ./ election_agg.totalvotes

election_panel_df = unstack(election_agg, [:county_fips, :year, :state, :state_po], :party, :vote_share)
rename!(lowercase, election_panel_df)
election_panel_df = filter(row -> length(row.county_fips) == 5, election_panel_df)
println("Election panel data prepared.")


# 3d: Prepare House Midterm Election Data (skip, not the right data, its district level)
#=
# -----------------------------------------------------
println("Preparing House midterm election data.")

# Clean FIPS codes to ensure they are 5-digit strings
dropmissing!(house_df, :county_fips)
house_df.county_fips = lpad.(replace.(string.(house_df.county_fips), r"\.0$" => ""), 5, '0')

# Filter for relevant years and parties
house_filtered = filter(row -> 
    !ismissing(row.year) && row.year in analysis_years && 
    !ismissing(row.party) && lowercase(row.party) in ["democrat", "republican"], 
    house_df
)

# Aggregate results to handle multiple candidates from the same party (probably wont end up using this data, its only on the district level)
house_agg = combine(
    groupby(house_filtered, [:county_fips, :year, :party]), 
    :candidatevotes => sum => :candidatevotes, 
    :totalvotes => first => :totalvotes
)

# Calculate vote share
house_agg.vote_share = house_agg.candidatevotes ./ house_agg.totalvotes

# Unstack to create a panel with columns for democrat and republican vote share
house_election_panel_df = unstack(house_agg, [:county_fips, :year], :party, :vote_share)

# Rename new columns (e.g., "DEMOCRAT") to lowercase ("democrat")
rename!(lowercase, house_election_panel_df)

println("House midterm election panel data prepared.")


# 3e: Prepare EV Station Data
dropmissing!(stations_df, :zip)
stations_df.zip = lpad.(string.(stations_df.zip), 5, '0')
println("EV Station data prepared.")
=#

# Step 3f

if run_BLS
    include(joinpath(dir_path,"jl_code", "bls_merge_final.jl"))
else
    bls_all = CSV.read(bls_data, DataFrame)  
    println("Loaded pre-compiled BLS wage data.")
end


rename!(bls_all,:area_fips => :county_fips)
rename!(bls_all,:qtr => :quarter)

bls_all.county_fips = clean_fips(bls_all.county_fips) 


# 3g: Prepare Tesla Price-to-Sales Ratio Data
println("\n--- Part 3G: Loading Tesla Price-to-Sales Ratio Data ---")

ps_ratio_raw_df = CSV.read(tesla_ps_ratio_path, DataFrame)

# Rename columns for easier use
rename!(ps_ratio_raw_df, 
    Symbol("Date") => :date_raw,
    Symbol("Stock Price") => :stock_price, # Adjust if the column name is different
    Symbol("Price to Sales Ratio") => :ps_ratio_tesla
)

# Parse dates and create year and quarter columns
# The date format in the CSV might be different, assuming "m/d/yyyy"
ps_ratio_raw_df.date = [try Date(d, "m/d/yyyy") catch; missing end for d in ps_ratio_raw_df.date_raw]
dropmissing!(ps_ratio_raw_df, :date) # Drop rows where date parsing failed

ps_ratio_raw_df.year = year.(ps_ratio_raw_df.date)
ps_ratio_raw_df.quarter = quarterofyear.(ps_ratio_raw_df.date)

# Select the final columns for the panel
ps_ratio_panel_df = select(ps_ratio_raw_df, :year, :quarter, :ps_ratio_tesla)

println("Processed P/S ratio data.")

#3h: merge in gdp data

gdp_data_1 = CSV.read(gdp_data_path_07_15, DataFrame)
gdp_data_2 = CSV.read(gdp_data_path_16_23, DataFrame)

dropmissing!(gdp_data_1, :GeoFips)
dropmissing!(gdp_data_2, :GeoFips)

select!(gdp_data_1, Not(:GeoName))
select!(gdp_data_2, Not(:GeoName))

gdp_data = leftjoin(gdp_data_1,gdp_data_2,on = [:GeoFips])

rename!(gdp_data,:GeoFips => :county_fips)

filter!(:county_fips => x -> length(x) < 6, gdp_data)

gdp_data.county_fips = lpad.(string.(gdp_data.county_fips), 5, '0')


gdp_data_long = stack(gdp_data, Not(:county_fips), variable_name=:year, value_name=:gdp)

gdp_data_long.year = parse.(Int,gdp_data_long.year)

#gdp_data = replace(gdp_data, "(NA)" => missing) 

function safe_parse(value)
    parsed = tryparse(Int, value)
    return parsed === nothing ? missing : parsed
end

# 2. Apply that function to the entire 'gdp' column
# This overwrites the old column with the new, clean one.
# Make sure to use the dot '.' for broadcasting!

gdp_data_long.gdp = safe_parse.(gdp_data_long.gdp)

# --- PART 4: BUILD THE QUARTERLY PANEL DATASET  ---

println("\n--- Part 4: Building Quarterly Panel Dataset ---")

# Step 4a: Prepare station data and add county fips
# ----------------------------------------------------
stations_df.open_date_parsed = [try Date(d) catch; missing end for d in stations_df.open_date]
dropmissing!(stations_df, :open_date_parsed)
stations_with_county = innerjoin(stations_df, crosswalk_df, on = :zip)

# Step 4b: Create the quarterly panel "spine"
# -------------------------------------------
unique_counties = unique(vcat(stations_with_county.county_fips, demographics_panel_df.county_fips, election_panel_df.county_fips))
quarters = DataFrame(Iterators.product(analysis_years, 1:4), [:year, :quarter])
panel_spine = crossjoin(DataFrame(county_fips = unique_counties), quarters)
println("Created panel spine with $(nrow(panel_spine)) county-quarter observations.")

# Step 4c: Reconstruct historical EV station counts quarterly
# -----------------------------------------------------------
# This block is now corrected to group by quarter and find the end-of-quarter date
panel_ev_counts = combine(groupby(panel_spine, [:county_fips, :year, :quarter])) do group
    current_county = first(group.county_fips)
    current_year = first(group.year)
    current_qtr = first(group.quarter)
    
    # Calculate the last day of the current quarter
    qtr_end_month = current_qtr * 3
    qtr_end_day = daysinmonth(current_year, qtr_end_month)
    qtr_end_date = Date(current_year, qtr_end_month, qtr_end_day)
    
    # Filter stations that existed in that county by that quarter's end date
    stations_in_period = filter(row -> 
        row.county_fips == current_county && 
        row.open_date_parsed <= qtr_end_date, 
        stations_with_county
    )
    
    return (
        station_count = nrow(stations_in_period),
        #total_level2_chargers = sum(skipmissing(stations_in_period.ev_level2_evse_num)),
        #total_dc_fast_chargers = sum(skipmissing(stations_in_period.ev_dc_fast_num))
    )
end

panel_ev_counts = transform(
    sort(panel_ev_counts, [:county_fips, :year, :quarter]),
    :station_count => (x -> [missing; diff(x) ./ x[1:end-1]]) => :pct_change_station_count
)

println("Reconstructed historical EV station counts for each quarter.")

# Step 4d: Merge all panel datasets together
# ------------------------------------------
panel_ev_counts.county_fips = string.(panel_ev_counts.county_fips)
demographics_panel_df.county_fips = string.(demographics_panel_df.county_fips)
election_panel_df.county_fips = string.(election_panel_df.county_fips)
bls_all.county_fips = string.(bls_all.county_fips)
migration_panel_df.county_fips = string.(migration_panel_df.county_fips)


final_panel = leftjoin(panel_ev_counts, demographics_panel_df, on = [:county_fips, :year])
final_panel = leftjoin(final_panel,gdp_data_long, on = [:county_fips, :year])
final_panel = leftjoin(final_panel, election_panel_df, on = [:county_fips, :year])
final_panel = leftjoin(final_panel, bls_all, on = [:county_fips, :year,:quarter])
final_panel = leftjoin(final_panel, migration_panel_df, on = [:county_fips, :year])
final_panel = leftjoin(final_panel, ps_ratio_panel_df, on = [:year, :quarter])
println("Merging complete.")


#CSV.write(panel_output_path, clean_panel) 


# Step 4e: Forward-fill the annual data (population and elections)
# ----------------------------------------------------------------
sort!(final_panel, [:county_fips, :year, :quarter])

for group in groupby(final_panel, :county_fips)
    # --- ADD THESE TWO LINES ---
    if "state" in names(group) && !all(ismissing, group.state)
        group.state = Impute.locf(group.state)
    end
    if "state_po" in names(group) && !all(ismissing, group.state_po)
        group.state_po = Impute.locf(group.state_po)
    end
    # --- END OF ADDED LINES ---

    if "democrat" in names(group) && !all(ismissing, group.democrat)
        group.democrat = Impute.locf(group.democrat)
    end
    if "republican" in names(group) && !all(ismissing, group.republican)
        group.republican = Impute.locf(group.republican)
    end
    # Add other annual columns you want to forward-fill here
    # e.g., population columns
end
println("Merged all panel data sources and forward-filled annual values.")

#=
# Step 4b: Create the panel "spine"
# ---------------------------------
# Parse the open_date column and join with crosswalk to add county_fips to each station
stations_df.open_date_parsed = [try Date(d) catch; missing end for d in stations_df.open_date]
dropmissing!(stations_df, :open_date_parsed)
stations_with_county = innerjoin(stations_df, crosswalk_df, on = :zip)

# The line to get all unique counties remains the same
unique_counties = unique(vcat(stations_with_county.county_fips, demographics_panel_df.county_fips, election_panel_df.county_fips))

# --- NEW QUARTERLY LOGIC ---
# First, create a small DataFrame of all year-quarter combinations we want in our panel
quarters = DataFrame(Iterators.product(analysis_years, 1:4), [:year, :quarter])

# Then, create the panel spine by taking the cross product of
# all unique counties and all year-quarter pairs.
panel_spine = crossjoin(DataFrame(county_fips = unique_counties), quarters)

# Step 4c: Reconstruct historical EV station counts
# -------------------------------------------------
panel_ev_counts = combine(groupby(panel_spine, [:county_fips, :year])) do group
    current_county=first(group.county_fips)
    current_year=first(group.year)
    year_end_date=Date(current_year,12,31)
    stations_in_period=filter(row->row.county_fips==current_county && row.open_date_parsed<=year_end_date, stations_with_county)
    return (
        station_count=nrow(stations_in_period), 
        total_level2_chargers=sum(skipmissing(stations_in_period.ev_level2_evse_num)), 
        total_dc_fast_chargers=sum(skipmissing(stations_in_period.ev_dc_fast_num))
    )
end
println("Reconstructed historical EV station counts.")

# Step 4d: Merge all panel datasets together
# ------------------------------------------
final_panel = leftjoin(panel_ev_counts, demographics_panel_df, on = [:county_fips, :year])
final_panel = leftjoin(final_panel, election_panel_df, on = [:county_fips, :year])

sort!(final_panel, [:county_fips, :year, :quarter])
for group in groupby(final_panel, :county_fips)
    if !all(ismissing, group.democrat)
        group.democrat = Impute.locf(group.democrat)
    end
    if !all(ismissing, group.republican)
        group.republican = Impute.locf(group.republican)
    end
end
println("Merged all panel data sources and forward-filled election data.")
=#

# --- PART 5: CREATE ANALYTICAL METRICS ---

println("\n--- Part 5: Creating Analytical Metrics ---")
final_panel.stations_per_100k_capita = (final_panel.station_count ./ final_panel.population) .* 100000
final_panel.dem_margin = coalesce.(final_panel.democrat, 0) .- coalesce.(final_panel.republican, 0)
println("Derived metrics created.")

clean_panel = mapcols(col -> [isnothing(v) ? missing : v for v in col], final_panel)
# --- PART 6: SAVE THE FINAL PANEL FILE ---

println("\n--- Part 6: Saving Final Panel File ---")
for col in names(final_panel)
    if eltype(final_panel[!, col]) >: Missing
        final_panel[!, col] = coalesce.(final_panel[!, col], 0)
    end
end

cols_to_fill = Symbol[]
for name in names(clean_panel)
    if startswith(name, "pop_") || startswith(name, "pct_") || startswith(name, "migration_") || name in ["population", "democrat", "republican", "dem_margin"]
        push!(cols_to_fill, Symbol(name))
    end
end

println("Identified $(length(cols_to_fill)) annual columns to forward-fill.")
println(cols_to_fill) # To see the full list

# --- Step 2: Ensure the data is sorted by county and time ---
sort!(clean_panel, [:county_fips, :year, :quarter])
println("DataFrame sorted by county and time.")

# --- Step 3: Group by county and apply the forward-fill ---
println("Applying Last Observation Carried Forward (LOCF)...")

# Use @time to see how long it takes
@time for group in groupby(clean_panel, :county_fips)
    for col in cols_to_fill
        # Check if the column exists in the group and has non-missing data to fill from
        if col in names(group) && !all(ismissing, group[!, col])
            # Impute.locf fills missing values with the last observation
            group[!, col] = Impute.locf(group[!, col])
        end
    end
end

println("✅ Forward-fill complete.")





CSV.write(panel_output_path, clean_panel)
println("✅ Success! Final panel data file saved to:\n", panel_output_path)

println("\nFinal dataset preview:")
display(first(final_panel, 10))

script_end_time = Dates.now()
elapsed_time = script_end_time - script_start_time
println("\n-----------------------------------------")
println("SCRIPT FINISHED at ", script_end_time)
println("Total elapsed time: ", lpad(floor(Int, Dates.value(elapsed_time) / 60000), 2, '0'), ":", lpad(floor(Int, (Dates.value(elapsed_time) / 1000) % 60), 2, '0'))
println("-----------------------------------------")
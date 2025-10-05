using DataFrames
using CSV
using GeoDataFrames
using GLMakie
using GeoMakie
using Colors
using Statistics

# ----------------------------------------------------------------------
# STEP 1: INITIAL SETUP & INFLATION DATA (RUNS ONCE)
# ----------------------------------------------------------------------
shapefile_path = "C:\\Users\\Pinguin\\Documents\\BC\\2025_2026/fall\\labor_1\\research_project_ev\\data_raw/tl_2024_us_county/tl_2024_us_county.shp"
output_path = "C:\\Users\\Pinguin\\Documents\\BC\\2025_2026/fall\\labor_1\\research_project_ev\\figures/"

# !! UPDATED !!: CPI data now includes 2020-2022
const CPI_DATA = Dict(
    2012 => 229.594, 2013 => 232.957, 2014 => 236.736,
    2015 => 237.017, 2016 => 240.007, 2017 => 245.120,
    2018 => 251.107, 2019 => 255.657, 2020 => 258.811,
    2021 => 270.970, 2022 => 292.655 # New data and new base year
)
# !! UPDATED !!: Base year is now 2022
const CPI_BASE_YEAR = 2022
const CPI_BASE_VALUE = CPI_DATA[CPI_BASE_YEAR]

println("Loading shapefile...")
counties_gdf = GeoDataFrames.read(shapefile_path)
counties_gdf = select(counties_gdf, [:geometry, :GEOID, :STATEFP])
println("Shapefile loaded successfully.")

fips_to_exclude = Set(["02", "15", "60", "66", "69", "72", "78"])
continental_counties_gdf = filter(row -> row.STATEFP ∉ fips_to_exclude, counties_gdf)
println("Filtered for Continental US counties.")

raw_wage_df = bls_all
println("Full wage data loaded from 'bls_all' DataFrame.")

# ----------------------------------------------------------------------
# STEP 2: LOOP THROUGH YEARS AND WAGE TYPES TO GENERATE MAPS
# ----------------------------------------------------------------------

# !! UPDATED !!: Year range is now 2012 to 2022
years_to_process = 2012:2022
wage_types_to_process = [
    (:adj_low_wage, "Low-Skill"),
    (:adj_high_wage, "High-Skill")
]

for year in years_to_process
    
    inflation_factor = CPI_BASE_VALUE / CPI_DATA[year]
    yearly_wage_df = filter(row -> row.year == year, raw_wage_df)
    
    if isempty(yearly_wage_df)
        println("WARNING: No data found for year $year. Skipping.")
        continue
    end

    transform!(yearly_wage_df,
        :low_avg_wkly_wage => (c -> c .* inflation_factor) => :adj_low_wage,
        :high_avg_wkly_wage => (c -> c .* inflation_factor) => :adj_high_wage
    )

    for (wage_col, wage_str) in wage_types_to_process
        
        println("\n--- Processing: Year $year, Wage Type: $wage_str (Inflation Adjusted) ---")

        annual_wage_df = combine(
            groupby(yearly_wage_df, :county_fips),
            wage_col => mean => :avg_wage
        )
        
        annual_wage_df.GEOID = lpad.(string.(annual_wage_df.county_fips), 5, '0')
        merged_gdf = innerjoin(continental_counties_gdf, annual_wage_df, on = :GEOID)
        println("Found and merged $(nrow(merged_gdf)) counties for $year.")

        if nrow(merged_gdf) == 0
            println("WARNING: Merge resulted in an empty table for $year, $wage_str. Skipping plot.")
            continue
        end

        color_data = replace(merged_gdf.avg_wage, missing => NaN)

        fig = Figure(size = (1200, 800))
        ga = GeoAxis(
            fig[1, 1],
            dest = "+proj=aea +lat_1=29.5 +lat_2=45.5 +lat_0=37.5 +lon_0=-96",
            title = "Average $(wage_str) Weekly Wage ($year) - in $(CPI_BASE_YEAR) Dollars",
            xgridvisible = false, ygridvisible = false,
            xticklabelsvisible = false, yticklabelsvisible = false
        )

        poly!(
            ga, merged_gdf.geometry,
            color = color_data, colormap = :viridis,
            strokecolor = :black, strokewidth = 0.25,
            colorrange = (minimum(filter(!isnan, color_data)), maximum(filter(!isnan, color_data)))
        )

        Colorbar(fig[1, 2], colormap = :viridis, label = "Avg Weekly Wage ($(CPI_BASE_YEAR) Dollars)", colorrange = (minimum(filter(!isnan, color_data)), maximum(filter(!isnan, color_data))))
        
        output_filename = joinpath(output_path, "wages_$(lowercase(wage_str))_skill_$(year)_adj.png")
        save(output_filename, fig)

        println("Successfully saved map to $(output_filename)")
    end
end

println("\nAll 22 inflation-adjusted maps have been generated successfully.")
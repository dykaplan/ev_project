using DataFrames
using CSV
using FixedEffectModels
using Statistics
using Plots
using ShiftedArrays
using GLM
using Match

# ===================================================================
# PART 1: SETUP AND LOAD DATA
# ===================================================================

println("--- Part 1: Setup ---")
# --- Configuration ---
dir_path = "C:/Users/Pinguin/Documents/BC/2025_2026/fall/labor_1/research_project_ev/"
data_in_path = joinpath(dir_path, "data/data_clean")
tables_path = joinpath(dir_path, "tables_figures/tables")
plots_path = joinpath(dir_path, "tables_figures/figures")
mkpath(tables_path)
mkpath(plots_path)

# Load the quarterly panel data
panel_path = joinpath(dir_path, "data/data_final", "all_data_panel_analysis.csv")
analysis_df = CSV.read(panel_path, DataFrame)

println("Successfully loaded quarterly panel data with $(nrow(analysis_df)) observations.")

# ===================================================================
# PART 2: AGGREGATE TO YEARLY PANEL
# ===================================================================

println("\n--- Part 2: Aggregating to a yearly panel ---")

# --- Define a safe function to get the first non-missing value ---
# This avoids an error if a group has only missing values for a column
function safe_first(v)
    itr = skipmissing(v)
    return isempty(itr) ? missing : first(itr)
end

# --- Aggregation to Yearly ---
wage_cols = [
    :wage_construction, :wage_manufacturing, :wage_retail,
    :wage_adminwaste, :wage_healthsocial, :wage_highskill,
    :wage_entertainment
]
other_cols = [:population, :station_count, :migration_individuals]

grouped_data = groupby(analysis_df, [:county_fips, :year])

aggregations = [
    [col => (x -> mean(skipmissing(x))) => col for col in wage_cols]...,
    # Use the new safe_first function for population
    :population => safe_first => :population,
    :migration_individuals => safe_first => :migration_individuals,
    # The maximum() function is already safe for all-missing groups
    :station_count => (x -> maximum(skipmissing(x))) => :station_count
]
yearly_df = combine(grouped_data, aggregations...)
println("Aggregated to $(nrow(yearly_df)) county-year observations.")


# ===================================================================
# PART 3: ROLLING WINDOW INTERACTION ANALYSIS
# ===================================================================

println("\n--- Part 3: Setting up the Rolling Window Analysis ---")

# --- Initial Data Preparation (Done Once) ---
model_data_master = yearly_df

# Define sectors
lhs_sectors = [
    "wage_construction", "wage_manufacturing", "wage_retail",
    "wage_adminwaste", "wage_healthsocial", "wage_entertainment"
]
rhs_sector = "wage_highskill"

# DataFrame to store all results over time
results_over_time = DataFrame(
    end_year = Int[],
    dependent_variable = String[],
    interaction_coefficient = Float64[],
    std_error = Float64[],
    p_value = Float64[],
    n_observations = Int[]
)


#Now Diff-in-Diff


counties_with_stations = filter(row -> !ismissing(row.station_count) && row.station_count > 0, analysis_df)

# 2. Group the filtered data by year and calculate the median of the per-capita metric
median_per_capita_by_year = combine(
    groupby(counties_with_stations, :year),
    :stations_per_100k_capita => (x -> median(skipmissing(x))) => :median_stations_per_100k
)



# Assume 'yearly_df' is your fully prepared yearly panel DataFrame

# Define the fixed adoption threshold based on your analysis
const ADOPTION_THRESHOLD = 1.2

println("Finding counties that NEVER crossed the $(ADOPTION_THRESHOLD) stations/100k capita threshold...")

# 1. Find the set of all counties that EVER crossed the threshold at any point in time
treated_counties = Set(
    filter(row -> !ismissing(row.stations_per_100k_capita) && row.stations_per_100k_capita > ADOPTION_THRESHOLD, analysis_df).county_fips
)

# 2. Find the set of all unique counties in the entire dataset
all_counties = Set(analysis_df.county_fips)

# 3. The control group is the set of all counties minus the set of treated counties
control_counties = setdiff(all_counties, treated_counties)

# 4. Report the findings
println("\n--- Results ---")
println("Total unique counties in dataset: $(length(all_counties))")
println("Number of counties that EVER crossed the threshold (Treated Group): $(length(treated_counties))")
println("Number of counties that NEVER crossed the threshold (Control Group): $(length(control_counties))")

println("\nThis group of $(length(control_counties)) counties can serve as the 'never-treated' control group for a DiD analysis.")





println("\n--- Part 2: Aggregating to a yearly panel ---")

# Programmatically find all columns to aggregate for the matching model
wage_cols = names(analysis_df, r"^wage_")
demographic_cols = names(analysis_df, r"^(pop_|pct_)")
migration_cols = names(analysis_df, r"^migration_")
other_cols = ["population", "station_count", "stations_per_100k_capita"]
all_cols_to_agg = vcat(Symbol.(wage_cols), Symbol.(demographic_cols), Symbol.(migration_cols), Symbol.(other_cols)) |> unique

grouped_data = groupby(analysis_df, [:county_fips, :year])
aggregations = [col => (x -> occursin("wage", string(col)) ? mean(skipmissing(x)) : first(skipmissing(x))) => col for col in all_cols_to_agg if col != :station_count]
push!(aggregations, :station_count => (x -> maximum(skipmissing(x))) => :station_count)

yearly_df = combine(grouped_data, aggregations...)
println("Aggregated to $(nrow(yearly_df)) county-year observations with all necessary columns.")

# ===================================================================
# PART 3: PREPARE DATA FOR MATCHING
# ===================================================================

println("\n--- Part 3: Preparing Data for Matching ---")
const ADOPTION_THRESHOLD = 1.2

treated_fips = Set(filter(row -> !ismissing(row.stations_per_100k_capita) && row.stations_per_100k_capita > ADOPTION_THRESHOLD, yearly_df).county_fips)
baseline_df = filter(row -> row.year == 2012, yearly_df)
baseline_df.eventual_treat = [fips in treated_fips for fips in baseline_df.county_fips]

# ===================================================================
# PART 4: "KITCHEN SINK" PROPENSITY SCORE MATCHING
# ===================================================================

println("\n--- Part 4: Performing 'Kitchen Sink' Propensity Score Matching ---")

# 4a. Define all predictors for the matching model
# Exclude one from each category to avoid perfect multicollinearity
race_predictors = filter(x -> x != "pct_race_White", names(baseline_df, r"^pct_race_"))
age_predictors = filter(x -> x != "pct_Age_0_4", names(baseline_df, r"^pct_Age_"))
matching_predictors_str = vcat("population", wage_cols, migration_cols, race_predictors, age_predictors)
matching_predictors_sym = Symbol.(matching_predictors_str) |> unique

# Clean baseline data for all predictors
dropmissing!(baseline_df, matching_predictors_sym)
filter!(row -> row.population > 0, baseline_df)
for col in Symbol.(wage_cols)
    filter!(row -> row[col] > 0, baseline_df)
end

# 4b. Programmatically build the formula
rhs_terms = term(:log_population)
for col_str in vcat(wage_cols, migration_cols, race_predictors, age_predictors)
    s_col = Symbol(col_str)
    # Log-transform wages, use others as is
    t = occursin("wage", col_str) ? :(log($(s_col))) : s_col
    rhs_terms += term(t)
end

# 4c. Run the comprehensive logit model
ps_model = glm(@formula(eventual_treat ~ $(rhs_terms)), baseline_df, Binomial(), LogitLink())
baseline_df.pscore = predict(ps_model)

# 4d. Perform matching
matched_baseline = pairmatch(baseline_df, :eventual_treat, :pscore)
matched_fips = Set(matched_baseline.county_fips)
println("Matching complete. Found $(nrow(filter(row -> row.eventual_treat, matched_baseline))) matched pairs.")

# ===================================================================
# PART 5: RUN DiD ON THE MATCHED SAMPLE
# ===================================================================

println("\n--- Part 5: Running DiD on the Matched Sample ---")

matched_panel_df = filter(row -> row.county_fips in matched_fips, yearly_df)

treatment_timing = combine(
    groupby(filter(row -> !ismissing(row.stations_per_100k_capita) && row.stations_per_100k_capita > ADOPTION_THRESHOLD, matched_panel_df), :county_fips),
    :year => minimum => :treatment_year
)
did_matched_df = leftjoin(matched_panel_df, treatment_timing, on = :county_fips)
did_matched_df.post_treatment = ifelse.(.!ismissing.(did_matched_df.treatment_year) .& (did_matched_df.year .>= did_matched_df.treatment_year), 1, 0)

model_data = dropmissing(did_matched_df, :wage_construction)
matched_model = reg(model_data,
    @formula(log(wage_construction) ~ post_treatment + fe(county_fips) + fe(year))
)

println("\n--- DiD RESULTS ON FULLY MATCHED SAMPLE ---")
println(matched_model)







# --- 1. Define all predictors for the "kitchen sink" model ---
predictors = [
    :population, 
    :wage_highskill, 
    :wage_retail, 
    :migration_individuals,
    :pct_race_White,
    :pct_Age_15_29,
    :pct_Age_60_79,
    :ps_ratio_tesla
]

# --- 2. Create Lagged Predictors and the Binary Outcome ---
logit_df = transform(
    groupby(analysis_df, :county_fips),
    # Create lagged versions of all predictors
    [col => lag => Symbol(string(col) * "_lag") for col in predictors]...,
    # Create the binary outcome variable
    :station_count => (sc -> sc .> 0) => :has_stations
)

# --- 3. Prepare Data for the Model ---
# Create a list of all lagged predictor names for easy reference
lagged_predictors = [Symbol(string(col) * "_lag") for col in predictors]

# Drop any rows with missing data for our outcome or predictors
model_data = dropmissing(logit_df, vcat(:has_stations, lagged_predictors))

# Ensure variables for log transformation are positive
filter!(row -> row.population_lag > 0 && 
               row.wage_highskill_lag > 0 &&
               row.wage_retail_lag > 0, 
        model_data)

println("Prepared data for logit model with $(nrow(model_data)) observations.")

# --- 4. Run the Logit Model ---
logit_model = glm(
    @formula(has_stations ~ log(population_lag) + 
                            log(wage_highskill_lag) + 
                            log(wage_retail_lag) +
                            migration_individuals_lag +
                            pct_race_White_lag + 
                            pct_Age_15_29_lag + 
                            pct_Age_60_79_lag +
                            ps_ratio_tesla_lag),
    model_data,
    Binomial(),
    LogitLink()
)

println("\n--- \"KITCHEN SINK\" LOGIT MODEL RESULTS ---")
println(logit_model)
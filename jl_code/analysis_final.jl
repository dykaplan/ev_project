# ===================================================================
# ANALYSIS SCRIPT (Final, Level Models Only)
# --- Rolling Window with Lagged/Unlagged Per-Capita Analysis ---
# ===================================================================

using DataFrames
using CSV
using FixedEffectModels
using Statistics
using Plots
using ShiftedArrays
using StatsModels
using RegressionTables

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

function safe_first(v)
    itr = skipmissing(v)
    return isempty(itr) ? missing : first(itr)
end

wage_cols = [
    :wage_construction, :wage_manufacturing, :wage_retail,
    :wage_adminwaste, :wage_healthsocial, :wage_highskill,
    :wage_entertainment
]
grouped_data = groupby(analysis_df, [:county_fips, :year])
aggregations = [
    [col => (x -> mean(skipmissing(x))) => col for col in wage_cols]...,
    :population => safe_first => :population,
    :migration_individuals => safe_first => :migration_individuals,
    :station_count => (x -> maximum(skipmissing(x))) => :station_count
]
yearly_df = combine(grouped_data, aggregations...)
println("Aggregated to $(nrow(yearly_df)) county-year observations.")

# ===================================================================
# PART 3: ROLLING WINDOW INTERACTION ANALYSIS (REVISED)
# ===================================================================

println("\n--- Part 3: Setting up the Rolling Window Analysis ---")

# Define sectors
lhs_sectors = ["wage_construction", "wage_manufacturing", "wage_retail", "wage_adminwaste", "wage_healthsocial", "wage_entertainment"]
rhs_sector = "wage_highskill"

# DataFrame to store all results
results_over_time = DataFrame(
    end_year = Int[],
    model_spec = String[],
    dependent_variable = String[],
    interaction_coefficient = Float64[],
    std_error = Float64[],
    p_value = Float64[],
    n_observations = Int[]
)

# --- Nested Loop for Rolling Analysis ---
println("\n--- Starting Rolling Interaction Regression Loop ---")

# Outer loop: Expands the time window
for end_year in 2016:2022
    println("\n--- Analyzing data from 2012 to $(end_year) ---")

    # Filter the master data for the current time window
    model_df = filter(row -> row.year <= end_year, yearly_df)

    # --- Prepare all variables for this specific time window ---
    
    # Create the per-capita variable
    model_df.stations_per_100k_capita = (model_df.station_count ./ model_df.population) .* 100000

    # Create lagged version of the per-capita variable
    sort!(model_df, [:county_fips, :year])
    transform!(groupby(model_df, :county_fips), 
        :stations_per_100k_capita => (x -> lag(x, 1)) => :stations_per_100k_capita_lag1
    )
    
    # Filter for valid observations
    key_vars = vcat(lhs_sectors, rhs_sector, "stations_per_100k_capita", "stations_per_100k_capita_lag1")
    for col in key_vars; subset!(model_df, Symbol(col) => ByRow(x -> !ismissing(x) && x >= 0)); end
    subset!(model_df, :population => ByRow(pop -> !ismissing(pop) && pop > 0))
    subset!(model_df, :migration_individuals => ByRow(mig -> !ismissing(mig) && mig >= 0))


    # Define the specifications for the inner loops (ONLY Level models)
    timings =      [(name="Low-High Elasticity",      station_var="stations_per_100k_capita")]

    # Innermost loops for sectors and models
    for sector in lhs_sectors
        for timing in timings
            model_name = "$(timing.name)"
            
            lhs_var = Symbol(sector)
            rhs_var = Symbol(rhs_sector)
            station_var_full = Symbol(timing.station_var)
            
            vars_needed = [lhs_var, rhs_var, station_var_full, :population, :migration_individuals, :county_fips, :year]
            temp_df = dropmissing(model_df, vars_needed)

            if nrow(temp_df) > 50
                model = reg(temp_df, @eval(@formula($(lhs_var) ~ $(rhs_var) * $(station_var_full) + population + migration_individuals + fe(county_fips) + fe(year))))
                
                ct = coeftable(model)
                interaction_term_name = string(rhs_var) * " & " * string(station_var_full)
                coef_row = findfirst(x -> x == interaction_term_name, ct.rownms)

                if !isnothing(coef_row)
                    coef = ct.cols[1][coef_row]; se = ct.cols[2][coef_row]; pval = ct.cols[4][coef_row]; n_obs = nobs(model)
                    push!(results_over_time, (end_year, model_name, sector, coef, se, pval, n_obs))
                end
            end
        end
    end
end

# ===================================================================
# PART 4: SAVE THE RESULTS
# ===================================================================

println("\n--- Part 4: Saving Time Series Results ---")
println("EVOLUTION OF INTERACTION COEFFICIENTS OVER TIME (Sample):")
println(first(results_over_time, 12))

results_output_path = joinpath(tables_path, "rolling_interaction_results_level_models.csv")
CSV.write(results_output_path,results_over_time)
println("\n✅ Rolling analysis results saved to: $(results_output_path)")

# ===================================================================
# PART 5: PLOT THE RESULTS
# ===================================================================

println("\n--- Part 5: Generating Plots ---")
results_to_plot = CSV.read(results_output_path, DataFrame)
results_to_plot.ci_lower = results_to_plot.interaction_coefficient .- 1.96 .* results_to_plot.std_error
results_to_plot.ci_upper = results_to_plot.interaction_coefficient .+ 1.96 .* results_to_plot.std_error

title_map = Dict("wage_construction"=>"Construction", "wage_manufacturing"=>"Manufacturing", "wage_retail"=>"Retail", "wage_adminwaste"=>"Waste Mgmt", "wage_healthsocial"=>"Healthcare", "wage_entertainment"=>"Entertainment")

# Create a separate plot for Lagged vs. Unlagged models
for (spec_name, spec_group) in pairs(groupby(results_to_plot, :model_spec))
    all_sector_plots = []
    for sector in lhs_sectors
        sector_df = filter(row -> row.dependent_variable == sector, spec_group)
        if !isempty(sector_df)
            p = plot(sector_df.end_year, sector_df.interaction_coefficient, label="", title=get(title_map, sector, sector), xlabel="", ylabel="", ribbon=(sector_df.interaction_coefficient .- sector_df.ci_lower, sector_df.ci_upper .- sector_df.interaction_coefficient), fillalpha=0.2)
            hline!(p, [0], linestyle=:dash, color=:black, label="")
            push!(all_sector_plots, p)
        end
    end
    
    if !isempty(all_sector_plots)
        final_plot = plot(all_sector_plots..., layout=(2, 3), size=(1200, 800), plot_title="Interaction Effect Evolution: $(spec_name.model_spec) Model")
        output_plot_path = joinpath(plots_path, "rolling_effects_level_$(spec_name.model_spec).png")
        savefig(final_plot, output_plot_path)
        println("✅ Panel plot for spec '$(spec_name.model_spec)' saved to: $(output_plot_path)")
    end
end
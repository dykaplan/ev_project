using DataFrames
using CSV
using Statistics
using StatsBase
using StatsPlots
using Printf
using CategoricalArrays

# ===================================================================
# PART 1: SETUP AND LOAD DATA
# ===================================================================

println("--- Part 1: Setup ---")
# --- Configuration ---
dir_path = "C:/Users/Pinguin/Documents/BC/2025_2026/fall/labor_1/research_project_ev/"
data_in_path = joinpath(dir_path, "data/data_clean")
tables_path = joinpath(dir_path, "tables_figures/tables")
plots_path = joinpath(dir_path, "tables_figures/figures")

# Load the quarterly panel data
panel_path = joinpath(dir_path, "data/data_final", "all_data_panel_analysis.csv")
analysis_df = CSV.read(panel_path, DataFrame)

println("Successfully loaded quarterly panel data with $(nrow(analysis_df)) observations.")

# ===================================================================
# PART 2: DATA PREPARATION AND TIER ASSIGNMENT
# ===================================================================
println("\n--- Part 2: Preparing Data and Assigning Tiers ---")

# --- Step 1: Filter for counties with population > 1000 ---
analysis_df_filtered = filter(row -> !ismissing(row.population) && row.population > 1000, analysis_df)

# --- Step 2: Assign Tiers Based on 2022 Station Counts ---
df_2022 = filter(row -> row.year == 2022, analysis_df_filtered)

function assign_2022_tier(stations_per_capita)
    if ismissing(stations_per_capita) || stations_per_capita < 1
        return "Low (<1)"
    elseif stations_per_capita >= 1 && stations_per_capita < 10
        return "Medium (1-10)"
    else
        return "High (>=10)"
    end
end

df_2022.station_tier = assign_2022_tier.(df_2022.stations_per_100k_capita)
tier_lookup = select(df_2022, :county_fips, :station_tier)
unique!(tier_lookup, :county_fips)

df_with_tiers = leftjoin(analysis_df_filtered, tier_lookup, on = :county_fips)
dropmissing!(df_with_tiers, :station_tier)

# --- Set custom order for tiers ---
tier_levels = ["High (>=10)", "Medium (1-10)", "Low (<1)"]
df_with_tiers.station_tier = categorical(df_with_tiers.station_tier, levels=tier_levels, ordered=true)
println("Successfully assigned tiers.")


# --- Step 3: Create a Clean and Type-Stable DataFrame ---
required_cols = [
    :population, :pct_race_White, :pct_race_Black,
    :pct_race_Asian_Pacific_Islander, :pct_Age_5_14, :pct_Age_30_59,
    :pct_Age_60_79, :pct_sex_Female, :dem_margin, :migration_individuals,
    :migration_total_agi
]
df_clean = dropmissing(df_with_tiers, required_cols)
df_clean.population = Int.(df_clean.population)

println("\n--- Data Cleaning Complete ---")

# --- Step 4: Calculate POPULATION-WEIGHTED medians ---
yearly_comparison_table = combine(groupby(df_clean, [:year, :station_tier]),
    :population => median => :median_population,
    [:pct_race_White, :population] => ((x, w) -> median(x, weights(w))) => :median_pct_white,
    [:pct_race_Black, :population] => ((x, w) -> median(x, weights(w))) => :median_pct_black,
    [:pct_race_Asian_Pacific_Islander, :population] => ((x, w) -> median(x, weights(w))) => :median_pct_asian_pi,
    [:pct_Age_5_14, :population] => ((x, w) -> median(x, weights(w))) => :median_pct_age_5_14,
    [:pct_Age_30_59, :population] => ((x, w) -> median(x, weights(w))) => :median_pct_age_30_59,
    [:pct_Age_60_79, :population] => ((x, w) -> median(x, weights(w))) => :median_pct_age_60_79,
    [:pct_sex_Female, :population] => ((x, w) -> median(x, weights(w))) => :median_pct_female,
    [:dem_margin, :population] => ((x, w) -> median(x, weights(w))) => :median_dem_margin,
    [:migration_individuals, :population] => ((x, w) -> median(x, weights(w))) => :median_migration_individuals,
    [:migration_total_agi, :population] => ((x, w) -> median(x, weights(w))) => :median_migration_agi,
    nrow => :n_county_years
)
println("\n--- Step 4 successful: Population-weighted medians calculated. ---")

# Clean data for plotting
plot_df_cleaned = deepcopy(yearly_comparison_table)
plot_df_cleaned.median_migration_individuals = replace(plot_df_cleaned.median_migration_individuals, 0 => 1)
plot_df_cleaned.median_migration_agi = replace(plot_df_cleaned.median_migration_agi, 0 => 1)

gr()

# ===================================================================
# PART 3: GENERATE AND SAVE PLOTS
# ===================================================================
println("\n--- Part 3: Generating plot panels ---")

# --- Define formatters ---
function number_formatter(y)
    if y >= 1_000_000; return @sprintf("%.0fM", y/1_000_000);
    elseif y >= 1_000; return @sprintf("%.0fk", y/1_000);
    else; return @sprintf("%.0f", y); end
end

# --- Function to create a single subplot ---
function create_subplot(df, var, title, xlabel_text)
    is_log_scale = var in [:median_population, :median_migration_agi, :median_migration_individuals]
    is_percent_scale = occursin("pct", string(var)) || var == :median_dem_margin
    title_text = title * (is_log_scale ? " (Log Scale)" : "")

    y_formatter_func = :auto
    if is_log_scale; y_formatter_func = number_formatter;
    elseif is_percent_scale; y_formatter_func = y -> @sprintf("%.1f%%", y * 100); end

    p = plot(
        df.year,
        df[!, var],
        group = df.station_tier,
        legend = false,
        title = title_text,
        yaxis = is_log_scale ? :log10 : :linear,
        yformatter = y_formatter_func,
        xticks = 2012:4:2024,
        linewidth = 2,
        titlefontsize = 10,
        tickfontsize = 8,
        xlabel = xlabel_text,
        ylabel = "",
        margin = 5Plots.mm
    )
    
    return p
end

# ===================================================================
# PART 4: BUILD AND SAVE PLOTS WITHOUT LEGENDS
# ===================================================================

# --- Build and Save Panel 1 (Demographics) ---
println("Building Panel 1...")
let
    # --- FIX: Use the correct column names with the "median_" prefix ---
    vars_panel_1 = [
        :median_population,
        :median_pct_white,
        :median_pct_black,
        :median_pct_asian_pi,
        :median_pct_female,
        :median_dem_margin
    ]
    titles_panel_1 = [ "Population", "Pct White", "Pct Black", "Pct Asian PI", "Pct Female", "Dem Margin" ]
    
    plot_collection_1 = [
        let
            xlabel = (i > 3) ? "Year" : ""
            create_subplot(plot_df_cleaned, var, titles_panel_1[i], xlabel)
        end
        for (i, var) in enumerate(vars_panel_1)
    ]

    final_panel_1 = plot(plot_collection_1...,
        layout = (2, 3),
        size = (1400, 900),
        plot_title = "County Characteristics by EV Charger Station Tier (Panel 1: Demographics)",
        left_margin = 10Plots.mm,
        bottom_margin = 10Plots.mm,
        top_margin = 10Plots.mm
    )
    panel1_path = joinpath(plots_path, "demographics", "demographics_panel_1_core.png")
    savefig(final_panel_1, panel1_path)
    println("Panel 1 saved to: $(panel1_path)")
end


# --- Build and Save Panel 2 (Age & Migration) ---
println("Building Panel 2...")
let
    vars_panel_2 = [
        :median_pct_age_5_14, :median_pct_age_30_59, :median_pct_age_60_79,
        :median_migration_individuals, :median_migration_agi
    ]
    titles_panel_2 = [ "Pct Age 5-14", "Pct Age 30-59", "Pct Age 60-79", "Migrating Individuals", "Migration AGI" ]
    
    plot_collection_2 = [
        let
            xlabel = (i > 2) ? "Year" : ""
            create_subplot(plot_df_cleaned, var, titles_panel_2[i], xlabel)
        end
        for (i, var) in enumerate(vars_panel_2)
    ]

    final_panel_2 = plot(plot_collection_2...,
        layout = (2, 3),
        size = (1400, 900),
        plot_title = "County Characteristics by EV Charger Station Tier (Panel 2: Age & Migration)",
        left_margin = 10Plots.mm,
        bottom_margin = 10Plots.mm,
        top_margin = 10Plots.mm
    )
    panel2_path = joinpath(plots_path, "demographics", "demographics_panel_2_age_migration.png")
    savefig(final_panel_2, panel2_path)
    println("Panel 2 saved to: $(panel2_path)")
end

println("\n--- Success! ---")
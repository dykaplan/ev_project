
using DataFrames, GLM, Distributions
main_dir = "C:/Users/Pinguin/Documents/BC/2025_2026/fall/labor_1/research_project_ev/data/data_final"

data = joinpath(main_dir, "all_data_panel_analysis.csv")

clean_data = dropmissing(data, [
    :stations_per_100k_capita,
    # Key Covariates
    :population,
    :gdp,
    :migration_individuals,
    :migration_total_agi,
    :pct_sex_Male,
    # All Race Variables
    :pct_race_White,
    :pct_race_Black,
    :pct_race_American_Indian_Alaska_Native,
    :pct_race_Asian_Pacific_Islander,
    # All Age Variables
    :pct_Age_0_4,
    :pct_Age_5_14,
    :pct_Age_15_29,
    :pct_Age_30_59,
    :pct_Age_60_79,
    :pct_Age_80_Plus
])


formula = @formula(stations_per_100k_capita ~ population +
                                              # Race variables (White is the reference)
                                              pct_race_Black +
                                              pct_race_American_Indian_Alaska_Native +
                                              pct_race_Asian_Pacific_Islander +
                                              # Age variables (Age 30-59 is the reference)
                                              pct_Age_0_4 +
                                              pct_Age_5_14 +
                                              pct_Age_15_29 +
                                              pct_Age_60_79 +
                                              pct_Age_80_Plus +
                                              # Other covariates
                                              pct_sex_Male +
                                              gdp +
                                              migration_individuals +
                                              migration_total_agi)
                                              
treatment_model = glm(formula, clean_data, Normal())
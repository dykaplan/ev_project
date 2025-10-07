using DataFrames, Distributions, Plots, StatsPlots, StatsBase, GLM, HypothesisTests, MultivariateStats, ProgressMeter, CSV, ShiftedArrays

# get data and clean a bit

println("--- 1. Preparing Data ---")
main_dir = "C:/Users/Pinguin/Documents/BC/2025_2026/fall/labor_1/research_project_ev/data/data_final"
out_path = "C:\\Users\\Pinguin\\Documents\\BC\\2025_2026\\fall\\labor_1\\research_project_ev\\tables_figures\\figures"
data = CSV.read(joinpath(main_dir, "all_data_panel_analysis.csv"), DataFrame)

panel_2022 = filter(row -> row.year == 2022 && !ismissing(row.stations_per_100k_capita), data)
fips_to_keep = Set(filter(:stations_per_100k_capita => >(0), panel_2022).county_fips)
data_filtered = filter(:county_fips => fips -> fips in fips_to_keep, data)

panel_2022_clean_for_quantile = dropmissing(data_filtered, :stations_per_100k_capita)
top_quartile_threshold = quantile(panel_2022_clean_for_quantile.stations_per_100k_capita, 0.75)
treated_fips = Set(filter(:stations_per_100k_capita => >=(top_quartile_threshold), panel_2022_clean_for_quantile).county_fips)
data.treatment = ifelse.(in.(data.county_fips, (treated_fips,)), 1, 0)

sort!(data, [:county_fips, :year])
gdf = groupby(data, :county_fips)

vars_to_lag_3 = [:population, :gdp, :migration_individuals, :migration_total_agi]
for var in vars_to_lag_3
    for l in 1:3
        transform!(gdf, var => (x -> lag(x, l)) => Symbol("$(var)_lag$(l)"))
    end
end
vars_to_lag_1 = [:pct_race_Black, :pct_race_Asian_Pacific_Islander, :pct_race_Hispanic, :pct_Age_5_14, :pct_Age_15_29, :pct_Age_30_59, :pct_Age_60_79]
for var in vars_to_lag_1
    transform!(gdf, var => lag => Symbol("$(var)_lag1"))
end

model_df = filter(:year => ==(2022), data)
lagged_vars_3 = [Symbol("$(var)_lag$(l)") for var in vars_to_lag_3 for l in 1:3]
lagged_vars_1 = [Symbol("$(var)_lag1") for var in vars_to_lag_1]
vars_to_impute = filter(x -> occursin("migration", string(x)), lagged_vars_3)
vars_to_drop = vcat(filter(x -> !(x in vars_to_impute), lagged_vars_3), lagged_vars_1)
dropmissing!(model_df, vars_to_drop)

for col in vars_to_impute
    median_val = median(skipmissing(model_df[!, col]))
    model_df[!, col] = coalesce.(model_df[!, col], median_val)
end

# PSM model

println("\n--- 2. Fitting Propensity Score Model ---")
formula = @formula(treatment ~ population_lag1 + population_lag2 + gdp_lag1 + migration_individuals_lag1 + migration_total_agi_lag1 + pop_race_Black + pop_race_Hispanic)
predictors = [:population_lag1, :population_lag2, :gdp_lag1, :migration_individuals_lag1, :migration_total_agi_lag1, :pop_race_Black, :pop_race_Hispanic]

dropmissing!(model_df, predictors)
for col in predictors
    model_df[!, col] = Float64.(model_df[!, col])
end
for v in predictors
    x = Float64.(model_df[!, v])
    μ, σ = mean(x), std(x)
    model_df[!, v] = σ > 0 ? (x .- μ) ./ σ : x
end

logit_model = glm(formula, model_df, Binomial(), LogitLink(), dropcollinear=true)
model_df.propensity_score = predict(logit_model)
model_df.ipw = ifelse.(model_df.treatment .== 1, 1 ./ model_df.propensity_score, 1 ./ (1 .- model_df.propensity_score))
println(first(model_df[!, [:county_fips, :treatment, :propensity_score, :ipw]], 6))

# balance test

println("\n--- 3. Performing Matching and Balance Checks ---")
treated = filter(:treatment => ==(1), model_df)
controls = filter(:treatment => ==(0), model_df)
match_indices = [argmin(abs.(controls.propensity_score .- t_ps)) for t_ps in treated.propensity_score]
matched_controls = controls[match_indices, :]
matched_df = vcat(treated, matched_controls)
println("Number of matched pairs: ", nrow(treated))

balance_table = DataFrame(Covariate=String[], MeanTreated=Float64[], MeanControl_Matched=Float64[], StdMeanDiff=Float64[], P_Value=Float64[])
for cov in predictors
    treated_values = filter(:treatment => ==(1), matched_df)[!, cov]
    control_values = filter(:treatment => ==(0), matched_df)[!, cov]
    mean_t = mean(treated_values)
    mean_c = mean(control_values)
    std_original_treated = std(filter(:treatment => ==(1), model_df)[!, cov])
    smd = (mean_t - mean_c) / std_original_treated
    p_val = pvalue(EqualVarianceTTest(treated_values, control_values))
    push!(balance_table, (string(cov), mean_t, mean_c, smd, p_val))
end
println(balance_table)

#F-test seriously fails here
treated_data = Matrix(filter(:treatment => ==(1), matched_df)[!, predictors])
control_data = Matrix(filter(:treatment => ==(0), matched_df)[!, predictors])
test = EqualCovHotellingT2Test(treated_data, control_data)

# diag plot 
println("\n--- 4. Generating Diagnostic Plots ---")
density(model_df.propensity_score, group = model_df.treatment, title = "Overlap of Propensity Scores", xlabel = "Propensity Score", ylabel = "Density")

savefig(joinpath(out_path,"ipw_overlap_plot.png"))

# treatment effect

println("\n--- 5. Estimating Treatment Effects ---")
low_wage_var = :wage_healthsocial
high_wage_var = :wage_highskill

# Matching ATT
dropmissing!(matched_df, [low_wage_var, high_wage_var])
filter!(row -> row[low_wage_var] > 0 && row[high_wage_var] > 0, matched_df)
matched_df.log_wage_ratio = log.(matched_df[!, low_wage_var] ./ matched_df[!, high_wage_var])
att_point_estimate = mean(filter(:treatment => ==(1), matched_df).log_wage_ratio) - mean(filter(:treatment => ==(0), matched_df).log_wage_ratio)

n_bootstraps = 100
att_estimates = Vector{Float64}(undef, n_bootstraps)
@showprogress "Bootstrapping Matching ATT:" for i in 1:n_bootstraps
    sample_df = model_df[rand(1:nrow(model_df), nrow(model_df)), :]
    ps_model_boot = glm(formula, sample_df, Binomial(), LogitLink())
    sample_df.propensity_score = predict(ps_model_boot)
    treated_boot = filter(:treatment => ==(1), sample_df)
    controls_boot = filter(:treatment => ==(0), sample_df)
    if nrow(treated_boot) == 0 || nrow(controls_boot) == 0; att_estimates[i] = NaN; continue; end
    match_indices_boot = [argmin(abs.(controls_boot.propensity_score .- t_ps)) for t_ps in treated_boot.propensity_score]
    matched_controls_boot = controls_boot[match_indices_boot, :]
    matched_df_boot = vcat(treated_boot, matched_controls_boot)
    dropmissing!(matched_df_boot, [low_wage_var, high_wage_var])
    filter!(row -> row[low_wage_var] > 0 && row[high_wage_var] > 0, matched_df_boot)
    if nrow(filter(:treatment => ==(1), matched_df_boot)) == 0 || nrow(filter(:treatment => ==(0), matched_df_boot)) == 0; att_estimates[i] = NaN; continue; end
    matched_df_boot.log_wage_ratio = log.(matched_df_boot[!, low_wage_var] ./ matched_df_boot[!, high_wage_var])
    att_estimates[i] = mean(filter(:treatment => ==(1), matched_df_boot).log_wage_ratio) - mean(filter(:treatment => ==(0), matched_df_boot).log_wage_ratio)
end
bootstrapped_se = std(filter(!isnan, att_estimates))
println("\n--- Final Matching Results ---")
println("ATT Point Estimate (on log scale): ", att_point_estimate)
println("Bootstrapped Standard Error:       ", bootstrapped_se)

# IPW ATE
df_point_estimate = copy(model_df)
dropmissing!(df_point_estimate, vcat(Symbol.(coefnames(logit_model)[2:end]), [low_wage_var, high_wage_var]))
filter!(row -> row[low_wage_var] > 0 && row[high_wage_var] > 0, df_point_estimate)
df_point_estimate.log_wage_ratio = log.(df_point_estimate[!, low_wage_var] ./ df_point_estimate[!, high_wage_var])
ipw_ate_model = lm(@formula(log_wage_ratio ~ treatment), df_point_estimate, wts = df_point_estimate.ipw)
ate_point_estimate = StatsBase.coef(ipw_ate_model)[2]

n_bootstraps_ipw = 100
ate_estimates_ipw = Vector{Float64}(undef, n_bootstraps_ipw)
@showprogress "Bootstrapping IPW ATE:" for i in 1:n_bootstraps_ipw
    sample_df = model_df[rand(1:nrow(model_df), nrow(model_df)), :]
    dropmissing!(sample_df, vcat(Symbol.(coefnames(logit_model)[2:end]), [low_wage_var, high_wage_var]))
    filter!(row -> row[low_wage_var] > 0 && row[high_wage_var] > 0, sample_df)
    if nrow(sample_df) < 20 || length(unique(sample_df.treatment)) < 2; ate_estimates_ipw[i] = NaN; continue; end
    ps_model_boot = glm(formula, sample_df, Binomial(), LogitLink())
    sample_df.propensity_score = predict(ps_model_boot)
    sample_df.ipw = ifelse.(sample_df.treatment .== 1, 1 ./ sample_df.propensity_score, 1 ./ (1 .- sample_df.propensity_score))
    sample_df.log_wage_ratio = log.(sample_df[!, low_wage_var] ./ sample_df[!, high_wage_var])
    ipw_model_boot = lm(@formula(log_wage_ratio ~ treatment), sample_df, wts=sample_df.ipw)
    if "treatment" in coefnames(ipw_model_boot); ate_estimates_ipw[i] = StatsBase.coef(ipw_model_boot)[2]; else; ate_estimates_ipw[i] = NaN; end
end
bootstrapped_se_ipw = std(filter(!isnan, ate_estimates_ipw))
println("\n--- Final IPW Results ---")
println("ATE Point Estimate:          ", ate_point_estimate)
println("Bootstrapped Standard Error: ", bootstrapped_se_ipw)




#now doubly robust:

dropmissing!(model_df, [low_wage_var, high_wage_var])
filter!(row -> row[low_wage_var] > 0 && row[high_wage_var] > 0, model_df)
model_df.log_wage_ratio = log.(model_df[!, low_wage_var] ./ model_df[!, high_wage_var])


# estimate the two outcome models
stable_predictors = [  :population_lag1, :population_lag2, :gdp_lag1,
    :migration_individuals_lag1, 
    :migration_total_agi_lag1,:pop_race_Black,:pop_race_Hispanic
]
dropmissing!(model_df, [low_wage_var, high_wage_var])
filter!(row -> row[low_wage_var] > 0 && row[high_wage_var] > 0, model_df)
model_df.log_wage_ratio = log.(model_df[!, low_wage_var] ./ model_df[!, high_wage_var])


rhs_terms = reduce(+, Term.(stable_predictors))
outcome_formula = FormulaTerm(Term(:log_wage_ratio), rhs_terms)

# outcome model for the control group 
m0_model = lm(outcome_formula, filter(:treatment => ==(0), model_df))

# outcome model for the treated group 
m1_model = lm(outcome_formula, filter(:treatment => ==(1), model_df))

# get predictions for ALL observations from both models
model_df.mu0_hat = predict(m0_model, model_df)
model_df.mu1_hat = predict(m1_model, model_df)


#  Doubly Robust Formula
clamped_ps = clamp.(model_df.propensity_score, 0.01, 0.99)

# first part of the formula (using clamped scores)
part1 = (model_df.treatment .* (model_df.log_wage_ratio .- model_df.mu1_hat)) ./ clamped_ps

# second part of the formula (using clamped scores)
part2 = ((1 .- model_df.treatment) .* (model_df.log_wage_ratio .- model_df.mu0_hat)) ./ (1 .- clamped_ps)

# third part of the formula
part3 = model_df.mu1_hat .- model_df.mu0_hat

# ATE is the mean of the sum of these parts
ate_dr = mean(part1 .- part2 .+ part3)

println("\n--- Doubly Robust Estimate ---")
println("Doubly Robust ATE: ", ate_dr)


println("\n--- Calculating Bootstrapped Standard Error for Doubly Robust ATE ---")

n_bootstraps = 100

ate_dr_estimates = Vector{Float64}(undef, n_bootstraps)

@showprogress for i in 1:n_bootstraps
    #  sample from the ORIGINAL model_df each time
    sample_df = model_df[rand(1:nrow(model_df), nrow(model_df)), :]

    # re-run the Doubly Robust analysis on the sample
    dropmissing!(sample_df, vcat(stable_predictors, [low_wage_var, high_wage_var]))
    filter!(row -> row[low_wage_var] > 0 && row[high_wage_var] > 0, sample_df)
    if nrow(sample_df) < 20 || length(unique(sample_df.treatment)) < 2
        ate_dr_estimates[i] = NaN
        continue
    end
    sample_df.log_wage_ratio = log.(sample_df[!, low_wage_var] ./ sample_df[!, high_wage_var])

    #fit all three models on the bootstrap sample
    ps_model_boot = glm(formula, sample_df, Binomial(), LogitLink())
    m0_model_boot = lm(outcome_formula, filter(:treatment => ==(0), sample_df))
    m1_model_boot = lm(outcome_formula, filter(:treatment => ==(1), sample_df))

    # get predictions
    sample_df.ps = predict(ps_model_boot)
    
    # predict calls for the LM models are more stable and can stay as they are
    sample_df.mu0_hat = predict(m0_model_boot, sample_df)
    sample_df.mu1_hat = predict(m1_model_boot, sample_df)
    
    clamped_ps_boot = clamp.(sample_df.ps, 0.01, 0.99)

    # d. apply the DR formula to get the ATE for this sample
    part1 = (sample_df.treatment .* (sample_df.log_wage_ratio .- sample_df.mu1_hat)) ./ clamped_ps_boot
    part2 = ((1 .- sample_df.treatment) .* (sample_df.log_wage_ratio .- sample_df.mu0_hat)) ./ (1 .- clamped_ps_boot)
    part3 = sample_df.mu1_hat .- sample_df.mu0_hat
    
    ate_dr_estimates[i] = mean(part1 .- part2 .+ part3)
end

# final results 
bootstrapped_se_dr = std(filter(!isnan, ate_dr_estimates))
println("\n--- Final Doubly Robust Results ---")
println("ATE Point Estimate:          ", ate_dr)
println("Bootstrapped Standard Error: ", bootstrapped_se_dr)


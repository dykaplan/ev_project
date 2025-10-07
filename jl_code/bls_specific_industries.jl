# ===================================================================
# UTILITY SCRIPT: Process and AGGREGATE Yearly BLS Files 
# ===================================================================

using DataFrames
using CSV
using Statistics
using ProgressMeter

# --- CONFIGURATION ---
dir_path = "C:/Users/Pinguin/Documents/BC/2025_2026/fall/labor_1/research_project_ev/"
data_in_path = joinpath(dir_path, "data/data_raw")
data_intermediate_path = joinpath(dir_path, "data/data_intermediate")

main_bls_path = joinpath(data_in_path, "BLS Quarterly Wage Data")
output_path = joinpath(data_intermediate_path, "BLS_wages_by_skill_yearly/")
years_to_process = 2012:2024

# --- HELPER FUNCTION: Classify industry by NAICS code (UPDATED) ---
function classify_skill_group(industry_code)
    code_str = string(industry_code)
    if isempty(code_str)
        return "Other"
    end

    # --- Category Definitions ---
    # 1. Hand-picked 6-digit NAICS codes
    construction_specific = Set(["236118", "238160", "238170", "238320", "238330", "238910"])
    admin_waste_specific = Set(["561320", "561720", "561730", "562111"])
    health_social_specific = Set(["621610", "623110", "623312", "623990", "624120", "624410"])

    # 2. Broader categories defined by prefixes ("Net Fishing")
    manufacturing_prefixes = Set(["311", "313", "314", "315", "316", "321", "337"])
    entertainment_prefixes = Set(["7111", "7115", "7131", "7139", "71213", "71219"])
    high_wage_sectors = Set(["51", "52", "54", "55"]) #2-digit sectors #Information (51), Financial Activities (52), Professional and Business Services (54), and Management of Companies and Enterprises (55)

    # --- Classification Logic (from most specific to most broad) ---

    # A. Check against the hand-picked 6-digit lists first
    if code_str in construction_specific; return "Wage_Construction"; end
    if code_str in admin_waste_specific; return "Wage_AdminWaste"; end
    if code_str in health_social_specific; return "Wage_HealthSocial"; end

    # B. Check against other prefix-based categories
    # Using startswith is more robust for these education categories
    if startswith(code_str, "6111"); return "Wage_HealthSocial"; end # K-12
    if startswith(code_str, "6115"); return "Wage_TradeEducation"; end
    if startswith(code_str, "6116"); return "Wage_EducationOther"; end
    
    if any(s -> startswith(code_str, s), manufacturing_prefixes); return "Wage_Manufacturing"; end
    if any(s -> startswith(code_str, s), entertainment_prefixes); return "Wage_Entertainment"; end
    
    # C. Check against broad 2-digit sectors
    sector = ""
    if length(code_str) >= 2
        sector = startswith(code_str, "44") || startswith(code_str, "45") ? "44-45" : code_str[1:2]
    end

    if sector == "44-45"; return "Wage_Retail"; end
    if sector in high_wage_sectors; return "Wage_HighSkill"; end
    
    # D. If no match, classify as "Other"
    return "Other"
end

# --- SCRIPT LOGIC (Unchanged) ---
println("Starting yearly AGGREGATION of BLS files...")
mkpath(output_path)

for year in years_to_process
    println("\n--- Processing and Aggregating Year: $(year) ---")
    
    input_folder = joinpath(main_bls_path, "$(year).q1-q4.by_industry")
    if !isdir(input_folder); println("Warning: Directory not found for year $(year). Skipping."); continue; end
    
    all_csvs = filter(f -> endswith(f, ".csv"), readdir(input_folder))
    if isempty(all_csvs); println("Warning: No CSV files found for year $(year). Skipping."); continue; end

    skill_data_raw = Dict{String, DataFrame}()

    @showprogress "Reading files for $(year)... " for file_name in all_csvs
        parts = split(file_name)
        if length(parts) < 2; continue; end
        
        naics_code = parts[2]
        skill_group = classify_skill_group(naics_code)
        
        if skill_group != "Other"
           temp_df = CSV.read(joinpath(input_folder, file_name), DataFrame, 
                                silencewarnings=true, 
                                types=Dict(:industry_title => String, 
                                           :industry_code => String, 
                                           :disclosure_code => String,
                                           :oty_disclosure_code => String,
                                           :lq_disclosure_code => String))
            
            if !haskey(skill_data_raw, skill_group)
                skill_data_raw[skill_group] = DataFrame()
            end
            append!(skill_data_raw[skill_group], temp_df)
        end
    end

    function aggregate_skill_df(df::DataFrame)
    if isempty(df); return DataFrame(); end
    grouping_keys = [:area_fips, :year, :qtr, :area_title]
    valid_grouping_keys = intersect(grouping_keys, Symbol.(names(df)))
    if isempty(valid_grouping_keys); return DataFrame(); end
    
    agg_df = combine(groupby(df, valid_grouping_keys)) do sdf
        sum_wages = sum(skipmissing(sdf.total_qtrly_wages))
        
        # --- THIS IS THE CORRECTED LOGIC ---
        sum_empl1 = sum(skipmissing(sdf.month1_emplvl))
        sum_empl2 = sum(skipmissing(sdf.month2_emplvl))
        sum_empl3 = sum(skipmissing(sdf.month3_emplvl))
        avg_empl = (sum_empl1 + sum_empl2 + sum_empl3) / 3
        # --- END OF CORRECTION ---
        
        avg_wkly_wage = (avg_empl > 0) ? round((sum_wages / avg_empl) / 13, digits=2) : 0.0
        return (avg_wkly_wage = avg_wkly_wage,)
    end
    return agg_df
end

    skill_data_agg = Dict{String, DataFrame}()

    for (skill_group, raw_df) in skill_data_raw
        println("Processing $(skill_group)...")
        filter!(row -> row.agglvl_code == 78, raw_df)
        filter!(row -> startswith(row.agglvl_title, "County"), raw_df)
        filter!(row -> row.own_code != 0, raw_df)
        
        agg_df = aggregate_skill_df(raw_df)
        if !isempty(agg_df)
            rename!(agg_df, :avg_wkly_wage => Symbol(lowercase(skill_group)))
            skill_data_agg[skill_group] = agg_df
        end
    end

    if !isempty(skill_data_agg)
        join_keys = [:area_fips, :year, :qtr, :area_title]
        all_dfs = collect(values(skill_data_agg))
        
        yearly_merged_df = reduce((df1, df2) -> outerjoin(df1, df2, on = join_keys, makeunique=true), all_dfs)

        combined_output_file = joinpath(output_path, "$(year)_wages_by_skill_AGGREGATED.csv")
        CSV.write(combined_output_file, yearly_merged_df)
        println("\nSaved COMBINED skill data for $(year) to: $(combined_output_file)")
    else
        println("\nNo data found for $(year) to save after filtering.")
    end
end

println("\n✅ All BLS years processed.")

# --- PART 3: FINAL COMBINATION (Unchanged) ---
println("\n--- PART 3: Combining all yearly files into one master file... ---")
yearly_files_to_combine = [
    joinpath(output_path, "$(y)_wages_by_skill_AGGREGATED.csv")
    for y in years_to_process
    if isfile(joinpath(output_path, "$(y)_wages_by_skill_AGGREGATED.csv"))
]

if !isempty(yearly_files_to_combine)
    bls_all_years_df = vcat([CSV.read(file, DataFrame) for file in yearly_files_to_combine]...)
    final_output_file = joinpath(data_intermediate_path, "BLS_wages_all_years_industries_COMBINED.csv")
    CSV.write(final_output_file, bls_all_years_df)

    println("Successfully combined $(length(yearly_files_to_combine)) yearly files into one.")
    println("Final master file saved to: $(final_output_file)")
else
    println("Error: No yearly aggregated files were found to combine.")
end
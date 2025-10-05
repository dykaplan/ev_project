function clean_fips(fips_col)
    # This function robustly handles FIPS codes that might be numbers,
    # floats (like 25017.0), or strings, and formats them to a
    # 5-digit zero-padded string, while preserving missing values.
    
    cleaned_fips = Vector{Union{String, Missing}}(undef, length(fips_col))
    for (i, f) in enumerate(fips_col)
        if ismissing(f) || f == ""
            cleaned_fips[i] = missing
        else
            # Try to parse as a number to handle floats like 25017.0
            num_fips = tryparse(Float64, string(f))
            if !isnothing(num_fips)
                cleaned_fips[i] = lpad(string(Int(num_fips)), 5, '0')
            else
                # If it's not a number, just pad the string
                cleaned_fips[i] = lpad(string(f), 5, '0')
            end
        end
    end
    return cleaned_fips
end


function aggregate_and_widen(df, group_cols, pivot_col, prefix)
    pop_by_group = combine(groupby(df, group_cols), :Population => sum => :population)
    pop_wide = unstack(pop_by_group, [:county_fips, :Year], pivot_col, :population)
    cols_to_rename = setdiff(names(pop_wide), ["county_fips", "Year"])
    rename!(pop_wide, [col => prefix * col for col in cols_to_rename]...)
    return pop_wide
end


function categorize_age(age_code)
    if age_code <= 1; return "Age_0_4";
    elseif age_code <= 3; return "Age_5_14";
    elseif age_code <= 6; return "Age_15_29";
    elseif age_code <= 12; return "Age_30_59";
    elseif age_code <= 16; return "Age_60_79";
    else; return "Age_80_Plus"; end
end
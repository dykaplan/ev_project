
dir_path = "C:/Users/Pinguin/Documents/BC/2025_2026/fall/labor_1/research_project_ev/code/"

#Load Helper functions
include(joinpath(dir_path,"jl_code", "helper_functions.jl"))

#run data clean and Merge
include(joinpath(dir_path,"jl_code", "merge_final.jl"))

#make panel station maps
include(joinpath(dir_path,"jl_code", "panel_station_maps.jl"))

#run voting trends code
include(joinpath(dir_path,"jl_code", "voting_ev_trends.jl"))

#run station maps code
include(joinpath(dir_path,"jl_code", "station_maps.jl"))

#run main analysis code
include(joinpath(dir_path,"jl_code", "analysis.jl"))

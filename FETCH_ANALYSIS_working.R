## Fetch analysis based on local projections with a for loop 
## Local projections based on UTM zones
## ECU import improved by importing a buffered ecu layer by 2000km around seagrass sites in ARC
## Haven't checked that previous problem of incomplete dataset import is resolved 
## check final fetch outputs and write ecu layer back to arc if requried

library(sp)
#library(rgdal)
library(sf)
library(tidyverse)
library(dplyr)
library('waver')
library(rgeos)
#library(tidyverse)
library(arcgisbinding)
arc.check_product()
print('libraries imported')

## import seagrass sites and transform
sg_path <- "C:/Users/nw431/OneDrive - University of Exeter/1_PhD Research/Project1_Carbon Model/Seagrass Carbon Data/Notebooks/MASTER_carbon_dataset2_locations_validated.csv"

#load csv with lat long subset to only necessary columns
sg_locs <- read.csv(sg_path)
#sg_locs <- sg_locs[sg_locs$Source == 'Green 2018' | sg_locs$Source == 'Laing 2022' ,] #select only UK
sg_locs <- sg_locs[!is.na(sg_locs$Lat),]  #remove any without location information
#sg_df <- subset(sg_locs, select = c('Dataset_ID', 'Site_name', 'Lat', 'Long')) #convert to simple dataframe and no geometry column
keep = c('Dataset_ID', 'Site_name', 'Lat', 'Long')
sg_df <- sg_locs[keep]
print('seagrass dataframe')
#str(sg_df)  #to check import details

## if just running for a few sites - import csv with list of ids

#just run for these sites #make ids a vector
#path <- 'C:/Users/nw431/OneDrive - University of Exeter/1_PhD Research/Project1_Carbon Model/Seagrass env variables/1_Environmental_variables/R/Final_fetch_calc/Fetch_calc_rerun_Feb23.csv'
#data_ids <- read_csv(path)%>% pull('IDs')


#filter df by list of ids
#sg_df <- sg_df%>%filter(Dataset_ID %in% data_ids)
print('subsetted')

#open ECU dataset from ArcGis 
# path is to buffered and clipped seagrass-ECU dataset by 2000km - 'updated' version has had coastline modified so all sites in sea
#ecu_path <- "C:\\Users\\nw431\\OneDrive - University of Exeter\\1_PhD Research\\Project1_Carbon Model\\Seagrass env variables\\1_Environmental_variables\\data\\external\\ecu_final\\Ecological Coastal Units (ECU) 1km Segments.shp"
#ecu_path <- "C:\\Users\\nw431\\OneDrive - University of Exeter\\1_PhD Research\\Project1_Carbon Model\\Seagrass env variables\\1_Environmental_variables\\data\\external\\ecu_final\\ECU_buffered2000.shp"
ecu_path <- "C:\\Users\\nw431\\OneDrive - University of Exeter\\1_PhD Research\\Project1_Carbon Model\\Seagrass env variables\\1_Environmental_variables\\R\\Final_fetch_calc\\data\\ECU_buffered2000_updated.shp"
arc_df <- arc.open(ecu_path)
filtered_df <- arc.select(arc_df, fields =c('Shape', 'FID'))  #filter just for the basic shape
#shoreline_sp <- arc.data2sp(filtered_df) #problems inporting into sp
shoreline_sf <- arc.data2sf(filtered_df)  #import into sf dataframe

print('ecu shp imported')

#function to identify UTMZone from Long
long2UTM <- function(long) {
    (floor((long + 180)/6) %% 60) + 1
}

#function to create epsg from utm zone
utm2epsg <- function(zone) {
    32600 + zone
}

#add in columns to sg_df with utm and epsg based on location
sg_df_UTM <- sg_df
sg_df_UTM['zone'] <- long2UTM(sg_df_UTM['Long'])
sg_df_UTM['epsg'] <- utm2epsg(sg_df_UTM['zone'])
print('epsg column added')

#make spatial object (sf object) to have a geometry column
sg_sf <- st_as_sf(sg_df_UTM, coords = c('Long', 'Lat')) #Long = X, Lat = Y

#next two lines of code to remove those that have already been processed - remove to process whole dataset normally
#existing_epsg <- c(32631, 32618)
#sg_sf <- sg_sf[!(sg_sf$epsg %in% existing_epsg), ]  #removes those from the epsg list

# add it's crs
st_crs(sg_sf) <- 4326
print('seagrass loctions as sf dataframe with crs')

## START FOR LOOP TO LOOP THROUGH EPSGs
v <- unique(sg_sf$epsg)  #find the unique epsgs assigned in sg_df to loop through
bearings = seq(0,359,22.5)

#define final dataframe to append data to
fetch_ALL <- data.frame()

#iterate through epsg values (v) to transform and calculate fetch by batch

for (x in v){
  
  print('looping')
  #looking at the first epsg code
  sg_sf_sub <- sg_sf[sg_sf$epsg == x ,] #subset to one epsg code
  epsg <- paste("EPSG:", x, sep = "")
  
  #transform shoreline and sg to local EPSG
  sg_sf_proj <- st_transform(sg_sf_sub, crs = epsg) #transforms sg to local epsg
  shore_sf_proj <-st_transform(shoreline_sf, crs = epsg) #transform shoreline to local epsg
  print('seagrass and shoreline spatial dataframes in same crs')
  #if(st_is_valid(shoreline_sf_trans) == FALSE){
    #shoreline_sf_val <- st_make_valid(shoreline_sf_trans)
  #}
  print(shore_sf_proj)
  #turn back into sp objects#

  shore_sf_proj <- shore_sf_proj %>% filter(is.na(st_dimension(.)) == FALSE)
  #shore_sf_proj <- shore_sf_proj %>% filter(na.omit(st_is_valid(shore_sf_proj))== FALSE)
  shore_sf_proj <- shore_sf_proj %>% filter(!is.na(st_is_valid(.)))
  
  #convert to sp
  sg_sp_trans <- as(sg_sf_proj, "Spatial")
  shoreline_sp_trans <- as(shore_sf_proj, "Spatial")
  print("converted to sp objects")

  #run fetch
  fetch = fetch_len_multi(sg_sp_trans, bearings = bearings, shoreline_sp_trans, dmax = 100000, spread = 0, method = 'btree', projected = TRUE)

  #merge back with original dataset

  fetch_df <- fetch%>%
    as.data.frame()%>%
    tibble::rownames_to_column('ID')

  # prepare the seagrass locations dataframe
  rownames(sg_sf_sub) = seq(length=nrow(sg_sf_sub))
  sg_df_indexed <- sg_sf_sub%>%
    as.data.frame()%>%
    tibble::rownames_to_column('ID')

  fetch_final <- sg_df_indexed %>%
    left_join(fetch_df, by = 'ID')

  #export dataframe 

  output_path <- paste0(getwd(),"/outputs/outputs_loop/waver_fetch", x, ".csv", sep = "")
  #name <- paste("fetch_final_", x, sep="")
  write.csv(fetch_final, output_path, row.names = FALSE)

  #gather and save in arc compatible viz format
  bearing_ch <- as.character(seq(0,359,22.5))
  gathered_fetch <- fetch_final%>%
    gather(all_of(bearing_ch), key = 'Bearing', value = 'Fetch_Distance')
  output_path <- paste0(getwd(),"/outputs/outputs_loop/gathered_fetch", x, ".csv", sep = "")
  write_csv(gathered_fetch, output_path)
}

print('complete')

#arc_path_sh = 'C:/Users/nw431/OneDrive - University of Exeter/1_PhD Research/Project1_Carbon Model/Seagrass env variables/1_Environmental_variables/data/external/ecu/Default.gdb/shore_sp_proj_32610'
#arc.write(arc_path_sh, shoreline_sp_trans)
#arc_path_sg = 'C:/Users/nw431/OneDrive - University of Exeter/1_PhD Research/Project1_Carbon Model/Seagrass env variables/1_Environmental_variables/data/external/ecu/Default.gdb/sg_sp_proj_32610'
#arc.write(arc_path_sg, sg_sp_trans)

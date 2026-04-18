install.packages("StormR")

library(StormR)


sds <- defStormsDataset(
  filename = "C:/Users/warre/Documents/Programming/IBTrACS.SA.v04r01.nc",
  sep = NULL,
  fields = c(names = "name", seasons = "season", isoTime = "iso_time", lon = "usa_lon",
            lat = "usa_lat", msw = "usa_wind", basin = "basin", rmw = "usa_rmw", pressure =
              "usa_pres", poci = "usa_poci"),
  basin = NULL,
  seasons = c(1980, as.numeric(format(Sys.time(), "%Y"))),
  unitConversion = c(msw = "knt2ms", rmw = "nm2km", pressure = "mb2pa", poci = "mb2pa"),
  notNamed = "NOT_NAMED",
  verbose = 1
)

# Define the storm list for the year 2004
st <- defStormsList(sds = sds, loi = 'Brazil', season = 2004, verbose = 1)

# Run the model for the MSW product
catarina <- spatialBehaviour(st, verbose = 2, product = "MSW")

# Print the names of the analysis produced
names(catarina)

# Write the raster output to a file
file_path_2 <- "C:/Users/warre/Documents/Programming/Trial"
writeRast(catarina$UNNAMED_MSW, path = file_path_2, overwrite=TRUE)

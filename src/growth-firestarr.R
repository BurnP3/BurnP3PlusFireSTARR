# Clean global environment variables
native_proj_lib <- Sys.getenv("PROJ_LIB")
Sys.unsetenv("PROJ_LIB")
options(scipen = 999)

# Check and load packages ----
library(rsyncrosim)
suppressPackageStartupMessages(library(tidyverse))
suppressPackageStartupMessages(library(lubridate))
suppressPackageStartupMessages(library(terra))
suppressPackageStartupMessages(library(data.table))
suppressPackageStartupMessages(library(sf))

checkPackageVersion <- function(packageString, minimumVersion){
  result <- compareVersion(as.character(packageVersion(packageString)), minimumVersion)
  if (result < 0) {
    stop("The R package ", packageString, " (", as.character(packageVersion(packageString)), ") does not meet the minimum requirements (", minimumVersion, ") for this version of BurnP3+ FireSTARR. Please upgrade this package and rerun this scenario.", type = "warning")
  } else if (result > 0) {
    updateRunLog("Using a newer version of ", packageString, " (", as.character(packageVersion(packageString)), ") than BurnP3+ FireSTARR was built against (", minimumVersion, ").", type = "info")
  }
}

checkPackageVersion("rsyncrosim", "2.1.0")
checkPackageVersion("tidyverse",  "2.0.0")
checkPackageVersion("terra",      "1.5.21")
checkPackageVersion("dplyr",      "1.1.2")
checkPackageVersion("codetools",  "0.2.19")
checkPackageVersion("data.table", "1.14.8")
checkPackageVersion("sf",         "1.0.7")

# Setup ----
progressBar(type = "message", message = "Preparing inputs...")

# Initialize first breakpoint for timing code
currentBreakPoint <- proc.time()

# Find SyncroSim proj location
ssim_proj_lib <- ssimEnvironment()$ProgramDirectory %>%
  file.path("gdal", "share") %>%
  normalizePath()

## Connect to SyncroSim ----

myScenario <- scenario()

# Load Run Controls and identify iterations to run
RunControl <- datasheet(myScenario, "burnP3Plus_RunControl", returnInvisible = T)
iterations <- seq(RunControl$MinimumIteration, RunControl$MaximumIteration)

# Load remaining datasheets
BatchOption <- datasheet(myScenario, "burnP3Plus_BatchOption")
ResampleOption <- datasheet(myScenario, "burnP3Plus_FireResampleOption")
DeterministicIgnitionLocation <- datasheet(myScenario, "burnP3Plus_DeterministicIgnitionLocation", lookupsAsFactors = F, optional = T, returnInvisible = T) %>% unique
DeterministicBurnCondition <- datasheet(myScenario, "burnP3Plus_DeterministicBurnCondition", lookupsAsFactors = F, optional = T, returnInvisible = T) %>% unique
FuelType <- datasheet(myScenario, "burnP3Plus_FuelType", lookupsAsFactors = F)
FuelTypeCrosswalk <- datasheet(myScenario, "burnP3PlusFireSTARR_FuelCodeCrosswalk", lookupsAsFactors = F, optional = T)
ValidFuelCodes <- datasheet(myScenario, "burnP3PlusFireSTARR_FuelCode") %>% pull()
SeasonTable <- datasheet(myScenario, "burnP3Plus_Season", lookupsAsFactors = F, optional = T, includeKey = T, returnInvisible = T)
FBPVariableTable <- datasheet(myScenario, "burnP3Plus_FBPOutputVariable", lookupsAsFactors = F, optional = T, returnInvisible = T)
WindGrid <- datasheet(myScenario, "burnP3Plus_WindGrid", lookupsAsFactors = F, optional = T)
GreenUp <- datasheet(myScenario, "burnP3Plus_GreenUp", lookupsAsFactors = F, optional = T)
Curing <- datasheet(myScenario, "burnP3Plus_Curing", lookupsAsFactors = F, optional = T)
# FuelLoad <- datasheet(myScenario, "burnP3Plus_FuelLoad", lookupsAsFactors = F, optional = T)
OutputOptions <- datasheet(myScenario, "burnP3Plus_OutputOption", optional = T)
OutputOptionsSpatial <- datasheet(myScenario, "burnP3Plus_OutputOptionSpatial", optional = T) %>% mutate(BurnPerimeter = as.character(BurnPerimeter))
OutputOptionFBPSpatial <- datasheet(myScenario, "burnP3Plus_OutputOptionFBPSpatial", optional = T, returnInvisible = T) %>% mutate(Variable = as.character(Variable))
FireZoneTable <- datasheet(myScenario, "burnP3Plus_FireZone")
WeatherZoneTable <- datasheet(myScenario, "burnP3Plus_WeatherZone")

# Create function to test if datasheets are empty
isDatasheetEmpty <- function(ds){
  if (nrow(ds) == 0) {
    return(TRUE)
  }
  if (all(is.na(ds))) {
    return(TRUE)
  }
  return(FALSE)
}

# Import relevant rasters, allowing for missing elevation
fuelsRaster <- rast(datasheet(myScenario, "burnP3Plus_LandscapeRasters")[["FuelGridFileName"]])
elevationRaster <- tryCatch(
  rast(datasheet(myScenario, "burnP3Plus_LandscapeRasters")[["ElevationGridFileName"]]),
  error = function(e) NULL)

# FireSTARR requires raster width and heigh to be divisible by 16 and sets border pixels to be Non-fuel
# - To handle this, we need to pad rasters by at least one pixel and enough to be divisble by 16
# - Floor and ceiling are used to handle cases where an odd number of pixels need to be added
# - Also note that adding a positive integer to xmin / ymin unintuitively decreases these values (ie makes the extent larger)
pad_cols <- 16 - ((ncol(fuelsRaster) + 1) %% 16) + 1
pad_rows <- 16 - ((nrow(fuelsRaster) + 1) %% 16) + 1
padded_extent <- ext(fuelsRaster) + 
  c(xmin =  abs(  floor(pad_cols / 2) * xres(fuelsRaster)),
    xmax =  abs(ceiling(pad_cols / 2) * xres(fuelsRaster)),
    ymin =  abs(  floor(pad_rows / 2) * yres(fuelsRaster)),
    ymax =  abs(ceiling(pad_rows / 2) * yres(fuelsRaster)))

## Handle empty values ----
if(isDatasheetEmpty(FuelTypeCrosswalk)) {
  updateRunLog("No fuels code crosswalk found! Using default crosswalk for Canadian Forest Service fuel codes.", type = "warning")
  FuelTypeCrosswalk <- fread(file.path(ssimEnvironment()$PackageDirectory, "Default Fuel Crosswalk.csv"))
  saveDatasheet(myScenario, as.data.frame(FuelTypeCrosswalk), "burnP3PlusFireSTARR_FuelCodeCrosswalk")
}

if(isDatasheetEmpty(OutputOptions)) {
  updateRunLog("No tabular output options chosen. Defaulting to keeping all tabular outputs.", type = "info")
  OutputOptions[1,] <- rep(TRUE, length(OutputOptions[1,]))
  saveDatasheet(myScenario, OutputOptions, "burnP3Plus_OutputOption")
} else if (any(is.na(OutputOptions))) {
  updateRunLog("Missing one or more tabular output options. Defaulting to keeping unspecified tabular outputs.", type = "info")
  OutputOptions <- OutputOptions %>%
    replace(is.na(.), TRUE)
  saveDatasheet(myScenario, OutputOptions, "burnP3Plus_OutputOption")
}

if(isDatasheetEmpty(OutputOptionsSpatial)) {
  updateRunLog("No spatial output options chosen. Defaulting to keeping all spatial outputs and final burn perimeters.", type = "info")
  OutputOptionsSpatial[1,] <- rep(TRUE, length(OutputOptionsSpatial[1,]))
  OutputOptionsSpatial$BurnPerimeter <- "Final"
  saveDatasheet(myScenario, OutputOptionsSpatial, "burnP3Plus_OutputOptionSpatial")
} else if (any(is.na(OutputOptionsSpatial))) {
  updateRunLog("Missing one or more spatial output options. Defaulting to keeping unspecified spatial outputs.", type = "info")
  OutputOptionsSpatial <- OutputOptionsSpatial %>%
    replace(is.na(.), TRUE)
  OutputOptionsSpatial$BurnPerimeter <- replace(OutputOptionsSpatial$BurnPerimeter, OutputOptionsSpatial$BurnPerimeter == TRUE, "Final")
  saveDatasheet(myScenario, OutputOptionsSpatial, "burnP3Plus_OutputOptionSpatial")
}

if (!isDatasheetEmpty(OutputOptionFBPSpatial)) {
  # Check if any unsupported outputs are chosen
  if(OutputOptionFBPSpatial %>%
     pull(Variable) %>%
     str_detect("Rate of Spread|Fire Intensity|Direction", negate = T) %>%
     any)
    updateRunLog("FireSTARR currently only produces Rate of Spread, Fire Intensity, and Spread Direction FBP burn maps. Other spatial FBP output options will be ignored.", type = "warning")

  # Fill missing values for all but Percentile outputs, which are left as NA to indicate non-use
  OutputOptionFBPSpatial <- OutputOptionFBPSpatial %>%
    mutate(across(
      any_of(c("Average", "Minimum", "Maximum", "Median", "Individual")),
      \(x) replace_na(x, FALSE)))
  
  saveDatasheet(myScenario, OutputOptionFBPSpatial, "burnP3Plus_OutputOptionFBPSpatial")

  # Parse table to determine which outputs should be generated
  outputComponentsToKeepDisplayName <- OutputOptionFBPSpatial %>%
    dplyr::filter(any(Average, Minimum, Maximum, Median, Individual, as.logical(c(Percentile1, Percentile2, Percentile3)))) %>%
    pull(Variable)

  # Set a flag to decide whether or not to handle secondary outputs
  keepSecondaries <- length(outputComponentsToKeepDisplayName) > 0
} else {
  # Set flags to not save FBP outputs
  outputComponentsToKeepDisplayName <- character(0)
  keepSecondaries <- F
}

if(isDatasheetEmpty(BatchOption)) {
  updateRunLog("No batch size chosen. Defaulting to batches of 250 iterations.", type = "info")
  BatchOption[1,] <- c(250)
  saveDatasheet(myScenario, BatchOption, "burnP3Plus_BatchOption")
}

if(isDatasheetEmpty(ResampleOption)) {
  updateRunLog("No Minimum Fire Size chosen.\nDefaulting to a Minimum Fire Size of 0ha and with no extra fires. \nPlease see the Fire Resampling Options table for more details.", type = "info")
  ResampleOption[1,] <- c(0,0)
  saveDatasheet(myScenario, ResampleOption, "burnP3Plus_FireResampleOption")
}

# Handle unsupported inputs
if(!isDatasheetEmpty(WindGrid)) {
  updateRunLog("FireSTARR currently does not support Wind Grids. Wind Grid options ignored.", type = "warning")
}

if (isDatasheetEmpty(GreenUp)) {
  GreenUp[1, ] <- c(NA, TRUE)
  saveDatasheet(myScenario, GreenUp, "burnP3Plus_GreenUp")
} else if (is.character(GreenUp$GreenUp)) GreenUp$GreenUp <- GreenUp$GreenUp != "No"

if (isDatasheetEmpty(Curing)) {
  Curing[1, ] <- c(NA, 75L)
  saveDatasheet(myScenario, Curing, "burnP3Plus_Curing")
}

# if(!isDatasheetEmpty(FuelLoad)) {
#   updateRunLog("FireSTARR transformer currently does not support manually specifying Fuel Loading.", type = "warning")
# }

# Fill missing season values

# Define function to fill missing season values and save changes back to library
fill_season <- function(datasheet, datasheet_name = "", update_library = F) {
  datasheet <- datasheet %>%
    mutate(
      Season = if(!exists("Season", where = .)) NA_character_ else as.character(Season),
      Season = replace_na(Season, "All"))

  if (update_library)
    saveDatasheet(myScenario, datasheet, datasheet_name)

  return(datasheet)
}

DeterministicIgnitionLocation <- fill_season(DeterministicIgnitionLocation, "burnP3Plus_DeterministicIgnitionLocation", TRUE)
GreenUp <- fill_season(GreenUp, "burnP3Plus_GreenUp", TRUE)
Curing <- fill_season(Curing, "burnP3Plus_Curing", TRUE)
# FuelLoad <- fill_season(FuelLoad, "burnP3Plus_FuelLoad", TRUE) # Currently not supported by FireSTARR

if(isDatasheetEmpty(FireZoneTable))
  FireZoneTable <- data.frame(Name = "", ID = 0)
if(isDatasheetEmpty(WeatherZoneTable))
  WeatherZoneTable <- data.frame(Name = "", ID = 0)

## Check raster inputs for consistency ----

# Ensure fuels crs can be converted to Lat / Long
test.point <- vect(matrix(crds(fuelsRaster)[1,],ncol=2), crs = crs(fuelsRaster))
# Ensure fuels crs can be converted to Lat / Long
if(test.point %>% is.lonlat){stop("Incorrect coordinate system. Projected coordinate system required, please reproject your grids.")}
tryCatch(test.point %>% terra::project("epsg:4326"), error = function(e) stop("Error parsing provided Fuels map. Cannot calculate Latitude and Longitude from provided Fuels map, please check CRS."))

# Define function to check input raster for consistency
checkSpatialInput <- function(x, name, checkProjection = T, warnOnly = F) {
  # Only check if not null
  if(!is.null(x)) {
    # Ensure comparable number of rows and cols in all spatial inputs
      if(nrow(fuelsRaster) != nrow(x) | ncol(fuelsRaster) != ncol(x))
        if(warnOnly) {
          updateRunLog("Number of rows and columns in ", name, " map do not match Fuels map. Please check that the extent and resolution of these maps match.", type = "warning")
          invisible(NULL)
        } else
          stop("Number of rows and columns in ", name, " map do not match Fuels map. Please check that the extent and resolution of these maps match.")
    
    # Info if CRS is not matching
    if(checkProjection)
      if(crs(x) != crs(fuelsRaster))
        updateRunLog("Projection of ", name, " map does not match Fuels map. Please check that the CRS of these maps match.", type = "info")
  }
  
  # Silently return for clean pipelining
  invisible(x)
}

# Check optional inputs
checkSpatialInput(elevationRaster, "Elevation")

## Set constants ----

# Names and output file suffixes of FireSTARR-specific secondary outputs
outputComponentNames <- c("RateOfSpread", "FireIntensity", "SpreadDirection")
outputComponentRawSuffix <- c("_ros", "_intensity", "_raz") # Note that intensity outputs just end in "_.tif"
outputComponentCleanSuffix <- c("ros", "fi", "raz")

## Extract relevant parameters ----

# Batch sizes to use to limit disk usage of output files
batchSize <- BatchOption$BatchSize

# Determine which, if any, extra ignitions (in iteration 0) this job is responsible for burning
extraIgnitionIDs <- DeterministicIgnitionLocation %>%
    filter(Iteration == 0) %>%
    pull(FireID)

# Define function to determine if the current job is multiprocessed
getRunContext <- function() {
  libraryPath <- ssimEnvironment()$LibraryFilePath %>% normalizePath()
  libraryName <- libraryPath %>% basename %>% {tools::file_path_sans_ext(.)}

  # Libraries are identified as remote if the path includes the Parallel folder and library follows the Job-<jobid> naming convention
  isParallel <- libraryPath %>%
    str_split("/|(\\\\)") %>%
    pluck(1) %>%
    str_detect("MultiProc") %>%
    any %>%
    `&`(str_detect(libraryName, "Job-\\d"))

  # Return if false
  if (!isParallel)
    return(list(isParallel = F, numJobs = 1, jobIndex = 1))

  # Otherwise parse number of jobs and current job index
  numJobs <- libraryPath %>%
    dirname() %>%
    list.files("Job-\\d+.ssim.temp") %>%
    length()
  jobIndex <- str_extract(libraryName, "\\d+") %>% as.integer()

  return(list(isParallel = T, numJobs = numJobs, jobIndex = jobIndex))
}

# Determine if jobs are being multiprocessed
runContext <- getRunContext()

# Determine which subset of the extra iterations this job is responsible for
if(runContext$numJobs > 1 & length(extraIgnitionIDs) > 0)
  extraIgnitionIDs <- split(extraIgnitionIDs, cut(seq_along(extraIgnitionIDs), runContext$numJobs, labels = F)) %>% pluck(as.character(runContext$jobIndex))

# Filter deterministic tables accordingly

DeterministicIgnitionLocation <- DeterministicIgnitionLocation %>%
  filter(Iteration %in% iterations | (Iteration == 0 & FireID %in% extraIgnitionIDs))
DeterministicBurnCondition <- DeterministicBurnCondition %>%
  filter(Iteration %in% iterations | (Iteration == 0 & FireID %in% extraIgnitionIDs))

# Burn maps must be kept to generate summarized maps later, this boolean summarizes
# whether or not burn maps are needed
saveBurnMaps <- any(OutputOptionsSpatial$BurnMap, OutputOptionsSpatial$SeasonalBurnMap,
                    OutputOptionsSpatial$BurnProbability, OutputOptionsSpatial$SeasonalBurnProbability,
                    OutputOptionsSpatial$RelativeBurnProbability, OutputOptionsSpatial$SeasonalRelativeBurnProbability,
                    OutputOptionsSpatial$BurnCount, OutputOptionsSpatial$SeasonalBurnCount,
                    OutputOptionsSpatial$AllPerim, keepSecondaries,
                    OutputOptionsSpatial$BurnPerimeter != "No") # Note that FireSTARR must produce raster outputs in order for them to be vectorized

# Decide whether or not to save outputs seasonally
saveSeasonalBurnMaps <- any(OutputOptionsSpatial$SeasonalBurnMap,
                            OutputOptionsSpatial$SeasonalBurnProbability,
                            OutputOptionsSpatial$SeasonalRelativeBurnProbability,
                            OutputOptionsSpatial$SeasonalBurnCount)

minimumFireSize <- ResampleOption$MinimumFireSize

# Combine fuel type definitions with codes if provided
if(!isDatasheetEmpty(FuelTypeCrosswalk)) {
  FuelType <- FuelType %>%
    left_join(FuelTypeCrosswalk, by = c("Name" = "FuelType"))
} else
  FuelType <- FuelType %>%
    mutate(Code = Name)

## Error check fuels ----

# Ensure all fuel types are assigned to a fuel code
if(any(is.na(FuelType$Code)))
  stop("Could not find a valid FireSTARR Fuel Code for one or more Fuel Types. Please add FireSTARR Fuel Code Crosswalk records for the following Fuel Types in the project scope: ", 
       FuelType %>% filter(is.na(Code)) %>% pull(Name) %>% str_c(collapse = "; "))

# Ensure all fuel codes are valid
# - This should only occur if the Fuel Code Crosswalk is empty and Fuel Type names are being used as codes
if(any(!FuelType$Code %in% ValidFuelCodes))
  stop("Invalid fuel codes found in the Fuel Type definitions. Please consider setting an exlicit Fuel Code Crosswalk for FireSTARR in the project scope.")

# Ensure that there are no fuels present in the grid that are not tied to a valid code
fuelIdsPresent <- fuelsRaster %>% unique() %>% pull
if(any(!fuelIdsPresent %in% c(FuelType$ID, NaN)))
  stop("Found one or more values in the Fuels Map that are not assigned to a known Fuel Type. Please add definitions for the following Fuel IDs: ", 
       dplyr::setdiff(fuelIdsPresent, data.frame(Fuels = FuelType[,"ID"])) %>% str_c(collapse = " "))

## Setup files and folders ----

# Create temp folder, ensure it is empty
tempDir <- ssimEnvironment()$TempDirectory %>%
  str_replace_all("\\\\", "/") %>%
  file.path("growth-firestarr/")
unlink(tempDir, recursive = T, force = T)
dir.create(tempDir, showWarnings = F)

# Copy FireSTARR executable
setwd(tempDir)

# Select the appropriate executable for the system OS
if(.Platform$OS.type == "unix") {
  firestarrExecutable <- "./tbd"
  
  # Ensure external dependencies are available
  external_libs <- system2("ldconfig", "-p")
  if(!all(
    str_detect(external_libs, "libgeotiff.so.5"),
    str_detect(external_libs, "libtiff.so.6"),
    str_detect(external_libs, "libproj.so.25")))
    stop("BurnP3+ FireSTARR on Linux currently requires one or more system dependencies that were not found. Please ensure the following packages or their equivalents for your distribution are installed: `libgeotiff5`, `libtiff6`, `libproj25")
} else {
  firestarrExecutable <- "tbd.exe"
}
firestarrSettings <- "settings.ini"
file.copy(file.path(ssimEnvironment()$PackageDirectory, firestarrExecutable), tempDir, overwrite = T)
file.copy(file.path(ssimEnvironment()$PackageDirectory, firestarrSettings), tempDir, overwrite = T)

# Set as executable if in linux
if(.Platform$OS.type == "unix")
  system2("chmod", c("+x", firestarrExecutable))
  
# Find look up for internal fuel names
InternalFuelNameFile <- file.path(ssimEnvironment()$PackageDirectory, "Internal Fuel Names.csv")

# Set names for model input files
fuelsRasterFile     <- file.path(tempDir, "rasters", "default", "fuel_16_0.tif")
elevationRasterFile <- file.path(tempDir, "rasters", "default", "dem_16_0.tif")
fuelLookupFile      <- file.path(tempDir, "fuel.lut")

# Create folders for various outputs
weatherFolder <- "weathers"
gridOutputFolder <- "outputs"
shapeOutputFolder <- "shapes"
accumulatorOutputFolder <- "accumulator"
seasonalAccumulatorOutputFolder <- "accumulator-seasonal"
allPerimOutputFolder <- "allperim"
secondaryOutputFolder <- "secondary"
unlink(weatherFolder, recursive = T, force = T)
unlink(gridOutputFolder, recursive = T, force = T)
unlink(shapeOutputFolder, recursive = T, force = T)
unlink(accumulatorOutputFolder, recursive = T, force = T)
unlink(allPerimOutputFolder, recursive = T, force = T)
unlink(secondaryOutputFolder, recursive = T, force = T)
dir.create(weatherFolder, showWarnings = F)
dir.create(gridOutputFolder, showWarnings = F)
dir.create(shapeOutputFolder, showWarnings = F)
dir.create(accumulatorOutputFolder, showWarnings = F)
dir.create(seasonalAccumulatorOutputFolder, showWarnings = F)
dir.create(allPerimOutputFolder, showWarnings = F)
dir.create(secondaryOutputFolder, showWarnings = F)

# Create path for geopackage for storing vector outputs
# - Having a unique name for each job (if multiprocessed) helps organize things during merge
geopackage_path <- 
  str_c(
    "burn-perimeters",
    ifelse(runContext$isParallel, str_c("-", runContext$jobIndex), "")) %>%
  str_c(".gpkg") %>%
  file.path(shapeOutputFolder, .)

# Note geopackage recommends `_` for word separation in table, feature, etc names
geopackage_layer_name <-
  str_c(
    str_to_lower(OutputOptionsSpatial$BurnPerimeter),
    "_burn_perimeters"
  )

## Function Definitions ----

### Convenience and conversion functions ----

# Function to time code by returning a clean string of time since this function was last called
updateBreakpoint <- function() {
  # Calculate time since last breakpoint
  newBreakPoint <- proc.time()
  elapsed <- (newBreakPoint - currentBreakPoint)['elapsed']
  
  # Update current breakpoint
  currentBreakPoint <<- newBreakPoint
  
  # Return cleaned elapsed time
  if (elapsed < 60) {
    return(str_c(round(elapsed), "sec"))
  } else if (elapsed < 60^2) {
    return(str_c(round(elapsed / 60, 1), "min"))
  } else
    return(str_c(round(elapsed / 60 / 60, 1), "hr"))
}

# Define a function to facilitate recoding values using a lookup table
lookup <- function(x, old, new) dplyr::recode(x, !!!set_names(new, old))

# FS outputs don't always have exactly the same CRS as the inputs even though things don't get reprojected
# - this utitily function overrides (not projects) the crs quietly and returns the object (for piping)
set_crs <- function(x, template_crs) {
  crs(x) <- template_crs
  return(x)
}

# Function to delete files in file
resetFolder <- function(path) {
  list.files(path, full.names = T) %>%
    unlink(recursive = T, force = T)
  invisible()
}

# Function to convert from latlong to cell index
cellFromLatLong <- function(x, lat, long) {
  xy_coords <- data.frame(long=long, lat=lat) %>%
    st_as_sf(crs = "EPSG:4326", coords = c("long","lat")) %>%
    st_transform(crs = crs(x)) %>%
    st_coordinates
  
  return(cellFromXY(x, xy = xy_coords))
}

# Function to get median julian day from season
# - 2001 is default to avoid leap years
getSeasonMedianDate <- function(season, year = 2001) {
  # Extract Julian day
  julian_day <- SeasonTable %>%
    dplyr::filter(Name == season) %>%
    pull(JulianDay)

  # Create date object
  d <- lubridate::ymd(20010101)

  # Set julian day and year
  lubridate::yday(d) <- julian_day
  lubridate::year(d) <- year

  return(d)
}

# Function to assign crs from a template to an object and return object
alignOutputs <- function(x, template, binarize = T) {
  # FireSTARR overwrites crs, but does not reproject
  # - Assign it back to the template CRS
  crs(x) <- crs(template)
  output <- x %>%
    crop(template) %>%
    extend(template)
  
  # FireStarr returns NA instead of zero
  # - values can either be binarized to 1 / 0 or just have NA reclassed to zero
  if(binarize) {
    output <- output %>%
      is.na %>% 
      `!`       
  } else {
    output <- output %>%
      classify(matrix(c(NA, 0), ncol = 2))
  }

  return(output)
}

# Get burn area from output csv
getBurnArea <- function(inputFile) {
  # Handle vector of inputs in case outputs were were produced daily
  inputFile <- tail(inputFile, 1)

  # Handle failed burns
  if (is.na(inputFile)) return(0)

  # Find last file of burn sizes
  sizeFile <- list.files(dirname(inputFile), "sizes", full.names = T) %>%
    tail(1)
  
  if (length(sizeFile) != 1) stop(length(sizeFile), inputFile, "Cannot find file of fire sizes! This should be produced by FireStarr for all valid runs")

  # Read and return size in hectares
  fread(sizeFile) %>%
    pull %>%
    tail(1) %>%
    return
}

# Get burn areas from all generated output files
getBurnAreas <- function(rawOutputGridPaths) {
  # Calculate burn areas for each fire
  burnAreas <- c(NA_real_)
  length(burnAreas) <- length(rawOutputGridPaths)

  burnAreas <- unlist(lapply(rawOutputGridPaths[seq_along(burnAreas)],getBurnArea))
  
  return(burnAreas)
}

# Function to determine which fires should be kept after resampling
getResampleStatus <- function(burnSummary) {
  burnSummary %>%
    mutate(
      ResampleStatus = case_when(
        Area < minimumFireSize ~ "Discarded",
        Iteration == 0         ~ "Extra",
        TRUE                   ~ "Kept"
      )) %>%
    return()
}

# Function to convert, accumulate, and clean up raw outputs
processOutputs <- function(batchOutput, rawOutputGridPaths) {
  # Identify which unique fire ID's belong to each iteration
  # - bind_rows is used to ensure iterations aren't lost if all fires in an iteration are discarded due to size
  batchOutput <- batchOutput %>%
    filter(ResampleStatus == "Kept" | ResampleStatus == "Extra") %>%
    bind_rows(tibble(Iteration = unique(batchOutput$Iteration)))
    
  # Summarize the FireIDs to export by Iteration
  ignitionsToExportTable <- batchOutput %>%
    dplyr::select(Iteration, UniqueFireID, FireID, Season) %>%
    group_by(Iteration) %>%
    summarize(UniqueFireIDs = list(UniqueFireID),
              FireIDs = list(FireID),
              Seasons = list(Season))
  
  # Generate burn count maps
  for (i in seq_len(nrow(ignitionsToExportTable))){
    generateBurnAccumulators(Iteration = ignitionsToExportTable$Iteration[i], UniqueFireIDs = ignitionsToExportTable$UniqueFireIDs[[i]], burnGrids = rawOutputGridPaths, FireIDs = ignitionsToExportTable$FireIDs[[i]], Seasons = ignitionsToExportTable$Seasons[[i]])
    invisible(gc())
  }
}

# Function to call FireSTARR on the (global) parameter file
runFireSTARR <- function(UniqueBatchFireIndex, Latitude, Longitude, numDays, Season, Curing, GreenUp, DC, DMC, FFMC, ...) {
  outputFolder <- file.path(gridOutputFolder, str_pad(UniqueBatchFireIndex, 5, pad="0"))
  dir.create(outputFolder, showWarnings = F)

  # Get median date of current season for ignition date
  ignDate <- getSeasonMedianDate(Season) %>%
    format("%Y-%m-%d") # yyyy-mm-dd is expected by FireSTARR
  
  # Generate outputs daily if daily burn perimeters are required, otherwise just produce final outputs
  if(OutputOptionsSpatial$BurnPerimeter == "Daily")
    numDays <- seq(numDays)
  
  firestarr_args <- 
    c(outputFolder,
      ignDate, 
      Latitude, Longitude,
      "13:00", # Mock start time set in weather files
      "--ffmc", FFMC,
      "--dmc", DMC,
      "--dc", DC,
      "--output_date_offsets", str_c("[", str_c(numDays, collapse = ","), "]"),
      "--raster-root rasters",
      "-s --occurrence --no-intensity --no-probability --deterministic",
      if(keepSecondaries) "-i" else NULL, # The individual burn map flag (-i) is used to generate all secondary outputs
      "-q -q -q -q", # Reduce output level multiple times for silent output
      "--curing", Curing,
      if(GreenUp) "--force-greenup" else "--force-no-greenup",
      "--wx", str_c(weatherFolder, "/weather", UniqueBatchFireIndex, ".csv"))

  # Log arguemnts used for run
  fwrite(list(c(str_c("./", firestarrExecutable), firestarr_args)), "fs-arguments.log", eol = " ")

  system2(firestarrExecutable, firestarr_args, stdout = FALSE) # For debugging, consider saving stdout to file instead
}

runFireSTARRBatch <- function(ignitionData) {
  resetFolder(gridOutputFolder)

  Sys.setenv("PROJ_LIB" = ssim_proj_lib)
  
  ignitionData %>%
    mutate(
      UniqueBatchFireIndex = row_number(),
      numDays = map_int(data, nrow),
      DC      = map_dbl(data, ~ .x %>% pull(DroughtCode) %>% head(1)),
      DMC     = map_dbl(data, ~ .x %>% pull(DuffMoistureCode) %>% head(1)),
      FFMC    = map_dbl(data, ~ .x %>% pull(FineFuelMoistureCode) %>% head(1))) %>%
    pwalk(runFireSTARR)

  Sys.unsetenv("PROJ_LIB")
}

# Function to run one batch of iterations
runBatch <- function(batchInputs) {
  # Generate batch-specific inputs
  # - Unnest and process ignition info
  batchInputs <- unnest(batchInputs, data)
  
  # - Unnest and process weather info
  batchWeather <- unnest(batchInputs, data)
  generateWeatherFiles(batchWeather)
  
  # Run FireSTARR on the batch
  runFireSTARRBatch(batchInputs)
  
  # Get relative paths to all raw outputs
  rawOutputGridPaths <- list.dirs(gridOutputFolder)[-1] %>%
    map(~ .x %>% list.files("occurrence_.*tif$", full.names = T)) %>%
    map(~ if (length(.x) == 0) {return(NA)} else return(.x)) # Catch zero length outputs just in case
  
  # Get burn areas
  burnAreas <- getBurnAreas(rawOutputGridPaths)

  # Convert and save spatial outputs as needed
  batchOutput <- batchInputs %>%
    select(Iteration, FireID, Season) %>%
    mutate(
      UniqueFireID = row_number(),
      Area = burnAreas) %>%
    getResampleStatus()
    
  # Save GeoTiffs if needed
  if(saveBurnMaps)
    processOutputs(batchOutput, rawOutputGridPaths)
  
  # Clear up temp files
  resetFolder(gridOutputFolder)
  
  # Update Progress Bar
  progressBar("step")
  progressBar(type = "message", message = "Growing fires...")
  
  # Return relevant outputs
  batchOutput %>%
    select(-UniqueFireID, -Season) %>%
    return()
}

### File generation functions ----

# Function to convert daily weather data for every day of burning to format
# expected by C2F and save to file
generateWeatherFile <- function(weatherData, UniqueBatchFireIndex, season, year = 2001) {
  ignDate <- getSeasonMedianDate(season, year)

  weatherData %>%
    # To convert daily weather to hourly, we need to repeat each row for every
    # hour burned that day and pad the rest of the day with zeros. To do this,
    # we first generate and append a row of all zeros.
    add_row() %>%
    mutate_all(function(x) c(head(x, -1), 0)) %>%
    # Next we use slice to repeat rows as needed.
    slice(pmap(.,
      function(BurnDay, HoursBurning, ..., zeroRowID) {
        if (BurnDay != 0) {
          c(rep(BurnDay, HoursBurning), rep(zeroRowID, 24 - HoursBurning))
        }
      },
      zeroRowID = nrow(.)
    ) %>%
      unlist()) %>%
    # Next we add a mock date (June 1). Note this is also required when calling firestarr
    mutate(
      time = (row_number() + 12) %% 24,
      date = as.integer((row_number() + 12) / 24) + ignDate,
      date = str_c(date, " ", str_pad(time, 2, pad="0"), ":00:00") # Default date printing format of lubridate happens to match yyyy-mm-dd format FireSTARR expects
    ) %>%
    # Fires are started on the first day at 1pm, but some versions of tbd require weather from midnight
    # - We accomplish this by repeating the first day's weather as needed and updating the time accordingly
    (function(x) { 
      slice(x, rep(1, 13)) %>%
      mutate(date = str_c(ignDate, " ", str_pad(0:12, 2, pad="0"), ":00:00")) %>%
      bind_rows(x)}) %>%

    # Finally we rename and reorder columns and write to file
    dplyr::transmute(Scenario = 0, Date = date, PREC = Precipitation, TEMP = Temperature, RH = RelativeHumidity, WS = WindSpeed, WD = WindDirection, FFMC = FineFuelMoistureCode, DMC = DuffMoistureCode, DC = DroughtCode, ISI = InitialSpreadIndex, BUI = BuildupIndex, FWI = FireWeatherIndex) %>%
    fwrite(str_c(weatherFolder, "/weather", UniqueBatchFireIndex, ".csv"))
  invisible()
}

# Function to split deterministic burn conditions into separate weather files by iteration and fire id
generateWeatherFiles <- function(DeterministicBurnCondition){
  # Clear out old weather files if present
  resetFolder(weatherFolder)
  
  # Generate files as needed
  DeterministicBurnCondition %>%
    dplyr::select(-starts_with("Timestep")) %>%
    group_by(Iteration, FireID, Season) %>%
    nest() %>%
    ungroup() %>%
    arrange(Iteration, FireID) %>%
    transmute(weatherData = data, UniqueBatchFireIndex = row_number(), season = Season) %>%
    pmap(generateWeatherFile)
  invisible()
}

# Function to summarize individual burn grids by iteration
generateBurnAccumulators <- function(Iteration, UniqueFireIDs, burnGrids, FireIDs, Seasons) {
  # For iteration zero (fires for resampling), only save individual burn maps and secondary outputs
  if(Iteration == 0) {
    for(i in seq_along(UniqueFireIDs)){
      if(!is.na(UniqueFireIDs[i]) & !is.na(burnGrids[[UniqueFireIDs[i]]] %>% append(NA, after = 0) %>% tail(1)) ){
        burnArea <- rast(burnGrids[[UniqueFireIDs[i]]] %>% tail(1)) %>% 
          alignOutputs(fuelsRaster)

        burnArea %>%
          mask(fuelsRaster) %>%
          writeRaster(str_c(allPerimOutputFolder, "/it", Iteration,"_fire", FireIDs[i], ".tif"), 
              overwrite = T,
              NAflag = -9999,
              wopt = list(filetype = "GTiff",
                    datatype = "INT4S",
                    gdal = c("COMPRESS=DEFLATE","ZLEVEL=9","PREDICTOR=2")))
        
        if(OutputOptionsSpatial$BurnPerimeter == "Final") {
          burnArea %>%
            # Trim down to only burned area by first converting all non-burned pixels to NA
            classify(matrix(c(0, NA), ncol = 2)) %>%
            trim() %>%
            # Convert and save
            as.polygons() %>%
            set_crs(crs(fuelsRaster)) %>%
            st_as_sf() %>%
            st_cast("MULTIPOLYGON") %>%
            st_buffer(0) %>% # Prevent specific invalidity case that st_make_valid doesn't catch
            st_make_valid() %>%
            mutate(
              Iteration = Iteration,
              FireID = FireIDs[i],
              geometry = geometry,
              .keep = "none"
              ) %>%
            st_write(
              dsn = geopackage_path,
              layer = geopackage_layer_name,
              quiet = TRUE,
              append = TRUE)
        }

        if(OutputOptionsSpatial$BurnPerimeter == "Daily") {
          # For daily perimeters, iterate over daily burn maps to generate burn perimeters
          burn_to_date <- NULL
          for (j in seq_along(burnGrids[[UniqueFireIDs[i]]])) {
            # Update yesterday's burn
            burn_yesterday <- burn_to_date

            # Vectorize current day's grid
            burn_to_date <- rast(burnGrids[[UniqueFireIDs[i]]][j]) %>%
              trim() %>% 
              as.polygons() %>%
              set_crs(crs(fuelsRaster)) %>%
              st_as_sf() %>%
              st_cast("MULTIPOLYGON") %>%
              st_buffer(0) %>% # Prevent specific invalidity case that st_make_valid doesn't catch
              st_make_valid() %>%
              mutate(
                Iteration = Iteration,
                FireID = FireIDs[i],
                BurnDay = j,
                geometry = geometry,
                .keep = "none")
            
            # Subtract previous days burn if not the first day
            if (j == 1) {
              burn_today <- burn_to_date
            } else {
              st_agr(burn_to_date) = "constant"
              burn_today <- burn_to_date %>%
                st_difference(st_geometry(burn_yesterday))
            }

            # Save output
            st_write(
              obj = burn_today,
              dsn = geopackage_path,
              layer = geopackage_layer_name,
              quiet = TRUE,
              append = TRUE)
          }
        }

        # Save requested secondary outputs
        for (component in outputComponentsToKeep) {
          # Find the correct secondary output in the same folder as the burn grid
          inputComponentFileName <- burnGrids[[UniqueFireIDs[i]]] %>%
            tail(1) %>%
            dirname() %>%
            list.files(pattern = str_c("^\\d.*", lookup(component, outputComponentNames, outputComponentRawSuffix), ".tif$"), full.names = T) %>%
            tail(1)
          if (length(inputComponentFileName) > 0) {
            # Generate output file name
            outputComponentFileName <- str_c(secondaryOutputFolder, "/it", Iteration,"_fire", FireIDs[i], "_", lookup(component, outputComponentNames, outputComponentCleanSuffix), ".tif") %>%
              normalizePath(mustWork = F)

            # Rewrite as GeoTiff to output folder
            rast(inputComponentFileName) %>%
              alignOutputs(fuelsRaster, binarize = F) %>%
              # Set zero values to NA for easier summarizing
              classify(matrix(c(0, NA), ncol = 2)) %>%
              writeRaster(outputComponentFileName,
                overwrite = T,
                NAflag = -9999,
                wopt = list(
                  filetype = "GTiff",
                  datatype = "FLT4S",
                  gdal = c("COMPRESS=DEFLATE", "ZLEVEL=9", "PREDICTOR=2")
                )
              )

            # Update corresponding table in SyncroSim
            outputComponentTables[[component]] <<- rbind(
              outputComponentTables[[component]],
              data.frame(
                Iteration = Iteration,
                Timestep = FireIDs[i], # TODO: Separate out timestep and fire ID
                FireID = FireIDs[i],
                FileName = outputComponentFileName
              )
            )
          }
          unlink(inputComponentFileName)
        }
      }
    }
    return()
  }
  
  # initialize empty matrix
  accumulator <- rast(fuelsRaster, vals = 0)

  # initialize a list of empty matrices for each season
  seasonValues <- SeasonTable %>%
    filter(Name != "All") %>%
    pull(Name) %>%
    unique
  seasonalAccumulators <- accumulator %>% 
    list() %>%
    rep(length(seasonValues)) %>%
    set_names(seasonValues)
  
  # Combine burn grids
  for(i in seq_along(UniqueFireIDs)){
    if(!is.na(UniqueFireIDs[i]) & !is.na(burnGrids[[UniqueFireIDs[i]]] %>% append(NA, after = 0)  %>% tail(1)) ){
      # Read in and add current burn map
      burnArea <- rast(burnGrids[[UniqueFireIDs[i]]] %>% tail(1)) %>%
        alignOutputs(fuelsRaster)
        
      accumulator <- sum(accumulator, burnArea, na.rm = T)
      
      # Add to seasonal accumulator
      if(saveSeasonalBurnMaps) {
        thisSeason <- Seasons[i]
        if (thisSeason %in% seasonValues)
          seasonalAccumulators[[thisSeason]] <- sum(seasonalAccumulators[[thisSeason]], burnArea, na.rm = T)
      }
      
      if(OutputOptionsSpatial$AllPerim == T){
        burnArea %>%
          mask(fuelsRaster) %>%
          writeRaster(str_c(allPerimOutputFolder, "/it", Iteration,"_fire", FireIDs[i], ".tif"), 
              overwrite = T,
              NAflag = -9999,
              wopt = list(filetype = "GTiff",
                    datatype = "INT4S",
                    gdal = c("COMPRESS=DEFLATE","ZLEVEL=9","PREDICTOR=2")))
      }

      if(OutputOptionsSpatial$BurnPerimeter == "Final") {
        burnArea %>%
          # Trim down to only burned area by first converting all non-burned pixels to NA
          classify(matrix(c(0, NA), ncol = 2)) %>%
          trim() %>%
          # Convert and save
          as.polygons() %>%
          set_crs(crs(fuelsRaster)) %>%
          st_as_sf() %>%
          st_cast("MULTIPOLYGON") %>%
          st_buffer(0) %>% # Prevent specific invalidity case that st_make_valid doesn't catch
          st_make_valid() %>%
          mutate(
            Iteration = Iteration,
            FireID = FireIDs[i],
            geometry = geometry,
            .keep = "none"
            ) %>%
          st_write(
            dsn = geopackage_path,
            layer = geopackage_layer_name,
            quiet = TRUE,
            append = TRUE)
      }

      if(OutputOptionsSpatial$BurnPerimeter == "Daily") {
        # For daily perimeters, iterate over daily burn maps to generate burn perimeters
        burn_to_date <- NULL
        for (j in seq_along(burnGrids[[UniqueFireIDs[i]]])) {
          # Update yesterday's burn
          burn_yesterday <- burn_to_date

          # Vectorize current day's grid
          burn_to_date <- rast(burnGrids[[UniqueFireIDs[i]]][j]) %>%
            trim() %>% 
            as.polygons() %>%
            set_crs(crs(fuelsRaster)) %>%
            st_as_sf() %>%
            st_cast("MULTIPOLYGON") %>%
            st_buffer(0) %>% # Prevent specific invalidity case that st_make_valid doesn't catch
            st_make_valid() %>%
            mutate(
              Iteration = Iteration,
              FireID = FireIDs[i],
              BurnDay = j,
              geometry = geometry,
              .keep = "none")
          
          # Subtract previous days burn if not the first day
          if (j == 1) {
            burn_today <- burn_to_date
          } else {
            st_agr(burn_to_date) = "constant"
            burn_today <- burn_to_date %>%
              st_difference(st_geometry(burn_yesterday))
          }

          # Save output
          st_write(
            obj = burn_today,
            dsn = geopackage_path,
            layer = geopackage_layer_name,
            quiet = TRUE,
            append = TRUE)
        }
      }

      # Save requested secondary outputs
      for (component in outputComponentsToKeep) {
        # Find the correct secondary output in the same folder as the burn grid
        inputComponentFileName <- burnGrids[[UniqueFireIDs[i]]] %>%
          tail(1) %>%
          dirname() %>%
          list.files(pattern = str_c("^\\d.*", lookup(component, outputComponentNames, outputComponentRawSuffix), ".tif$"), full.names = T) %>%
          tail(1)
        if (length(inputComponentFileName) > 0) {
          # Generate output file name
          outputComponentFileName <- str_c(secondaryOutputFolder, "/it", Iteration,"_fire", FireIDs[i], "_", lookup(component, outputComponentNames, outputComponentCleanSuffix), ".tif") %>%
            normalizePath(mustWork = F)

          # Rewrite as GeoTiff to output folder
          rast(inputComponentFileName) %>%
            alignOutputs(fuelsRaster, binarize = F) %>%
            # Set zero values to NA for easier summarizing
            classify(matrix(c(0, NA), ncol = 2)) %>%
            writeRaster(outputComponentFileName,
              overwrite = T,
              NAflag = -9999,
              wopt = list(
                filetype = "GTiff",
                datatype = "FLT4S",
                gdal = c("COMPRESS=DEFLATE", "ZLEVEL=9", "PREDICTOR=2")
              )
            )

          # Update corresponding table in SyncroSim
          outputComponentTables[[component]] <<- rbind(
            outputComponentTables[[component]],
            data.frame(
              Iteration = Iteration,
              Timestep = FireIDs[i], # TODO: Separate out timestep and fire ID
              FireID = FireIDs[i],
              FileName = outputComponentFileName
            )
          )
        }
        unlink(inputComponentFileName)
      }
    }
  }

  # Binarize accumulator to burn or not
  accumulator[accumulator != 0] <- 1

  # Mask and save as raster
  accumulator %>%
    mask(fuelsRaster) %>%
    writeRaster(str_c(accumulatorOutputFolder, "/it", Iteration, ".tif"), 
                overwrite = T,
                NAflag = -9999,
                wopt = list(filetype = "GTiff",
                    datatype = "INT4S",
                    gdal = c("COMPRESS=DEFLATE","ZLEVEL=9","PREDICTOR=2")))

  # Repeat for each seasonal accumulator
  if(saveSeasonalBurnMaps) {
    for (season in seasonValues) {
      # Binarize accumulator to burn or not
      seasonalAccumulators[[season]][seasonalAccumulators[[season]] != 0] <- 1

      # Mask and save as raster
      seasonalAccumulators[[season]] %>%
        mask(fuelsRaster) %>%
        writeRaster(str_c(seasonalAccumulatorOutputFolder, "/it", Iteration, "-sn", lookup(season, SeasonTable$Name, SeasonTable$SeasonId), ".tif"), 
                    overwrite = T,
                    NAflag = -9999,
                    wopt = list(filetype = "GTiff",
                        datatype = "INT4S",
                        gdal = c("COMPRESS=DEFLATE","ZLEVEL=9","PREDICTOR=2")))
    }
  }
}


updateRunLog("Finished parsing run inputs in ", updateBreakpoint())

# Prepare Shared Inputs ----

# Spatial data
dir.create(dirname(fuelsRasterFile), showWarnings = F, recursive = T)

# FireSTARR assigns all border pixels to non-fuels, so we pad all inputs around by 1 pixel
fuelsRaster %>%
  extend(padded_extent) %>%
  writeRaster(fuelsRasterFile, overwrite = T, datatype = "INT2S", NAflag = -9999, gdal=c("TILED=YES"))
elevationRaster %>%
  extend(padded_extent) %>%
  writeRaster(elevationRasterFile, overwrite = T, datatype = "INT2S", NAflag = -9999, gdal=c("TILED=YES"))

# Reformat fuel lookup table
FuelType %>%
  left_join(fread(InternalFuelNameFile), by = "Code") %>%
  transmute(
    grid_value = ID,
    export_value = ID,
    descriptive_name = str_c(InternalName),
    fuel_type = Code
  ) %>%
  mutate(r = 0, g = 0, b = 0, h = 0, s = 0, l = 0) %>%
  write_csv(fuelLookupFile, escape = "none")

# Calculate ignition location in grid row and column
ignitionLocation <- DeterministicIgnitionLocation %>%
  dplyr::select("Iteration","FireID","Latitude","Longitude","Season") %>%
  arrange("Iteration", "FireID")

# Decide which burn components to keep
# - Convert from display name from UI to internal component names
outputComponentsToKeep <- outputComponentsToKeepDisplayName %>%
  lookup(FBPVariableTable$DisplayName, FBPVariableTable$Name)

# - Initialize list of tables to hold outputs
outputComponentTables <- list()
for (component in outputComponentsToKeep) {
  outputComponentTables[[component]] <- data.frame()
}

# Combine deterministic input tables ----
fireGrowthInputs <- DeterministicBurnCondition %>%
  # Group by iteration and fire ID for the `growFire()` function
  nest(.by = c(Iteration, FireID)) %>%
  
  # Add ignition location information
  left_join(ignitionLocation, c("Iteration", "FireID")) %>%

  # Calculate and append curing and greenup values as columns
  mutate(
    Curing = map_int(Season, function(season)
      Curing %>%
        dplyr::filter(Season %in% c(season, NA, "All")) %>%
        mutate(Season = na_if(Season, "All")) %>% # Replace "All" season with NA so it sorts to end with `arrange`
        arrange(Season) %>%
        pull(Curing) %>%
        pluck(1)),

    GreenUp = map_lgl(Season, function(season)
      GreenUp %>%
        dplyr::filter(Season %in% c(season, NA, "All")) %>%
        mutate(Season = na_if(Season, "All")) %>%
        arrange(Season) %>%
        pull(GreenUp) %>%
        pluck(1))
    ) %>%

  # Split extra ignitions into reasonable batch sizes
  mutate(extraIgnitionsBatch = (row_number() - 1) %/% batchSize + 1, 
         extraIgnitionsBatch = ifelse(Iteration == 0, extraIgnitionsBatch, 0)) %>%

  # Group by just iteration for the `runIteration()` function
  nest(.by = c(Iteration, extraIgnitionsBatch)) %>% 
  dplyr::select(-extraIgnitionsBatch) %>%
  
  # Finally split into batches of the appropriate size
  group_by(batchID = (cumsum(map_int(data, nrow)) - 1) %/% batchSize) %>%
  group_split(.keep = F)

updateRunLog("Finished generating shared inputs in ", updateBreakpoint())

# Grow fires ----
progressBar("begin", totalSteps = length(fireGrowthInputs))
progressBar(type = "message", message = "Growing fires...")

OutputFireStatistic <- fireGrowthInputs %>%
  map_dfr(runBatch)

updateRunLog("Finished burning fires in ", updateBreakpoint())

# Save relevant outputs ----

## Fire statistics table ----
# Generate the table if it is a requested output, or resampling is requested
if(OutputOptions$FireStatistics | minimumFireSize > 0) {
  progressBar(type = "message", message = "Generating fire statistics table...")
  
  # Load necessary rasters and lookup tables
  fireZoneRaster <- tryCatch(
    rast(datasheet(myScenario, "burnP3Plus_LandscapeRasters")[["FireZoneGridFileName"]]),
    error = function(e) NULL) %>%
    checkSpatialInput("Fire Zone", warnOnly = T)
  weatherZoneRaster <- tryCatch(
    rast(datasheet(myScenario, "burnP3Plus_LandscapeRasters")[["WeatherZoneGridFileName"]]),
    error = function(e) NULL) %>%
    checkSpatialInput("Weather Zone", warnOnly = T)
    
  # Add extra information to Fire Statistic table
  OutputFireStatistic <- OutputFireStatistic %>%
    
    # Start by joining summarized burn conditions
    left_join({
      # Start by summarizing burn conditions
      DeterministicBurnCondition %>%
      
        # Only consider iterations this job is responsible for
        filter(Iteration %in% iterations | (Iteration == 0 & FireID %in% extraIgnitionIDs)) %>%
          
        # Summarize burn conditions by fire
        group_by(Iteration, FireID) %>%
        summarize(
          FireDuration = max(BurnDay),
          HoursBurning = sum(HoursBurning)) %>%
        ungroup()},
      by = c("Iteration", "FireID")) %>%
  
      # Determine Fire and Weather Zones if the rasters are present, as well as fuel type of ignition location
      left_join(DeterministicIgnitionLocation, by = c("Iteration", "FireID"))

  # Determine Fire and Weather Zones if the rasters are present, as well as
  # fuel type of ignition location
  OutputFireStatistic$cell <- cellFromXY(
    fuelsRaster,
    xy = data.frame(long=OutputFireStatistic$Longitude,
                    lat=OutputFireStatistic$Latitude) %>%
        st_as_sf(crs = "EPSG:4326",
                coords = c("long","lat")) %>%
        st_transform(crs = crs(fuelsRaster)) %>%
        st_coordinates)

  if (!is.null(weatherZoneRaster)){
    OutputFireStatistic <- OutputFireStatistic %>%
      mutate(
        weatherzoneID = weatherZoneRaster[][cell],
        WeatherZone = lookup(weatherzoneID, WeatherZoneTable$ID, WeatherZoneTable$Name)
      ) %>%
      dplyr::select(-weatherzoneID)
  } else{
    OutputFireStatistic$WeatherZone <- WeatherZoneTable$Name
  }

  if (!is.null(fireZoneRaster)){
    OutputFireStatistic <- OutputFireStatistic %>%
      mutate(
        firezoneID = fireZoneRaster[][cell],
        FireZone = lookup(firezoneID, FireZoneTable$ID, FireZoneTable$Name)
      ) %>%
      dplyr::select(-firezoneID)
  } else{
    OutputFireStatistic$FireZone <- FireZoneTable$Name
  }

  OutputFireStatistic <- OutputFireStatistic %>%
    # Incorporate Lat and Long and add TimeStep manually
    mutate(
      fueltypeID = fuelsRaster[][cell],
      FuelType = lookup(fueltypeID, FuelType$ID, FuelType$Name),
      Timestep = 0) %>%

      # Clean up for saving
      dplyr::select(Iteration, Timestep, FireID, Latitude, Longitude, Season,
                    Cause, FireZone, WeatherZone, FuelType, FireDuration,
                    HoursBurning, Area, ResampleStatus) %>%
      as.data.frame()

    # Output if there are records to save
    if(!isDatasheetEmpty(OutputFireStatistic))
      saveDatasheet(myScenario, OutputFireStatistic, "burnP3Plus_OutputFireStatistic", append = T)
  
  updateRunLog("Finished collecting fire statistics in ", updateBreakpoint())
}

## Burn maps ----
if(saveBurnMaps) {
  progressBar(type = "message", message = "Saving burn maps...")
  
  # Build table of burn maps and save to SyncroSim
  OutputBurnMap <- 
    tibble(
      FileName = list.files(accumulatorOutputFolder, ".tif$", full.names = T) %>% normalizePath(),
      Iteration = str_extract(FileName, "\\d+.tif") %>% str_sub(end = -5) %>% as.integer(),
      Timestep = 0,
      Season = "All") %>%
    filter(Iteration %in% iterations) %>%
    as.data.frame
  
  if(saveSeasonalBurnMaps) {
    # If seasonal burn maps have been saved, append them to the table
    OutputBurnMap <- OutputBurnMap %>%
      bind_rows(
        tibble(
          FileName = list.files(seasonalAccumulatorOutputFolder, ".tif$", full.names = T) %>% normalizePath(),
          Iteration = str_extract(FileName, "\\d+-sn") %>% str_sub(end = -4) %>% as.integer(),
          Timestep = 0,
          Season = str_extract(FileName, "\\d+.tif") %>% str_sub(end = -5) %>% as.integer()) %>%
        mutate(
          Season = lookup(Season, SeasonTable$SeasonId, SeasonTable$Name)) %>%
        filter(Iteration %in% iterations)) %>%
      as.data.frame
  }
  
  # Output if there are records to save
  if(!isDatasheetEmpty(OutputBurnMap))
    saveDatasheet(myScenario, OutputBurnMap, "burnP3Plus_OutputBurnMap")
  
  # Output secondary outputs if present
  if (length(outputComponentsToKeep) > 0) {
    for (i in seq_along(outputComponentTables)) {
      if (!isDatasheetEmpty(outputComponentTables[[i]])) {
        saveDatasheet(myScenario, outputComponentTables[[i]], str_c("burnP3Plus_Output", outputComponentsToKeep[i], "Map"))
      }
    }
  }
  
  updateRunLog("Finished accumulating burn maps in ", updateBreakpoint())
}

## All Perims
if(OutputOptionsSpatial$AllPerim | (saveBurnMaps & minimumFireSize > 0)){
  progressBar(type = "message", message = "Saving individual burn maps...")

  # Build table of burn maps and save to SyncroSim
  OutputAllPerim <- 
    tibble(
      FileName = list.files(allPerimOutputFolder, ".tif$", full.names = T) %>% normalizePath(),
      Iteration = str_extract(FileName, "it\\d+") %>% str_extract("\\d+") %>% as.integer(),
      FireID = str_extract(FileName, "fire\\d+") %>% str_extract("\\d+") %>% as.integer(),
      Timestep = FireID) %>%
    filter(Iteration %in% iterations | (Iteration == 0 & FireID %in% extraIgnitionIDs)) %>%
    as.data.frame
  
  # Output if there are records to save
  if(!isDatasheetEmpty(OutputAllPerim))
    saveDatasheet(myScenario, OutputAllPerim, "burnP3Plus_OutputAllPerim")
  
  updateRunLog("Finished individual burn maps in ", updateBreakpoint())
}


## Burn perimeters ----
if(OutputOptionsSpatial$BurnPerimeter != "No") {
  progressBar(type = "message", message = "Saving burn perimeters...")

  OutputFirePerimeter <-
    tibble(
      FileName = geopackage_path %>% normalizePath(),
      Description = 
        str_c(
          OutputOptionsSpatial$BurnPerimeter,
          " burn perimeters", 
          ifelse(runContext$isParallel, str_c(" - Job ", runContext$jobIndex), ""))
    ) %>%
    as.data.frame()

  saveDatasheet(myScenario, OutputFirePerimeter, "burnP3Plus_OutputFirePerimeter")
  updateRunLog("Finished collecting burn perimeters in ", updateBreakpoint())
}

# Clean up
progressBar("end")

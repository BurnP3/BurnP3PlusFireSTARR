# Clean global environment variables
native_proj_lib <- Sys.getenv("PROJ_LIB")
Sys.unsetenv("PROJ_LIB")

# Check and load packages ----
library(rsyncrosim)
suppressPackageStartupMessages(library(tidyverse))
suppressPackageStartupMessages(library(lubridate))
suppressPackageStartupMessages(library(terra))
suppressPackageStartupMessages(library(data.table))

checkPackageVersion <- function(packageString, minimumVersion){
  result <- compareVersion(as.character(packageVersion(packageString)), minimumVersion)
  if (result < 0) {
    stop("The R package ", packageString, " (", as.character(packageVersion(packageString)), ") does not meet the minimum requirements (", minimumVersion, ") for this version of BurnP3+ FireSTARR. Please upgrade this package and rerun this scenario.", type = "warning")
  } else if (result > 0) {
    updateRunLog("Using a newer version of ", packageString, " (", as.character(packageVersion(packageString)), ") than BurnP3+ FireSTARR was built against (", minimumVersion, ").", type = "info")
  }
}

checkPackageVersion("rsyncrosim", "1.5.0")
checkPackageVersion("tidyverse",  "2.0.0")
checkPackageVersion("terra",      "1.5.21")
checkPackageVersion("dplyr",      "1.1.4")
checkPackageVersion("codetools",  "0.2.20")
checkPackageVersion("data.table", "1.16.0")

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
WindGrid <- datasheet(myScenario, "burnP3Plus_WindGrid", lookupsAsFactors = F, optional = T)
GreenUp <- datasheet(myScenario, "burnP3Plus_GreenUp", lookupsAsFactors = F, optional = T)
Curing <- datasheet(myScenario, "burnP3Plus_Curing", lookupsAsFactors = F, optional = T)
FuelLoad <- datasheet(myScenario, "burnP3Plus_FuelLoad", lookupsAsFactors = F, optional = T)
OutputOptions <- datasheet(myScenario, "burnP3Plus_OutputOption", optional = T)
OutputOptionsSpatial <- datasheet(myScenario, "burnP3Plus_OutputOptionSpatial", optional = T)

# Import relevant rasters, allowing for missing elevation
fuelsRaster <- rast(datasheet(myScenario, "burnP3Plus_LandscapeRasters")[["FuelGridFileName"]])
elevationRaster <- tryCatch(
  rast(datasheet(myScenario, "burnP3Plus_LandscapeRasters")[["ElevationGridFileName"]]),
  error = function(e) NULL)

## Handle empty values ----
if(nrow(FuelTypeCrosswalk) == 0) {
  updateRunLog("No fuels code crosswalk found! Using default crosswalk for Canadian Forest Service fuel codes.", type = "warning")
  FuelTypeCrosswalk <- fread(file.path(ssimEnvironment()$PackageDirectory, "Default Fuel Crosswalk.csv"))
  saveDatasheet(myScenario, as.data.frame(FuelTypeCrosswalk), "burnP3PlusFireSTARR_FuelCodeCrosswalk")
}

if(nrow(OutputOptions) == 0) {
  updateRunLog("No tabular output options chosen. Defaulting to keeping all tabular outputs.", type = "info")
  OutputOptions[1,] <- rep(TRUE, length(OutputOptions[1,]))
  saveDatasheet(myScenario, OutputOptions, "burnP3Plus_OutputOption")
} else if (any(is.na(OutputOptions))) {
  updateRunLog("Missing one or more tabular output options. Defaulting to keeping unspecified tabular outputs.", type = "info")
  OutputOptions <- OutputOptions %>%
    replace(is.na(.), TRUE)
  saveDatasheet(myScenario, OutputOptions, "burnP3Plus_OutputOption")
}

if(nrow(OutputOptionsSpatial) == 0) {
  updateRunLog("No spatial output options chosen. Defaulting to keeping all spatial outputs.", type = "info")
  OutputOptionsSpatial[1,] <- rep(TRUE, length(OutputOptionsSpatial[1,]))
  saveDatasheet(myScenario, OutputOptionsSpatial, "burnP3Plus_OutputOptionSpatial")
} else if (any(is.na(OutputOptionsSpatial))) {
  updateRunLog("Missing one or more spatial output options. Defaulting to keeping unspecified spatial outputs.", type = "info")
  OutputOptionsSpatial <- OutputOptionsSpatial %>%
    replace(is.na(.), TRUE)
  saveDatasheet(myScenario, OutputOptionsSpatial, "burnP3Plus_OutputOptionSpatial")
}

if(nrow(BatchOption) == 0) {
  updateRunLog("No batch size chosen. Defaulting to batches of 250 iterations.", type = "info")
  BatchOption[1,] <- c(250)
  saveDatasheet(myScenario, BatchOption, "burnP3Plus_BatchOption")
}

if(nrow(ResampleOption) == 0) {
  updateRunLog("No Minimum Fire Size chosen.\nDefaulting to a Minimum Fire Size of 1ha.\nPlease see the Fire Resampling Options table for more details.", type = "info")
  ResampleOption[1,] <- c(1,0)
  saveDatasheet(myScenario, ResampleOption, "burnP3Plus_FireResampleOption")
}

# Handle unsupported inputs
if(nrow(WindGrid) != 0) {
  updateRunLog("FireSTARR currently does not support Wind Grids. Wind Grid options ignored.", type = "warning")
}

if(nrow(GreenUp) != 0) {
  updateRunLog("FireSTARR transformer currently does not support Green Up. Green Up options ignored.", type = "warning")
}

if(nrow(Curing) != 0) {
  updateRunLog("FireSTARR transformer currently does not support manually specifying Curing.", type = "warning")
}

if(nrow(FuelLoad) != 0) {
  updateRunLog("FireSTARR transformer currently does not support manually specifying Fuel Loading.", type = "warning")
}

## Check raster inputs for consistency ----

# Ensure fuels crs can be converted to Lat / Long
test.point <- vect(matrix(crds(fuelsRaster)[1,],ncol=2), crs = crs(fuelsRaster))
# Ensure fuels crs can be converted to Lat / Long
if(test.point %>% is.lonlat){stop("Incorrect coordinate system. Projected coordinate system required, please reproject your grids.")}
tryCatch(test.point %>% project("epsg:4326"), error = function(e) stop("Error parsing provided Fuels map. Cannot calculate Latitude and Longitude from provided Fuels map, please check CRS."))

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
    str_detect("Parallel") %>%
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
                    OutputOptionsSpatial$AllPerim)

# Decide whether or not to save outputs seasonally
saveSeasonalBurnMaps <- any(OutputOptionsSpatial$SeasonalBurnMap,
                            OutputOptionsSpatial$SeasonalBurnProbability,
                            OutputOptionsSpatial$SeasonalRelativeBurnProbability,
                            OutputOptionsSpatial$SeasonalBurnCount)

minimumFireSize <- ResampleOption$MinimumFireSize

# Combine fuel type definitions with codes if provided
if(nrow(FuelTypeCrosswalk) > 0) {
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

# Copy FireSTARR executable
setwd(ssimEnvironment()$TempDirectory)

# Select the appropriate executable for the system OS
if(.Platform$OS.type == "unix") {
  firestarrExecutable <- "./FireSTARR"
  stop("FireSTARR transformer currently does not support runs on UNIX. This feature is planned for a future release.")
} else {
  firestarrExecutable <- "tbd.exe"
}
firestarrSettings <- "settings.ini"
file.copy(file.path(ssimEnvironment()$PackageDirectory, firestarrExecutable), ssimEnvironment()$TempDirectory, overwrite = T)
file.copy(file.path(ssimEnvironment()$PackageDirectory, firestarrSettings), ssimEnvironment()$TempDirectory, overwrite = T)

# Set as executable if in linux
if(.Platform$OS.type == "unix")
  system2("chmod", c("+x", firestarrExecutable))
  
# Find look up for internal fuel names
InternalFuelNameFile <- file.path(ssimEnvironment()$PackageDirectory, "Internal Fuel Names.csv")

# Create temp folder, ensure it is empty
tempDir <- "firestarr-inputs"
unlink(tempDir, recursive = T, force = T)
dir.create(tempDir, showWarnings = F)

weatherFolder <- file.path(tempDir, "weathers")
unlink(weatherFolder, recursive = T, force = T)
dir.create(weatherFolder, showWarnings = F)

# Set names for model input files
fuelsRasterFile     <- file.path(tempDir, "rasters", "default", "fuel_16_0.tif")
elevationRasterFile <- file.path(tempDir, "rasters", "default", "dem_16_0.tif")
fuelLookupFile      <- file.path(tempDir, "fuel.lut")

# Create folders for various outputs
gridOutputFolder <- "firestarr-outputs"
accumulatorOutputFolder <- "firestarr-accumulator"
seasonalAccumulatorOutputFolder <- "firestarr-accumulator-seasonal"
allPerimOutputFolder <- "firestarr-allperim"
unlink(gridOutputFolder, recursive = T, force = T)
unlink(accumulatorOutputFolder, recursive = T, force = T)
unlink(allPerimOutputFolder, recursive = T, force = T)
dir.create(gridOutputFolder, showWarnings = F)
dir.create(accumulatorOutputFolder, showWarnings = F)
dir.create(seasonalAccumulatorOutputFolder, showWarnings = F)
dir.create(allPerimOutputFolder, showWarnings = F)

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

# Function to delete files in file
resetFolder <- function(path) {
  list.files(path, full.names = T) %>%
    unlink(recursive = T, force = T)
  invisible()
}

# Function to convert from latlong to cell index
cellFromLatLong <- function(x, lat, long) {
  # Convert list of lat and long to SpatVector, reproject to source crs
  points <- matrix(c(long, lat), ncol = 2) %>%
    vect(crs = "EPSG:4326") %>%
    project(x)

  # Get vector of cell ID's from points
  return(cells(x, points)[, "cell"])
}

# Function to convert a raster and row and column indices to Lat Long
latlonFromRowCol <- function(x, row, col) {
  cellFromRowCol(x, row, col) %>%
    {
      as.points(x, na.rm = F)[.]
    } %>%
    project("EPSG:4326") %>%
    crds() %>%
    return()
}

# Function to convert from latlong to row and col
# - Note that unlike R, firestarr 0-indexes row and col, with rows starting from the bottom up
rowColFromLatLong <- function(x, lat, long) {
  # Convert list of lat and long to SpatVector, reproject to source crs
  points <- matrix(c(long, lat), ncol = 2) %>%
    vect(crs = "EPSG:4326") %>%
    project(x)
  
  # Get vector of cell ID's from points
  cellIDs <- cells(x, points)[, "cell"]
  rowCols <- rowColFromCell(x, cellIDs)

  # FireStarr counts rows from the bottom and both metrics must be 0 indexed
  rowCols[,1] <- nrow(fuelsRaster) - rowCols[,1]
  rowCols[,2] <- rowCols[,2] - 1
  return(rowCols)
}

# Function to assign crs from a template to an object and return object
alignOutputs <- function(x, template) {
  # FireSTARR overwrites crs, but does not reproject
  # - Assign it back to the template CRS
  crs(x) <- crs(template)
  x %>%
    crop(template) %>%
    extend(template) %>%
    is.na %>% # FireStarr returns NA instead of zero
    `!` %>%   # inverts the is.na
    return()
}

# Get burn area from output csv
getBurnArea <- function(inputFile) {
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
  for (i in seq_len(nrow(ignitionsToExportTable)))
    generateBurnAccumulators(Iteration = ignitionsToExportTable$Iteration[i], UniqueFireIDs = ignitionsToExportTable$UniqueFireIDs[[i]], burnGrids = rawOutputGridPaths, FireIDs = ignitionsToExportTable$FireIDs[[i]], Seasons = ignitionsToExportTable$Seasons[[i]])
}

# Function to call FireSTARR on the (global) parameter file
runFireSTARR <- function(UniqueBatchFireIndex, Latitude, Longitude, IgnRow, IgnColumn, numDays, DC, DMC, FFMC, ...) {
  outputFolder <- file.path(gridOutputFolder, str_pad(UniqueBatchFireIndex, 5, pad="0"))
  dir.create(outputFolder, showWarnings = F)
  
  system2(firestarrExecutable,
          c(outputFolder,
            "2000-06-01", # Mock date set in weather files
            Latitude, Longitude,
            "13:00", # Mock start time set in weather files
            "--ffmc", FFMC,
            "--dmc", DMC,
            "--dc", DC,
            "--output_date_offsets", str_c("{", numDays, "}"),
            "-s --occurrence --no-intensity --no-probability --deterministic --force-fuel --rowcol-ignition",
            "--ign-row", IgnRow,
            "--ign-col", IgnColumn,
            "--wx", str_c(weatherFolder, "/weather", UniqueBatchFireIndex, ".csv")))
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
    map_chr(~ .x %>% list.files("occurrence_", full.names = T) %>% append(NA, after = 0) %>% tail(1))
  
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
generateWeatherFile <- function(weatherData, UniqueBatchFireIndex) {
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
      date = as.integer((row_number() + 12) / 24) + ymd(20000601),
      date = str_c(year(date), "-", str_pad(month(date), 2, pad="0"), "-", str_pad(day(date), 2, pad="0"), " ", str_pad(time, 2, pad="0"), ":00:00")
    ) %>%
    # Fires are started on the first day at 1pm, but some versions of tbd require weather from midnight
    # - We accomplish this by repeating the first day's weather as needed and updating the time accordingly
    (function(x) { 
      slice(x, rep(1, 13)) %>%
      mutate(date = str_c("2000-06-01 ", str_pad(0:12, 2, pad="0"), ":00:00")) %>%
      bind_rows(x)}) %>%

    # Finally we rename and reorder columns and write to file
    dplyr::transmute(Scenario = 0, Date = date, PREC = Precipitation, TEMP = Temperature, RH = RelativeHumidity, WS = WindSpeed, WD = WindDirection, FFMC = FineFuelMoistureCode, DMC = DuffMoistureCode, DC = DroughtCode, ISI = InitialSpreadIndex, BUI = BuildupIndex, FWI = FireWeatherIndex) %>%
    fwrite(file.path(weatherFolder, str_c("weather", UniqueBatchFireIndex, ".csv")))
  invisible()
}

# Function to split deterministic burn conditions into separate weather files by iteration and fire id
generateWeatherFiles <- function(DeterministicBurnCondition){
  # Clear out old weather files if present
  resetFolder(weatherFolder)
  
  # Generate files as needed
  DeterministicBurnCondition %>%
    dplyr::select(-starts_with("Timestep")) %>%
    group_by(Iteration, FireID) %>%
    nest() %>%
    ungroup() %>%
    arrange(Iteration, FireID) %>%
    transmute(weatherData = data, UniqueBatchFireIndex = row_number()) %>%
    pmap(generateWeatherFile)
  invisible()
}

# Function to summarize individual burn grids by iteration
generateBurnAccumulators <- function(Iteration, UniqueFireIDs, burnGrids, FireIDs, Seasons) {
  # For iteration zero (fires for resampling), only save individual burn maps
  if(Iteration == 0) {
    for(i in seq_along(UniqueFireIDs)){
      if(!is.na(UniqueFireIDs[i]) & !is.na(burnGrids[UniqueFireIDs[i]]) ){
        rast(burnGrids[UniqueFireIDs[i]]) %>% 
          alignOutputs(fuelsRaster) %>%
          mask(fuelsRaster) %>%
          writeRaster(str_c(allPerimOutputFolder, "/it", Iteration,"_fire_", FireIDs[i], ".tif"), 
              overwrite = T,
              NAflag = -9999,
              wopt = list(filetype = "GTiff",
                    datatype = "INT4S",
                    gdal = c("COMPRESS=DEFLATE","ZLEVEL=9","PREDICTOR=2")))
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
    if(!is.na(UniqueFireIDs[i]) & !is.na(burnGrids[UniqueFireIDs[i]]) ){
      # Read in and add current burn map
      burnArea <- rast(burnGrids[UniqueFireIDs[i]]) %>%
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
          writeRaster(str_c(allPerimOutputFolder, "/it", Iteration,"_fire_", FireIDs[i], ".tif"), 
              overwrite = T,
              NAflag = -9999,
              wopt = list(filetype = "GTiff",
                    datatype = "INT4S",
                    gdal = c("COMPRESS=DEFLATE","ZLEVEL=9","PREDICTOR=2")))
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
        writeRaster(str_c(seasonalAccumulatorOutputFolder, "/it", Iteration, "-sn", lookup(season, SeasonTable$Name, SeasonTable$SeasonID), ".tif"), 
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
writeRaster(fuelsRaster, fuelsRasterFile, overwrite = T, datatype = "INT2S", NAflag = -9999, gdal=c("TILED=YES"))
writeRaster(elevationRaster, elevationRasterFile, overwrite = T, datatype = "INT2S", NAflag = -9999, gdal=c("TILED=YES"))

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
  mutate(
    IgnRow    = rowColFromLatLong(fuelsRaster, Latitude, Longitude)[,1],
    IgnColumn = rowColFromLatLong(fuelsRaster, Latitude, Longitude)[,2]) %>%
  dplyr::select("Iteration","FireID","IgnColumn","IgnRow","Latitude","Longitude","Season") %>%
  arrange("Iteration", "FireID")

# Combine deterministic input tables ----
fireGrowthInputs <- DeterministicBurnCondition %>%
  # Group by iteration and fire ID for the `growFire()` function
  nest(.by = c(Iteration, FireID)) %>%
  
  # Add ignition location information
  left_join(ignitionLocation, c("Iteration", "FireID")) %>%
  
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
  FireZoneTable <- datasheet(myScenario, "burnP3Plus_FireZone")
  WeatherZoneTable <- datasheet(myScenario, "burnP3Plus_WeatherZone")
    
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
      left_join(DeterministicIgnitionLocation, by = c("Iteration", "FireID")) %>%
      mutate(
        cell = cellFromLatLong(fuelsRaster, Latitude, Longitude),
        FireZone = ifelse(!is.null(fireZoneRaster), fireZoneRaster[][cell] %>% lookup(FireZoneTable$ID, FireZoneTable$Name), ""),
        WeatherZone = ifelse(!is.null(weatherZoneRaster), weatherZoneRaster[][cell] %>% lookup(WeatherZoneTable$ID, WeatherZoneTable$Name), ""),
        FuelType = fuelsRaster[][cell] %>% lookup(FuelType$ID, FuelType$Name)) %>%
      
      # Incorporate Lat and Long and add TimeStep manually
      mutate(Timestep = 0) %>%
    
      # Clean up for saving
      dplyr::select(Iteration, Timestep, FireID, Latitude, Longitude, Season, Cause, FireZone, WeatherZone, FuelType, FireDuration, HoursBurning, Area, ResampleStatus) %>%
      as.data.frame()
      
    # Output if there are records to save
    if(nrow(OutputFireStatistic) > 0)
      saveDatasheet(myScenario, OutputFireStatistic, "burnP3Plus_OutputFireStatistic", append = T)
  
  updateRunLog("Finished collecting fire statistics in ", updateBreakpoint())
}

## Burn maps ----
if(saveBurnMaps) {
  progressBar(type = "message", message = "Saving burn maps...")
  
  # Build table of burn maps and save to SyncroSim
  OutputBurnMap <- 
    tibble(
      FileName = list.files(accumulatorOutputFolder, full.names = T) %>% normalizePath(),
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
          FileName = list.files(seasonalAccumulatorOutputFolder, full.names = T) %>% normalizePath(),
          Iteration = str_extract(FileName, "\\d+-sn") %>% str_sub(end = -4) %>% as.integer(),
          Timestep = 0,
          Season = str_extract(FileName, "\\d+.tif") %>% str_sub(end = -5) %>% as.integer()) %>%
        mutate(
          Season = lookup(Season, SeasonTable$SeasonID, SeasonTable$Name)) %>%
        filter(Iteration %in% iterations)) %>%
      as.data.frame
  }
  
  # Output if there are records to save
  if(nrow(OutputBurnMap) > 0)
    saveDatasheet(myScenario, OutputBurnMap, "burnP3Plus_OutputBurnMap")
  
  updateRunLog("Finished accumulating burn maps in ", updateBreakpoint())
}

## All Perims
if(OutputOptionsSpatial$AllPerim | (saveBurnMaps & minimumFireSize > 0)){
  progressBar(type = "message", message = "Saving individual burn maps...")

  # Build table of burn maps and save to SyncroSim
  OutputAllPerim <- 
    tibble(
      FileName = list.files(allPerimOutputFolder, full.names = T) %>% normalizePath(),
      Iteration = str_extract(FileName, "\\d+_fire") %>% str_sub(end = -6) %>% as.integer(),
      FireID = str_extract(FileName, "\\d+.tif") %>% str_sub(end = -5) %>% as.integer(),
      Timestep = FireID) %>%
    filter(Iteration %in% iterations | (Iteration == 0 & FireID %in% extraIgnitionIDs)) %>%
    as.data.frame
  
  # Output if there are records to save
  if(nrow(OutputAllPerim) > 0)
    saveDatasheet(myScenario, OutputAllPerim, "burnP3Plus_OutputAllPerim")
  
  updateRunLog("Finished individual burn maps in ", updateBreakpoint())
}


## Burn perimeters ----
if(OutputOptionsSpatial$BurnPerimeter) {
  updateRunLog("The FireSTARR transformer currently does not provide burn perimeters.", type = "info")
}

# Clean up
progressBar("end")

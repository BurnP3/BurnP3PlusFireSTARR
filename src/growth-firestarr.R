# Setup ----
Sys.unsetenv("PROJ_LIB")
library(rsyncrosim)

# Find and source shared setup script and function definitions
getSharedDefinitionsPath <- function() {
  sessionPackages <- rsyncrosim::packages(session())
  libraryPackages <- rsyncrosim::packages(ssimLibrary())
  burnP3PlusVersion <- libraryPackages[libraryPackages$name == "burnP3Plus", "version"]
  sharedDefinitionsPath <- paste0(sessionPackages[sessionPackages$name == "burnP3Plus" & sessionPackages$version == burnP3PlusVersion, "location"], "/shared.R")
  return(sharedDefinitionsPath)
}
source(getSharedDefinitionsPath())

## Connect to SyncroSim ----

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

# Import relevant rasters
fuelsRaster <- loadSpatial$fuels()
elevationRaster <- loadSpatial$elevation()

## Validate and parse datasheets ----

# Note: these functions are defined in the shared definitions script
validateAndParseData$FuelType(crosswalk = "burnP3PlusFireSTARR_FuelCodeCrosswalk")
validateAndParseData$Zones()
validateAndParseData$DeterminsiticIgnitions()
validateAndParseData$DeterminsiticBurnConditions()
validateAndParseData$OutputOptions()
validateAndParseData$FBPOutputOptions()
validateAndParseData$BatchOptions()
validateAndParseData$ResampleOptions()
validateAndParseData$FireGrowthOptions()

# Drop list of function for handling missing data from memory to free up some memory
rm(validateAndParseData)

## Report selected but unsupported options ---
if(OutputOptionFBPSpatial %>%
   pull(Variable) %>%
   str_detect("Rate of Spread|Fire Intensity|Direction", negate = T) %>%
   any)
  updateRunLog("FireSTARR currently only produces Rate of Spread, Fire Intensity, and Spread Direction FBP burn maps. Other spatial FBP output options will be ignored.", type = "warning")

if(!isDatasheetEmpty(WindGrid)) {
  updateRunLog("FireSTARR currently does not support Wind Grids. Wind Grid options ignored.", type = "warning")
}

# # Fuel load is currently disabled for all packages
# if(!isDatasheetEmpty(FuelLoad)) {
#   updateRunLog("FireSTARR transformer currently does not support manually specifying Fuel Loading.", type = "warning")
# }


## Set model-specific constants ----

# Names and output file suffixes of FireSTARR-specific secondary outputs
outputComponentNames <- c("RateOfSpread", "FireIntensity", "SpreadDirection")
outputComponentRawSuffix <- c("_ros", "_intensity", "_raz") # Note that intensity outputs just end in "_.tif"
outputComponentCleanSuffix <- c("ros", "fi", "raz")

## Extract relevant parameters ----

# Find SyncroSim proj location
ssim_proj_lib <- ssimEnvironment()$ProgramDirectory %>%
  file.path("gdal", "share") %>%
  normalizePath()


## Determine which fires this job is responsible for ----
# Use the BP3+ heuristic job allocation rather than default SyncroSim allocation
firesToBurn <- splitFiresByJob()

#  Stop with a warning if there's nothing to do
if(nrow(firesToBurn) == 0) {
  updateRunLog("Found no fires to burn for this job! Please ensure your Run Controls are set properly for you Deterministic Inputs", type = "warning")
  # As we are not in a function call, this is the only way to end the transformer early without raising an error
  quit(save = "no", status = 0)
}


## Setup files and folders ----

# Create temp folder structure, ensure it is empty
generateSharedTempFilePaths("growth-firestarr")

# Copy FireSTARR executable
setwd(tempDir)

# Select the appropriate executable for the system OS
if(.Platform$OS.type == "unix") {
  firestarrExecutable <- "./tbd"
  
  # Ensure external dependencies are available
  external_libs <- system2("ldconfig", "-p", stdout = T)
  if(!all(
    any(str_detect(external_libs, "libgeotiff.so.5")),
    any(str_detect(external_libs, "libtiff.so.6")),
    any(str_detect(external_libs, "libproj.so.25"))))
    stop("BurnP3+ FireSTARR on Linux currently requires one or more system dependencies that were not found. Please ensure the following packages or their equivalents for your distribution are installed: `libgeotiff5`, `libtiff6`, `libproj25")
} else {
  firestarrExecutable <- "tbd.exe"
}
firestarrSettings <- "settings.ini"

fsExecutablePath <- file.path(ssimEnvironment()$PackageDirectory, firestarrExecutable)
fsSettingsPath <- file.path(ssimEnvironment()$PackageDirectory, firestarrSettings)

if(!file.exists(fsExecutablePath) | !file.exists(fsSettingsPath))
  stop("Could not find the FireSTARR executable or settings file within the BP3+ FireSTARR package folder. Please reinstall the package.")

file.copy(fsExecutablePath, tempDir, overwrite = T)
file.copy(fsSettingsPath, tempDir, overwrite = T)

# Set as executable if in linux
if(.Platform$OS.type == "unix")
  system2("chmod", c("+x", firestarrExecutable))
  
# Find look up for internal fuel names
InternalFuelNameFile <- file.path(ssimEnvironment()$PackageDirectory, "Internal Fuel Names.csv")

# Set names for model input files
fuelsRasterFile     <- file.path(tempDir, "rasters", "default", "fuel_16_0.tif")
elevationRasterFile <- file.path(tempDir, "rasters", "default", "dem_16_0.tif")
fuelLookupFile      <- file.path(tempDir, "fuel.lut")

## Transformer-specific Function Definitions ----

# Function to pad NA around a raster map out of memory
# - Noticeably slower than `extend` but lower memory footprint
# - This is usually okay since only fuels and elevation need to be extended once per job
extendOnDisk <- function(input_layer, output_extent, filename, ...) {
  # Get layer info and extend an empty copy of input to use as placeholder
  input_extent <- ext(input_layer)
  resolution <- res(input_layer)[1]
  x <- rast(input_layer) %>%
    extend(output_extent)
  
  # Calculate row and col offsets pre- and post-padding
  offset_above <-  (output_extent$ymax - input_extent$ymax) / resolution
  offset_right <-  (output_extent$xmax - input_extent$xmax) / resolution
  offset_left  <- -(output_extent$xmin - input_extent$xmin) / resolution
  
  # The blocks function is not available in the conda version of terra, so we access block info by opening and closing write connnection with an empty raster
  temp <- rast(input_layer)
  blockInfo <- writeStart(temp, filename = "")
  invisible(writeStop(temp))
  rm(temp)

  # Open the output template for writing
  writeStart(x, filename = filename, ...)

  for (i in seq_along(blockInfo$row) ) {
    # read values appears to leak memory until the file is closed, so we start and stop frequently to reduce memory overhead
    readStart(input_layer)

    # Read values and bind an appropriante number of NA columns to either side
    v <- readValues(input_layer, row = blockInfo$row[i], nrows = blockInfo$nrows[i])
    v <- matrix(v, ncol = ncol(input_layer), byrow = T)
    v <- cbind(
      matrix(NA_integer_, nrow = blockInfo$nrows[i], ncol = offset_left),
      v,
      matrix(NA_integer_, nrow = blockInfo$nrows[i], ncol = offset_right))
    v <- as.numeric(t(v))

    # Write values, offsetting by the number of rows padded above
    writeValues(x, v, blockInfo$row[i] + offset_above, nrows = blockInfo$nrows[i])
    readStop(input_layer)

    # Although garbage collection every block is slow, it help reduce memory overhead
    gc()
  }
  
  # Clean up
  writeStop(x)
  gc()
}

# Function to get burn grid paths after a batch of runs
# - This is a list where each element is a vector of output paths
# - Each list element might contain multiple paths if outputs are produced each day to generate daily perimeters
getRawOutputGridPaths <- function() {
  list.dirs(gridOutputFolder)[-1] %>%
    map(~ .x %>% list.files("occurrence_.*tif$", full.names = T)) %>%
    map(~ if (length(.x) == 0) {return(NA)} else return(.x)) # Catch zero length outputs just in case
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

# Function to convert Cell IDs from one extent (raw cropped outputs) to another (full landscape)
convertCellIDs <- function(cellIDs, from_path, to_layer) {
  # Load input layer from path
  from_layer <- rast(from_path) %>%
    # - FireSTARR overwrites CRS but does not reproject outputs, so we reassign here
    set_crs(crs(to_layer))
  
  # Find resolution and extent offsets
  resolution <- res(to_layer)[1] # Assumed to be square and identical for both layers
  x_offset <- (ext(from_layer)$xmin - ext(to_layer)$xmin) / resolution
  y_offset <- (ext(to_layer)$ymax - ext(from_layer)$ymax) / resolution

  # Convert to row col
  rowColFromCell(from_layer, cellIDs) %>%
    # Transpose to leverage R's vectorized addition
    t() %>%
    # Add row and col offsets (in that order)
    `+`(c(y_offset, x_offset)) %>%
    # Convert to Cell IDs using output extent
    {cellFromRowCol(to_layer, row = .[1,], col = .[2,])}
}

# Function to extract the cell IDs of burned pixels in the raw cropped extent (see convertCellIDs() above)
findBurnedCellIDs <- function(layer_filepath, template) {
  # Load layer
  rast(layer_filepath) %>%
    # - FireSTARR overwrites CRS but does not reproject outputs, so we reassign here
    set_crs(crs(template)) %>%
    # Find IDs of burned cells (value = 1)
    cells(y = 1) %>%
    unlist
}

# Function to extract FBP cell data for burned cells when the burned cell IDs are known
# - note that cell IDs are expected in the raw cropped extent, not the full landscape
extractTabularData <- function(layer_filepath, cellIDs, template) {
  # Load layer
  rast(layer_filepath) %>%
    # - FireSTARR overwrites CRS but does not reproject outputs, so we reassign here
    set_crs(crs(template)) %>%
    # Extract values from Cell IDs
    `[`(cellIDs) %>%
    # Return as vector
    pull()
}

readAsVector <- function(layer_path) {
  # Load raster layer
  raster_data <- rast(layer_path)

  # If there is no data, return and empty geom
  # - Note that unburned cells are NA in FireSTARR
  if(unlist(global(raster_data, "notNA")) == 0) {
    layer_data <- fuelsRaster %>%
      ext() %>%
      as.polygons() %>%
      erase(.,.)

  # Otherwise trim and convert raster data
  } else {
    layer_data <- raster_data %>%
      trim() %>% 
      as.polygons()
  }

  # Clean up and return
  layer_data %>%
    set_crs(crs(fuelsRaster)) %>%
    st_as_sf() %>%
    st_buffer(0) %>% # Prevent specific invalidity case that st_make_valid doesn't catch
    st_make_valid() %>%
    st_cast("MULTIPOLYGON") %>%
    return()
}

processOutputsPerFire <- function(BatchID, Iteration, FireID, UniqueFireID, burnGrids, ...) {
  # Return nothing if no maps were produced for this fire
  if(length(burnGrids) < UniqueFireID | all(is.na(burnGrids[[UniqueFireID]])))
    return()
  
  # Load spatial data if present
  # - Note that there might be multiple burn grids per fire if we're generating daily burn vectors. We want last present 
  burnPath <- burnGrids[[UniqueFireID]] %>%
    tail(1)

  # Get table of burned cells
  burnData <- data.table(
    BatchID = BatchID,
    Iteration = Iteration,
    FireID = FireID,
    # Keep cell ID in the original cropped raw output extent to extract FBP data, if present
    rawCellID = findBurnedCellIDs(burnPath, fuelsRaster))
  
  # Also calculate cell ID in final full extent for later
  burnData[, "CellID" := convertCellIDs(rawCellID, burnPath, fuelsRaster)]

  # Add columns of data for any FBP outputs
  for (component in outputComponentsToKeep) {
    # Find the correct secondary output in the same folder as the burn grid
    inputComponentFileName <- burnPath %>%
      dirname() %>%
      list.files(pattern = str_c("^\\d.*", lookup(component, outputComponentNames, outputComponentRawSuffix), ".tif$"), full.names = T) %>%
      tail(1)

    # If it exists, add a corresponding column to burnData
    # - Note that with data.table syntax this does not need to be assigned back to burnData
    if (length(inputComponentFileName) > 0) {
      burnData[, (component) := extractTabularData(inputComponentFileName, rawCellID, fuelsRaster)]
    }
  }

  # Drop the raw Cell ID column now that FBP outputs are handled
  burnData[, rawCellID := NULL]

  # Generate vector outputs if required
  if(OutputOptionsSpatial$BurnPerimeter != "No")
    generateVectorPerimeters(Iteration, FireID, UniqueFireID, burnGrids[[UniqueFireID]])

  # Return raw tabular outputs
  return(burnData)
}

# Function to convert, accumulate, and clean up raw outputs
processOutputs <- function(batchOutputs, rawOutputGridPaths) {
  # Don't save outputs from fires below minimum size
  batchOutputs <- batchOutputs %>%
    filter(ResampleStatus == "Kept" | ResampleStatus == "Extra")
    
  # Generate outputs and collect raw tabular outputs
  batchTabularData <- pmap_dfr(batchOutputs, processOutputsPerFire, burnGrids = rawOutputGridPaths)

  # Save batch outputs to temp dataset
  if(!isDatasheetEmpty(batchTabularData))
    arrow::write_dataset(
      dataset = batchTabularData,
      path = rawTableTempPath,
      format = "arrow",
      partitioning = "BatchID",
      existing_data_behavior = "delete_matching")
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
    c(str_c("\"", outputFolder, "\""),
      ignDate, 
      Latitude, Longitude,
      "13:00", # Mock start time set in weather files
      "--ffmc", FFMC,
      "--dmc", DMC,
      "--dc", DC,
      "--output_date_offsets", str_c("[", str_c(numDays, collapse = ","), "]"),
      "--raster-root rasters",
      "-s --occurrence --no-intensity --no-probability --deterministic",
      if(saveFBPMaps) "-i" else NULL, # The individual burn map flag (-i) is used to generate all secondary outputs
      "-q -q -q -q", # Reduce output level multiple times for silent output
      "--curing", Curing,
      if(GreenUp) "--force-greenup" else "--force-no-greenup",
      "--wx", str_c("\"", weatherFolder, "/weather", UniqueBatchFireIndex, ".csv\""))

  # Log arguemnts used for run
  fwrite(list(c(str_c("./", firestarrExecutable), firestarr_args)), "fs-arguments.log", eol = " ")

  system2(firestarrExecutable, firestarr_args, stdout = FALSE) # For debugging, consider saving stdout to file instead
}

runFireSTARRBatch <- function(ignitionData) {
  resetFolder(gridOutputFolder)

  if(.Platform$OS.type != "unix")
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
  # Generate batch-specific index for fires
  batchInputs <- batchInputs %>%
    mutate(UniqueFireID = row_number())

  # - Unnest and process weather info
  batchWeather <- unnest(batchInputs, data)
  generateWeatherFiles(batchWeather)
  
  # Run FireSTARR on the batch
  runFireSTARRBatch(batchInputs)
  
  # Get relative paths to all raw outputs
  rawOutputGridPaths <- getRawOutputGridPaths()
  
  # Get burn areas
  burnAreas <- getBurnAreas(rawOutputGridPaths)

  # Convert and save spatial outputs as needed
  batchOutputs <- batchInputs %>%
    select(BatchID, UniqueFireID, Iteration, FireID, Season) %>%
    mutate(Area = burnAreas) %>%
    getResampleStatus()
    
  # Save GeoTiffs if needed
  if(saveBurnMaps)
    processOutputs(batchOutputs, rawOutputGridPaths)
  
  # Clear up temp files
  resetFolder(gridOutputFolder)
  
  # Update Progress Bar
  progressBar("step")
  progressBar(type = "message", message = "Growing fires...")
  
  # Return relevant outputs
  batchOutputs %>%
    select(-BatchID, -UniqueFireID, -Season) %>%
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

generateVectorPerimeters <- function(Iteration, FireID, UniqueFireID, burnGrids, ...) {
  # Generate vector outputs if needed
  if(OutputOptionsSpatial$BurnPerimeter == "Final") {
    # Vectorize map from last day of burning
    readAsVector(burnGrids %>% tail(1)) %>%
      mutate(
        Iteration = Iteration,
        FireID = FireID,
        geometry = geometry,
        .keep = "none") %>%

      # Save outputs
      st_write(
        dsn = geopackage_path,
        layer = geopackage_layer_name,
        quiet = TRUE,
        append = TRUE)
  }

  if(OutputOptionsSpatial$BurnPerimeter == "Daily") {
    # For daily perimeters, iterate over daily burn maps to generate burn perimeters
    burn_to_date <- NULL
    for (burnDay in seq_along(burnGrids)) {
      # Update yesterday's burn
      burn_yesterday <- burn_to_date

      # Vectorize current day's grid
      burn_to_date <- readAsVector(burnGrids[burnDay]) %>%
        mutate(
          Iteration = Iteration,
          FireID = FireID,
          BurnDay = burnDay,
          geometry = geometry,
          .keep = "none")
      
      # Subtract previous days burn if not the first day
      if (burnDay == 1) {
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
}

updateRunLog("Finished parsing run inputs in ", updateBreakpoint())

# Prepare Shared Inputs ----

# Spatial data
dir.create(dirname(fuelsRasterFile), showWarnings = F, recursive = T)

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

fuelsRaster %>%
  extendOnDisk(
    padded_extent,
    filename = fuelsRasterFile,
    overwrite = T,
    datatype = "INT2S",
    NAflag = -9999,
    gdal=c("TILED=YES"))
elevationRaster %>%
  extendOnDisk(
    padded_extent,
    filename = elevationRasterFile,
    overwrite = T,
    datatype = "INT2S",
    NAflag = -9999,
    gdal=c("TILED=YES"))

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

# Organize ignition location
ignitionLocation <- DeterministicIgnitionLocation %>%
  dplyr::select("Iteration","FireID","Latitude","Longitude","Season") %>%
  arrange("Iteration", "FireID")

# Combine deterministic input tables ----
fireGrowthInputs <- 
  generateFireGrowthInputs(
    firesToBurn = firesToBurn,
    DeterministicBurnCondition = DeterministicBurnCondition,
    ignitionLocation = ignitionLocation) %>% 

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
        pluck(1))) %>%

  # Group and split for iterating over batches
  group_split(BatchID)

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
  
  # Add extra information to Fire Statistic table
  OutputFireStatistic <- augmentOutputFireStatistic(
    OutputFireStatistic = OutputFireStatistic,
    firesToBurn = firesToBurn,
    DeterministicBurnCondition = DeterministicBurnCondition)
  

    # Output if there are records to save
    if(!isDatasheetEmpty(OutputFireStatistic))
      saveDatasheet(myScenario, OutputFireStatistic, "burnP3Plus_OutputFireStatistic", append = T)
  
  updateRunLog("Finished collecting fire statistics in ", updateBreakpoint())
}

## Tabular burn maps ----
consolidateTabularOutputs()

## Burn perimeters ----
consolidateVectorOutputs()

# Clean up
progressBar("end")

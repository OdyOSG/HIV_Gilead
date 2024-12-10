# A. File Info  -----------------------

# Task: Build Cohorts
# Please refer to HowToRun.md in the documentation for instructions on
# running package

# B. Dependencies ----------------------
# Dependencies are handled by renv package.

## Load libraries and scripts

library(dplyr)
library(DatabaseConnector)
install.packages("renv")
#renv::init()

renv::restore()

source(here::here('analysis/private/_prepareStudy.R'))

# C. Connection ----------------------

## Set connection block
# <<<
configBlock <- "synpuf"
# >>>

## Provide connection details
#jdbc_driver_path <- ""
connectionDetails <- DatabaseConnector::createConnectionDetails (
  dbms = "",
  server = "",
  user = "",
  password = "",
  port = "",
  #pathToDriver = jdbc_driver_path

)

## Connect to database
con <- DatabaseConnector::connect(connectionDetails)


# D. Study Variables -----------------------
outputFolder <- ''    # output folder for csv files
incrementalFolder = "" # folder to keep track of generated cohorts "incremental"
## Load cohorts

executionSettings <- list(
  projectName = tolower(''),
  cohortTable = tolower(''),
  cdmDatabaseSchema = '',
  vocabDatabaseSchema = '',
  workDatabaseSchema = "",
  dbms = "",
  cohortDatabaseSchema = "",
  tablePrefix = "",
  databaseName = ""

)

# E. Script --------------------

# Generate initial csvs based on database specs
createStrataTable(con = con,
                  executionsettings = executionsettings)
# Generate yearly and subcohorts from cohortsToCreate/03

generateSubCohorts(con = con,
                   outputFolder = outputFolder,
                   executionSettings = executionSettings)



# Create Hiv neg table shells
createTableShells1(con = con, outputFolder = outputFolder)

# Create Hiv positive table shells 2 and 4
createTableShells2( con = con,
                    outputFolder = outputFolder,
                    executionSettings = executionSettings)

# F. Disconnect ------------------------

DatabaseConnector::disconnect(connection = con)

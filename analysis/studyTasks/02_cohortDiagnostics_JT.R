# A. File Info -----------------------

# Task: Cohort Diagnostics
# Please refer to HowToRun.md in the documentation for instructions on
# running package

# B. Dependencies ----------------------
# Dependencies are handled by renv package.

## Load libraries and scripts
library(tidyverse, quietly = TRUE)
library(DatabaseConnector)
library(config)
library(CohortDiagnostics)
#install.packages('https://github.com/OHDSI/CohortDiagnostics/archive/refs/tags/v3.2.5.tar.gz')
install.packages('https://github.com/OHDSI/OhdsiShinyModules/archive/refs/tags/v3.1.1.zip')
# May only be needed once.


source(here::here('analysis/private/_buildCohorts.R'))
source(here::here('analysis/private/_executeStudy.R'))
source(here::here('analysis/private/_utilities.R'))


# C. Connection ----------------------

## Set connection Block
# <<<
configBlock <- "synpuf"
# >>>

## Provide connection details
connectionDetails <- DatabaseConnector::createConnectionDetails(
  dbms = "postgresql",
  user = "ohdsi",
  password = "ohdsi",
  server = "testnode.arachnenetwork.com/synpuf_110k",
  port = 5441
)

## Connect to database
con <- DatabaseConnector::connect(connectionDetails)


# D. Study Variables -----------------------

## Administrative Variables
executionSettings <- list(
  projectName = tolower('jmt_hiv_gilead'),
  cohortTable = tolower('jmt'),
  cdmDatabaseSchema = "cdm_531",
  vocabDatabaseSchema = "cdm_531",
  workDatabaseSchema = "jmt_hiv_gilead",
  dbms = "postgresql",
  cohortDatabaseSchema = "jmt_hiv_gilead",
  tablePrefix = "jmt_hiv_gilead",
  databaseName = "synpuf",
  cohortTable = "jmt_hiv_gilead_synpuf"

)

outputFolder <- 'results'

## Add study variables or load from settings
diagCohorts <- getCohortManifest() %>%
  dplyr::filter(type == "target")
diagCohorts <- getCohortManifest()


# E. Script --------------------

## Run cohort diagnostics
runCohortDiagnostics(executionSettings = executionSettings,
                     con = con,
                     cohortManifest = diagCohorts,
                     outputFolder = outputFolder)

createMergedResultsFile('results', sqliteDbPath = "MyCohortDiagnosticsResults.sqlite", overwrite = TRUE)
# F. Disconnect ------------------------
launchDiagnosticsExplorer(
  sqliteDbPath = "MyCohortDiagnosticsResults.sqlite",
  makePublishable = TRUE,
  publishDir = file.path(getwd(), "MyStudyDiagnosticsExplorer"),
  overwritePublishDir = TRUE
)

createCohortExplorerApp(
  connectionDetails = connectionDetails,
  cohortDefinitionId = 1
)


DatabaseConnector::disconnect(con)

# A. File Info -----------------------

# Task: Cohort Diagnostics
# Please refer to HowToRun.md in the documentation for instructions on
# running package

# B. Dependencies ----------------------
# Dependencies are handled by renv package.

## Load libraries and scripts

install.packages('Matrix', type = 'source')

library(tidyverse, quietly = TRUE)
library(DatabaseConnector)
library(config)
library(CohortDiagnostics)
remotes::install_github("OHDSI/MethodEvaluation")
install.packages('https://github.com/OHDSI/CohortDiagnostics/archive/refs/tags/v3.2.5.tar.gz')
install.packages('https://github.com/OHDSI/OhdsiShinyModules/archive/refs/tags/v3.1.1.zip')
install.packages("https://cran.r-project.org/src/contrib/Archive/Matrix/Matrix_1.6-0.tar.gz")
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
connectionDetails <- DatabaseConnector::createConnectionDetails (
  dbms = "redshift",
  server = "gdash-p-usw2-dif-eks-redshift-cluster.ct4xaxb7ww1g.us-west-2.redshift.amazonaws.com/redshiftdb",
  user = "jtelford",
  password = "Pb6Lj2Ut9Kx0",
  port = "5439",
  pathToDriver = jdbc_driver_path

)

## Connect to database
con <- DatabaseConnector::connect(connectionDetails)


# D. Study Variables -----------------------

## Administrative Variables
executionSettings <- list(
  projectName = tolower('jmt_hiv_gilead'),
  cohortTable = tolower('jmt'),
  cdmDatabaseSchema = 'iqvia_ambulatory_emr_omop_20240501',
  vocabDatabaseSchema = 'iqvia_ambulatory_emr_omop_20240501',
  workDatabaseSchema = "sb_jtelford",
  dbms = "postgresql",
  cohortDatabaseSchema = "sb_jtelford",
  tablePrefix = "jmt_hiv_gilead",
  databaseName = "synpuf",
  cohortTable = "jmt_hiv_gilead_marketscan"

)

outputFolder <- 'resultsTime'

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

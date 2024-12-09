# A. File Info  -----------------------

# Task: Build Cohorts
# Please refer to HowToRun.md in the documentation for instructions on
# running package

# B. Dependencies ----------------------
# Dependencies are handled by renv package.

## Load libraries and scripts

library(dplyr)
library(DatabaseConnector)


source(here::here('analysis/private/_prepareStudy.R'))

# C. Connection ----------------------

## Set connection block
# <<<
configBlock <- "synpuf"
# >>>

## Provide connection details
jdbc_driver_path <- "../drivers/"
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
# databaseName: synpuf
# cohortTable: SurgeryWaitTime_synpuf
## Administrative Variables
outputFolder <- 'resultsHealthVerity'
incrementalFolder = "incrementalHIVHealthVerity"
## Load cohorts
# Needed to execute on Postgres, will be moved in final.
executionSettings <- list(
  projectName = tolower('jmt_hiv_gilead_healthV'),
  cohortTable = tolower('jmt_healthV'),
  cdmDatabaseSchema = 'healthverity_marketplace_omop_20221231',
  vocabDatabaseSchema = 'healthverity_marketplace_omop_20221231',
  workDatabaseSchema = "sb_jtelford",
  dbms = "redshift",
  cohortDatabaseSchema = "sb_jtelford",
  tablePrefix = "jmt_hiv_healthverity",
  databaseName = "synpuf"

)

# E. Script --------------------


## Generate cohorts

generateSubCohorts(connectionDetails = connectionDetails,
                   outputFolder = outputFolder,
                   executionSettings = executionSettings)

# Generate initial csvs based on database specs
createStrataTable(connectionDetails = connectionDetails,
                  executionsettings = executionsettings)

# Create Hiv neg table shells
createTableShells1(outputFolder = outputFolder)

# Create Hiv positive table shells
createTableShells2( connectionDetails = connectionDetails,
                    outputFolder = outputFolder,
                    executionSettings = executionSettings)



# F. Disconnect ------------------------

DatabaseConnector::disconnect(connection = con)
con <- DatabaseConnector::connect(connectionDetails)

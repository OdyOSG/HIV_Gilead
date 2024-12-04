# A. File Info -----------------------

# Study:
# Task: Build Stratas


# B. Dependencies ----------------------

## Load libraries and scripts
library(tidyverse, quietly = TRUE)
source("analysis/private/_utilities.R")
source("analysis/private/_buildStrata.R")


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
outputFolder <- 'results'

# D. Variables -----------------------

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
## Analysis Settings
analysisSettings <- readSettingsFile(here::here("analysis/settings/strata.yml"))


# E. Script --------------------

#startSnowflakeSession(con =con, executionSettings = executionSettings)

## Build stratas

buildStrata(con = con,
            executionSettings = executionSettings,
            analysisSettings = analysisSettings)


# F. Session Info ------------------------

DatabaseConnector::disconnect(con)

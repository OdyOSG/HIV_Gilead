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
#install.packages('https://github.com/OHDSI/CohortGenerator/archive/refs/tags/v0.8.1.tar.gz')
#install.packages('https://github.com/OHDSI/CirceR/archive/refs/tags/v1.3.3.tar.gz')
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
outputFolder <- 'results'
incrementalFolder = "incrementalHIV"
## Load cohorts
# Needed to execute on Postgres, will be moved in final.
executionSettings <- list(
  projectName = tolower('jmt_hiv_gilead'),
  cohortTable = tolower('jmt'),
  cdmDatabaseSchema = 'iqvia_ambulatory_emr_omop_20240501',
  vocabDatabaseSchema = 'iqvia_ambulatory_emr_omop_20240501',
  workDatabaseSchema = "sb_jtelford",
  dbms = "redshift",
  cohortDatabaseSchema = "sb_jtelford",
  tablePrefix = "jmt_hiv_gilead",
  databaseName = "synpuf"

)

# E. Script --------------------


## Generate cohorts

generateSubCohorts(con = con,
                   outputFolder = outputFolder,
                   executionSettings = executionSettings)



# F. Disconnect ------------------------

DatabaseConnector::disconnect(connection = con)

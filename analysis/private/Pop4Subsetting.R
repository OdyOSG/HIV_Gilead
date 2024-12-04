# Script for only Pop4 subsetting
library(CohortGenerator)
library(dplyr)
source(here::here('analysis/private/_buildCohorts.R'))
source(here::here('analysis/private/_executeStudy.R'))
source(here::here('analysis/private/_utilities.R'))
jdbc_driver_path <- "../drivers/"
connectionDetails <- DatabaseConnector::createConnectionDetails (
  dbms = "redshift",
  server = "gdash-p-usw2-dif-eks-redshift-cluster.ct4xaxb7ww1g.us-west-2.redshift.amazonaws.com/redshiftdb",
  user = "jtelford",
  password = "Pb6Lj2Ut9Kx0",
  port = "5439",
  pathToDriver = jdbc_driver_path

)
outputFolder <- 'results'
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

## Connect to database
con <- DatabaseConnector::connect(connectionDetails)
# Generate yearly cohorts
directory = "03_basecohorts"
files_with_pop4 <- list.files(
  path = paste0("cohortsToCreate/",directory),
  pattern = "Pop4",
  full.names = FALSE
)
# file = files_with_pop4[1]
# dt = 2016
# dates <- 2016:2023

for (file in files_with_pop4){
  cohortExpr =  jsonlite::read_json(paste0("cohortsToCreate/",directory,'/',file))
  # for (dt in dates){
  #
  #   print(paste0(file," ", dt))
  #   cohortExpr$PrimaryCriteria$CriteriaList[[1]]$DrugExposure$OccurrenceStartDate$Value <- paste(dt, "01", "01", sep = "-")
  #   cohortExpr$PrimaryCriteria$CriteriaList[[1]]$DrugExposure$OccurrenceStartDate$Extent <- paste(dt, "12", "31", sep = "-")
  shortname =  substr(file, 1, nchar(file) - 5)
    newpath = paste0("cohortsToCreate/07_Pop4_cohorts/",shortname,'.json')
    write(
      RJSONIO::toJSON(cohortExpr, indent = 2, digits = 10),
      file.path(
        newpath)
    )
  #   # Add Female Cohort
  #   cohortExpr$InclusionRules[[4]]$name = "Female"
  #   cohortExpr$InclusionRules[[4]]$description = "Female"
  #   cohortExpr$InclusionRules[[4]]$expression$DemographicCriteriaList[[1]]$Gender[[1]]$CONCEPT_CODE = "F"
  #   cohortExpr$InclusionRules[[4]]$expression$DemographicCriteriaList[[1]]$Gender[[1]]$CONCEPT_NAME = "Female"
  #   cohortExpr$InclusionRules[[4]]$expression$DemographicCriteriaList[[1]]$Gender[[1]]$CONCEPT_ID = "8507"
  shortname =  substr(file, 1, nchar(file) - 6)
  newpath = paste0("cohortsToCreate/07_Pop4_cohorts/",shortname,"F",'.json')
    write(
      RJSONIO::toJSON(cohortExpr, indent = 2, digits = 10),
      file.path(
        newpath)
    )

 # }
}

#get only pop4 cohorts
cohortManifest <- getCohortManifest(inputPath = "cohortsToCreate/07_Pop4_cohorts")
#cohortManifest <- cohortManifest[grep("Pop3Template2or3M201", cohortManifest$name), ]
cohortManifest <- cohortManifest[grep("Pop4", cohortManifest$name), ]
cohortDefinitionSet = prepManifestForCohortGenerator(cohortManifest)
cohortDefinitionSet$cohortId = cohortDefinitionSet$cohortId +4000


subsetDef1 <- createCohortSubsetDefinition(
  name = "Patients in cohort 4",
  definitionId = 1,
  subsetOperators =list(createDemographicSubset(
    name = "Age 15 - 29",
    ageMin = 15,
    ageMax = 29
  ))
)
subsetDef2 <- createCohortSubsetDefinition(
  name = "Patients in cohort 4",
  definitionId = 2,
  subsetOperators =list(createDemographicSubset(
    name = "Age 30 - 44",
    ageMin = 30,
    ageMax = 44
  ))
)
subsetDef3 <- createCohortSubsetDefinition(
  name = "Patients in cohort 4",
  definitionId = 3,
  subsetOperators =list(createDemographicSubset(
    ageMin = 45,
    ageMax = 55
  ))
)
subsetDef4 <- createCohortSubsetDefinition(
  name = "Patients in cohort 4",
  definitionId = 4,
  subsetOperators =list(createDemographicSubset(
    name = "Age 55+",
    ageMin = 56
  ))
)
# Add age sets
cohortDefinitionSet <- cohortDefinitionSet |>
  addCohortSubsetDefinition(subsetDef1) |>
  addCohortSubsetDefinition(subsetDef2) |>
  addCohortSubsetDefinition(subsetDef3) |>
  addCohortSubsetDefinition(subsetDef4)

racestratafile <- read.csv("analysis/settings/racestrata.csv")
for (x in 1:nrow(racestratafile)) {
  racestrata <- racestratafile[x, ]

subsetDef <- createCohortSubsetDefinition(
  name = paste0("Patients in cohort 1 that are "),
  definitionId = (x+4),
  subsetOperators =list(createDemographicSubset(
    name = racestrata$race,
    race = racestrata$code
  ))
)


  cohortDefinitionSet <- cohortDefinitionSet |>
     addCohortSubsetDefinition(subsetDef)
}

initializeCohortTables(executionSettings = executionSettings, con = con, dropTables = FALSE)
generatedCohorts <- generateCohortSet(
  connectionDetails = connectionDetails,
  cdmDatabaseSchema = 'iqvia_ambulatory_emr_omop_20240501',
  cohortDatabaseSchema = "sb_jtelford",
  cohortTableNames = getCohortTableNames("jmt"),
  cohortDefinitionSet = cohortDefinitionSet,
  incremental = TRUE,
  incrementalFolder = file.path(outputFolder, "RecordKeeping")
)

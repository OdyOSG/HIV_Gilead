# Take original Population 1 cohort and convert to yearly

directory = "03_basecohorts"
files_with_pop1 <- list.files(
  path = paste0("cohortsToCreate/",directory),
  pattern = "Pop1",
  full.names = FALSE
)

dates <- 2016:2023
file = files_with_pop1[1]
for (file in files_with_pop1){
  cohortExpr =  jsonlite::read_json(paste0("cohortsToCreate/",directory,'/',file))
  for (dt in dates){

    print(paste0(file," ", dt))
    cohortExpr$PrimaryCriteria$CriteriaList[[1]]$DrugExposure$OccurrenceStartDate$Value <- paste(dt, "01", "01", sep = "-")
    cohortExpr$PrimaryCriteria$CriteriaList[[1]]$DrugExposure$OccurrenceStartDate$Extent <- paste(dt, "12", "31", sep = "-")
    shortname =  substr(file, 1, nchar(file) - 5)
    newpath = paste0("cohortsToCreate/04_yearlycohorts/",shortname,dt,'.json')
    write(
      RJSONIO::toJSON(cohortExpr, indent = 2, digits = 1000),
      file.path(
        newpath)
    )

  }
}

# Stratify by age
agestratafile <- read_csv("analysis/settings/agestrata.csv")
directory = "04_yearlycohorts"
path = paste0("cohortsToCreate/",directory)
yearlyfiles_with_pop1 <- list.files(
  path = paste0("cohortsToCreate/",directory),
  pattern = "Pop1",
  full.names = FALSE
)
yearlyfiles_with_pop1 <- yearlyfiles_with_pop1[!grepl("Pop1R", yearlyfiles_with_pop1)]

for (file in yearlyfiles_with_pop1){
  cohortExpr =  jsonlite::read_json(paste0(path,'/',file))
   for (x in 1:nrow(agestratafile)) {
       agestrata <- agestratafile[x, ]

    if(agestrata$type == 'bt'){
      cohortExpr$InclusionRules[[2]]$name = agestrata$name
      cohortExpr$InclusionRules[[2]]$expression$DemographicCriteriaLis[[1]]$Age$Op = agestrata$type
      cohortExpr$InclusionRules[[2]]$expression$DemographicCriteriaList[[1]]$Age$Value = agestrata$lower
      cohortExpr$InclusionRules[[2]]$expression$DemographicCriteriaList[[1]]$Age$Extent = agestrata$upper
    }
    if(agestrata$type == 'gt'){
      cohortExpr$InclusionRules[[2]]$name = agestrata$name
      cohortExpr$InclusionRules[[2]]$expression$DemographicCriteriaLis[[1]]$Age$Op = agestrata$type
      cohortExpr$InclusionRules[[2]]$expression$DemographicCriteriaList[[1]]$Age$Value = agestrata$lower
      cohortExpr$InclusionRules[[2]]$expression$DemographicCriteriaList[[1]]$Age$Extent <- NULL
    }

    # Save the updated cohort JSON
    shortname =  substr(file, 1, nchar(file) - 5)
    stratapath = paste0("cohortsToCreate/05_ageracecohorts/pop1/",shortname,"_",agestrata$name,"_strata.json")
    print(stratapath)
    write(
      RJSONIO::toJSON(cohortExpr, indent = 2, digits = 10),
      file.path(
        stratapath)
    )
     }
}


# Add race subcohorts

racestratafile <- read_csv("analysis/settings/racestrata.csv")
directory = "04_yearlycohorts"
path = paste0("cohortsToCreate/",directory)
yearlyfiles_with_pop1 <- list.files(
  path = paste0("cohortsToCreate/",directory),
  pattern = "Pop1R",
  full.names = FALSE
)
for (file in yearlyfiles_with_pop1){
  cohortExpr =  jsonlite::read_json(paste0(path,'/',file))
  # loop through races
  for (x in 1:nrow(racestratafile)) {
    racestrata <- racestratafile[x, ]
  newcohortExpr = cohortExpr
   newcohortExpr$InclusionRules[[5]]$name = racestrata$race
   newcohortExpr$InclusionRules[[5]]$expression$DemographicCriteriaList[[1]]$Race[[1]]$CONCEPT_NAME = racestrata$race
   newcohortExpr$InclusionRules[[5]]$expression$DemographicCriteriaList[[1]]$Race[[1]]$CONCEPT_ID = racestrata$code



  shortname =  substr(file, 1, nchar(file) - 5)
  stratapath = paste0("cohortsToCreate/05_ageracecohorts/pop1/",shortname,"_",racestrata$race,"_strata.json")
  print(stratapath)
   write(
     RJSONIO::toJSON(newcohortExpr, indent = 2, digits = 10),
     file.path(
       stratapath)
  )
  }
}




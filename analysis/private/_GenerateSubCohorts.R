# Script for automated cohort generation and subsetting


generateSubCohorts <- function(connectionDetails = connectionDetails, outputFolder = outputFolder, executionSettings = executionSettings){

  con <- DatabaseConnector::connect(connectionDetails)
    library(CohortGenerator)
    if (!dir.exists(outputFolder)) {
      dir.create(outputFolder)
      message("Directory '", outputFolder, "' created.")
    } else {
      message("Directory '", outputFolder, "' already exists.")
    }
    cohortDatabaseSchema = executionSettings$cohortDatabaseSchema
    cohortTable     = executionSettings$cohortTable
    dates <- 2016:2023
    ### Above will be passed into function

    # Generate yearly cohorts
    directory = "03_basecohorts"
    files_with_pop1 <- list.files(
      path = paste0("cohortsToCreate/",directory),
      pattern = "Pop1",
      full.names = FALSE
    )


    # Change dates to 2016:2023 for final run

    for (file in files_with_pop1){
      cohortExpr =  jsonlite::read_json(paste0("cohortsToCreate/",directory,'/',file))
      cohortExpr$InclusionRules[[1]]$expression$CriteriaList[[1]]$StartWindow$End$Days = 7
      for (dt in dates){

        print(paste0(file," ", dt))

        cohortExpr$PrimaryCriteria$CriteriaList[[1]]$DrugExposure$OccurrenceStartDate$Value <- paste(dt, "01", "01", sep = "-")
        cohortExpr$PrimaryCriteria$CriteriaList[[1]]$DrugExposure$OccurrenceStartDate$Extent <- paste(dt, "12", "31", sep = "-")

        shortname =  substr(file, 1, nchar(file) - 5)
        newpath = paste0("cohortsToCreate/04_Pop1_cohorts/",shortname,dt,'.json')
        write(
          RJSONIO::toJSON(cohortExpr, indent = 2, digits = 10),
          file.path(
            newpath)
        )

      }
    }

    #get only pop1 cohorts
    cohortManifest <- getCohortManifest(inputPath = "cohortsToCreate/04_Pop1_cohorts")
    cohortManifest <- cohortManifest[grep("Pop1", cohortManifest$name), ]
    cohortDefinitionSet = prepManifestForCohortGenerator(cohortManifest)
    cohortDefinitionSet$cohortId = cohortDefinitionSet$cohortId +1000





    genders = c('male','female')

    offset=0
    for (gender in genders) {
    if (gender == 'female') offset = 10
    subsetDef1 <- createCohortSubsetDefinition(
      name = paste0(gender," patients in cohort 1"),
      definitionId = 1 + offset,
      subsetOperators =list(createDemographicSubset(
        name = "Age 15 - 29",
        ageMin = 15,
        ageMax = 29,
        gender = gender
      ))
    )
    subsetDef2 <- createCohortSubsetDefinition(
      name = paste0(gender," patients in cohort 1"),
      definitionId = 2 + offset,
      subsetOperators =list(createDemographicSubset(
        name = "Age 30 - 44",
        ageMin = 30,
        ageMax = 44,
        gender = gender
      ))
    )
    subsetDef3 <- createCohortSubsetDefinition(
      name = paste0(gender," patients in cohort 1"),
      definitionId = 3 + offset,
      subsetOperators =list(createDemographicSubset(
        name = "Age 45 - 55",
        ageMin = 45,
        ageMax = 55,
        gender = gender
      ))
    )
    subsetDef4 <- createCohortSubsetDefinition(
      name = paste0(gender," patients in cohort 1"),
      definitionId = 4 + offset,
      subsetOperators =list(createDemographicSubset(
        name = "Age 55+",
        ageMin = 56,
        gender = gender
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
      name = paste0(gender," patients in cohort 1"),
      definitionId = (x+4 + offset ),
      subsetOperators =list(createDemographicSubset(
        name = racestrata$race,
        race = racestrata$code,
        gender = gender
      ))
    )


      cohortDefinitionSet <- cohortDefinitionSet |>
         addCohortSubsetDefinition(subsetDef)
    }
    }
    cohortDefinitionSet1 = cohortDefinitionSet
    # Generate cohortdefset2

    directory = "03_basecohorts"
    files_with_pop2 <- list.files(
      path = paste0("cohortsToCreate/",directory),
      pattern = "Pop2",
      full.names = FALSE
    )



    for (file in files_with_pop2){
      cohortExpr =  jsonlite::read_json(paste0("cohortsToCreate/",directory,'/',file))
      for (dt in dates){

        print(paste0(file," ", dt))
        cohortExpr$PrimaryCriteria$CriteriaList[[1]]$DrugExposure$OccurrenceStartDate$Value <- paste(dt, "01", "01", sep = "-")
        cohortExpr$PrimaryCriteria$CriteriaList[[1]]$DrugExposure$OccurrenceStartDate$Extent <- paste(dt, "12", "31", sep = "-")
        cohortExpr$PrimaryCriteria$CriteriaList[[1]]$DrugExposure$OccurrenceStartDate$Op <- 'bt'
        shortname =  substr(file, 1, nchar(file) - 5)
        newpath = paste0("cohortsToCreate/05_Pop2_cohorts/",shortname,dt,'.json')
        write(
          RJSONIO::toJSON(cohortExpr, indent = 2, digits = 10),
          file.path(
            newpath)
        )

      }
    }

    cohortManifest <- getCohortManifest(inputPath = "cohortsToCreate/05_Pop2_cohorts")
    cohortManifest <- cohortManifest[grep("Pop2", cohortManifest$name), ]
    cohortDefinitionSet = prepManifestForCohortGenerator(cohortManifest)
    cohortDefinitionSet$cohortId = cohortDefinitionSet$cohortId +2000

    genders = c('male','female')

    offset=0
    for (gender in genders) {
      if (gender == 'female') offset = 10
      subsetDef1 <- createCohortSubsetDefinition(
        name = paste0(gender," patients in cohort 2"),
        definitionId = 1 + offset,
        subsetOperators =list(createDemographicSubset(
          name = "Age 15 - 29",
          ageMin = 15,
          ageMax = 29,
          gender = gender
        ))
      )
      subsetDef2 <- createCohortSubsetDefinition(
        name = paste0(gender," patients in cohort 2"),
        definitionId = 2 + offset,
        subsetOperators =list(createDemographicSubset(
          name = "Age 30 - 44",
          ageMin = 30,
          ageMax = 44,
          gender = gender
        ))
      )
      subsetDef3 <- createCohortSubsetDefinition(
        name = paste0(gender," patients in cohort 2"),
        definitionId = 3 + offset,
        subsetOperators =list(createDemographicSubset(
          name = "Age 45 - 55",
          ageMin = 45,
          ageMax = 55,
          gender = gender
        ))
      )
      subsetDef4 <- createCohortSubsetDefinition(
        name = paste0(gender," patients in cohort 2"),
        definitionId = 4 + offset,
        subsetOperators =list(createDemographicSubset(
          name = "Age 55+",
          ageMin = 56,
          gender = gender
        ))
      )
      # Add age sets
      cohortDefinitionSet <- cohortDefinitionSet |>
        addCohortSubsetDefinition(subsetDef1) |>
        addCohortSubsetDefinition(subsetDef2) |>
        addCohortSubsetDefinition(subsetDef3) |>
        addCohortSubsetDefinition(subsetDef4)

      racestratafile <- read.csv("analysis/settings/racestrata.csv")
      racestratafile <- read.csv("analysis/settings/racestrata.csv")
      for (x in 1:nrow(racestratafile)) {
        racestrata <- racestratafile[x, ]

          subsetDef <- createCohortSubsetDefinition(
            name = paste0(gender," patients in cohort 2"),
            definitionId = (x+4 + offset ),
            subsetOperators =list(createDemographicSubset(
              name = racestrata$race,
              race = racestrata$code,
              gender = gender
            ))
          )


        cohortDefinitionSet <- cohortDefinitionSet |>
          addCohortSubsetDefinition(subsetDef)
      }
    }
    cohortDefinitionSet2 = cohortDefinitionSet

    # Generate cohortdefset3

    directory = "03_basecohorts"
    files_with_pop3 <- list.files(
      path = paste0("cohortsToCreate/",directory),
      pattern = "Pop3",
      full.names = FALSE
    )

    for (file in files_with_pop3){
      cohortExpr =  jsonlite::read_json(paste0("cohortsToCreate/",directory,'/',file))
      cohortExpr$InclusionRules[[1]]$expression$CriteriaList[[1]]$StartWindow$End$Days = 7
      for (dt in dates){

        print(paste0(file," ", dt))
        cohortExpr$PrimaryCriteria$CriteriaList[[1]]$DrugExposure$OccurrenceStartDate$Value <- paste(dt, "01", "01", sep = "-")
        cohortExpr$PrimaryCriteria$CriteriaList[[1]]$DrugExposure$OccurrenceStartDate$Extent <- paste(dt, "12", "31", sep = "-")
        shortname =  substr(file, 1, nchar(file) - 5)
        newpath = paste0("cohortsToCreate/06_Pop3_cohorts/",shortname,dt,'.json')
        write(
          RJSONIO::toJSON(cohortExpr, indent = 2, digits = 10),
          file.path(
            newpath)
        )



      }
    }
    #get only pop3 High Risk cohorts
    cohortManifest <- getCohortManifest(inputPath = "cohortsToCreate/06_Pop3_cohorts")
    #cohortManifest <- cohortManifest[grep("Pop3Template2or3M201", cohortManifest$name), ]
    #cohortManifest <- cohortManifest[!grepl("Pop3Template20", cohortManifest$name), ]
    cohortDefinitionSet3a = prepManifestForCohortGenerator(cohortManifest)
    cohortDefinitionSet3a$cohortId = cohortDefinitionSet3a$cohortId +3000
    subsetDefm <- createCohortSubsetDefinition(
      name = paste0("male patients in cohort 3"),
      definitionId = 20 + offset,
      subsetOperators =list(createDemographicSubset(
        gender = 'male'
      ))
    )
    subsetDeff <- createCohortSubsetDefinition(
      name = paste0("female patients in cohort 3"),
      definitionId = 30 + offset,
      subsetOperators =list(createDemographicSubset(
        gender = 'female'
      ))
    )

    cohortDefinitionSet3a <- cohortDefinitionSet3a |>
      addCohortSubsetDefinition(subsetDefm) |>
      addCohortSubsetDefinition(subsetDeff)
    cohortDefinitionSet3a1list = cohortDefinitionSet3a[,1:2]
    #get only pop3 yearly cohorts for subsetting
    cohortManifest <- getCohortManifest(inputPath = "cohortsToCreate/06_Pop3_cohorts")
    #cohortManifest <- cohortManifest[grep("Pop3Template2or3M201", cohortManifest$name), ]
    cohortManifest <- cohortManifest[grep("Pop3Template20", cohortManifest$name), ]
    cohortDefinitionSet = prepManifestForCohortGenerator(cohortManifest)
    cohortDefinitionSet$cohortId = cohortDefinitionSet$cohortId +3000


    genders = c('male','female')

    offset=0
    for (gender in genders) {
      if (gender == 'female') offset = 10
      subsetDef1 <- createCohortSubsetDefinition(
        name = paste0(gender," patients in cohort 3"),
        definitionId = 1 + offset,
        subsetOperators =list(createDemographicSubset(
          name = "Age 15 - 29",
          ageMin = 15,
          ageMax = 29,
          gender = gender
        ))
      )
      subsetDef2 <- createCohortSubsetDefinition(
        name = paste0(gender," patients in cohort 3"),
        definitionId = 2 + offset,
        subsetOperators =list(createDemographicSubset(
          name = "Age 30 - 44",
          ageMin = 30,
          ageMax = 44,
          gender = gender
        ))
      )
      subsetDef3 <- createCohortSubsetDefinition(
        name = paste0(gender," patients in cohort 3"),
        definitionId = 3 + offset,
        subsetOperators =list(createDemographicSubset(
          name = "Age 45 - 55",
          ageMin = 45,
          ageMax = 55,
          gender = gender
        ))
      )
      subsetDef4 <- createCohortSubsetDefinition(
        name = paste0(gender," patients in cohort 3"),
        definitionId = 4 + offset,
        subsetOperators =list(createDemographicSubset(
          name = "Age 55+",
          ageMin = 56,
          gender = gender
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
            name = paste0(gender," patients in cohort 3"),
            definitionId = (x+4 + offset ),
            subsetOperators =list(createDemographicSubset(
              name = racestrata$race,
              race = racestrata$code,
              gender = gender
            ))
          )


        cohortDefinitionSet <- cohortDefinitionSet |>
          addCohortSubsetDefinition(subsetDef)
      }
    }






    cohortDefinitionSet3b = cohortDefinitionSet

    # Generate cohortdefset4

    directory = "03_basecohorts"
    files_with_pop4 <- list.files(
      path = paste0("cohortsToCreate/",directory),
      pattern = "Pop4",
      full.names = FALSE
    )

    for (file in files_with_pop4){
      cohortExpr =  jsonlite::read_json(paste0("cohortsToCreate/",directory,'/',file))
      for (dt in dates){

        print(paste0(file," ", dt))
        cohortExpr$PrimaryCriteria$CriteriaList[[1]]$DrugExposure$OccurrenceStartDate$Value <- paste(dt, "01", "01", sep = "-")
        cohortExpr$PrimaryCriteria$CriteriaList[[1]]$DrugExposure$OccurrenceStartDate$Extent <- paste(dt, "12", "31", sep = "-")
        shortname =  substr(file, 1, nchar(file) - 5)
        newpath = paste0("cohortsToCreate/07_Pop4_cohorts/",shortname,dt,'.json')
        write(
          RJSONIO::toJSON(cohortExpr, indent = 2, digits = 10),
          file.path(
            newpath)
        )



      }
    }
    cohortManifest <- getCohortManifest(inputPath = "cohortsToCreate/07_Pop4_cohorts")
    #cohortManifest <- cohortManifest[grep("Pop3Template2or3M201", cohortManifest$name), ]


    #cohortManifest <- cohortManifest[!grepl("Pop4Template20", cohortManifest$name), ]

    cohortDefinitionSet4a = prepManifestForCohortGenerator(cohortManifest)
    cohortDefinitionSet4a$cohortId = cohortDefinitionSet4a$cohortId +4000
    subsetDefm <- createCohortSubsetDefinition(
      name = paste0("male patients in cohort 4"),
      definitionId = 20 + offset,
      subsetOperators =list(createDemographicSubset(
        gender = 'male'
      ))
    )
    subsetDeff <- createCohortSubsetDefinition(
      name = paste0("female patients in cohort 4"),
      definitionId = 30 + offset,
      subsetOperators =list(createDemographicSubset(
        gender = 'female'
      ))
    )

    cohortDefinitionSet4a <- cohortDefinitionSet4a |>
      addCohortSubsetDefinition(subsetDefm) |>
      addCohortSubsetDefinition(subsetDeff)
    cohortDefinitionSet4alist = cohortDefinitionSet4a[,1:2]

    #get only pop4 yearly cohorts for subsetting
    cohortManifest <- getCohortManifest(inputPath = "cohortsToCreate/07_Pop4_cohorts")
    #cohortManifest <- cohortManifest[grep("Pop3Template2or3M201", cohortManifest$name), ]
    cohortManifest <- cohortManifest[grep("Pop4Template20", cohortManifest$name), ]
    cohortDefinitionSet = prepManifestForCohortGenerator(cohortManifest)
    cohortDefinitionSet$cohortId = cohortDefinitionSet$cohortId +4000


    genders = c('male','female')

    offset=0
    for (gender in genders) {
      if (gender == 'female') offset = 10
      subsetDef1 <- createCohortSubsetDefinition(
        name = paste0(gender," patients in cohort 4"),
        definitionId = 1 + offset,
        subsetOperators =list(createDemographicSubset(
          name = "Age 15 - 29",
          ageMin = 15,
          ageMax = 29,
          gender = gender
        ))
      )
      subsetDef2 <- createCohortSubsetDefinition(
        name = paste0(gender," patients in cohort 4"),
        definitionId = 2 + offset,
        subsetOperators =list(createDemographicSubset(
          name = "Age 30 - 44",
          ageMin = 30,
          ageMax = 44,
          gender = gender
        ))
      )
      subsetDef3 <- createCohortSubsetDefinition(
        name = paste0(gender," patients in cohort 4"),
        definitionId = 3 + offset,
        subsetOperators =list(createDemographicSubset(
          name = "Age 45 - 55",
          ageMin = 45,
          ageMax = 55,
          gender = gender
        ))
      )
      subsetDef4 <- createCohortSubsetDefinition(
        name = paste0(gender," patients in cohort 4"),
        definitionId = 4 + offset,
        subsetOperators =list(createDemographicSubset(
          name = "Age 55+",
          ageMin = 56,
          gender = gender
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
            name = paste0(gender," patients in cohort 4"),
            definitionId = (x+4 + offset ),
            subsetOperators =list(createDemographicSubset(
              name = racestrata$race,
              race = racestrata$code,
              gender = gender
            ))
          )



        cohortDefinitionSet <- cohortDefinitionSet |>
          addCohortSubsetDefinition(subsetDef)
      }
    }
    cohortDefinitionSet4b = cohortDefinitionSet

    # Generate pop 5 HIV positive
    directory = "03_basecohorts"
    files_with_pop5 <- list.files(
      path = paste0("cohortsToCreate/",directory),
      pattern = "Pop5",
      full.names = FALSE
    )


    # Change dates to 2016:2023 for final run

    for (file in files_with_pop5){
      cohortExpr =  jsonlite::read_json(paste0("cohortsToCreate/",directory,'/',file))
      for (dt in dates){
        print(paste0(file," ", dt))
        cohortExpr$PrimaryCriteria$CriteriaList[[1]]$ConditionOccurrence$OccurrenceStartDate$Value <- paste(dt, "01", "01", sep = "-")
        cohortExpr$PrimaryCriteria$CriteriaList[[1]]$ConditionOccurrence$OccurrenceStartDate$Extent <- paste(dt, "12", "31", sep = "-")
        cohortExpr$PrimaryCriteria$CriteriaList[[2]]$Measurement$OccurrenceStartDate$Value <- paste(dt, "01", "01", sep = "-")
        cohortExpr$PrimaryCriteria$CriteriaList[[2]]$Measurement$OccurrenceStartDate$Extent <- paste(dt, "12", "31", sep = "-")
        cohortExpr$PrimaryCriteria$CriteriaList[[3]]$Measurement$OccurrenceStartDate$Value <- paste(dt, "01", "01", sep = "-")
        cohortExpr$PrimaryCriteria$CriteriaList[[3]]$Measurement$OccurrenceStartDate$Extent <- paste(dt, "12", "31", sep = "-")
        shortname =  substr(file, 1, nchar(file) - 5)
        newpath = paste0("cohortsToCreate/08_Pop5_cohorts/",shortname,dt,'.json')
        write(
          RJSONIO::toJSON(cohortExpr, indent = 2, digits = 10),
          file.path(
            newpath)
        )

      }
    }

    #get only pop5 cohorts
    cohortManifest <- getCohortManifest(inputPath = "cohortsToCreate/08_Pop5_cohorts")
    cohortManifest <- cohortManifest[grep("Pop5", cohortManifest$name), ]
    cohortDefinitionSet = prepManifestForCohortGenerator(cohortManifest)
    cohortDefinitionSet$cohortId = cohortDefinitionSet$cohortId +5000
    cohortDefinitionSet5 = cohortDefinitionSet










    # Run everything
      cohortDefinitionSetCombined <- bind_rows(cohortDefinitionSet1,
                                               cohortDefinitionSet2,
                                               cohortDefinitionSet3a,
                                               cohortDefinitionSet3b,
                                               cohortDefinitionSet4a,
                                               cohortDefinitionSet4b,
                                               cohortDefinitionSet5)
      cohortDefinitionSetList = cohortDefinitionSetCombined[,1:2]
    write.csv(cohortDefinitionSetList,paste0(outputFolder,"/cohortDefinitionSetList.csv"), row.names = FALSE)
    initializeCohortTables(executionSettings = executionSettings, con = con, dropTables = FALSE)
    DatabaseConnector::disconnect(connection = con)
    con <- DatabaseConnector::connect(connectionDetails)

    definitionlist = c("cohortDefinitionSet1",
                         "cohortDefinitionSet2",
                         "cohortDefinitionSet3a",
                         "cohortDefinitionSet3b",
                         "cohortDefinitionSet4a",
                         "cohortDefinitionSet4b",
                         "cohortDefinitionSet5")


    name <- executionSettings$cohortTable
    cohortTableNames <- list(cohortTable = paste0(name),
                             cohortInclusionTable = paste0(name, "_inclusion"),
                             cohortInclusionResultTable = paste0(name, "_inclusion_result"),
                             cohortInclusionStatsTable = paste0(name, "_inclusion_stats"),
                             cohortSummaryStatsTable = paste0(name, "_summary_stats"),
                             cohortCensorStatsTable = paste0(name, "_censor_stats"))

    cohortsetname = definitionlist[1]
    for (cohortsetname in definitionlist){
        cohortdefsettouse = get(cohortsetname)
        cohortdefsettouse <- cohortdefsettouse[grepl("2016", cohortdefsettouse$cohortName) | grepl("2017", cohortdefsettouse$cohortName), ]
        CohortGenerator::generateCohortSet(
          connection = con,
          cdmDatabaseSchema = executionSettings$cdmDatabaseSchema,
          cohortDatabaseSchema =  executionSettings$workDatabaseSchema,
          cohortTableNames = cohortTableNames,
          cohortDefinitionSet = cohortdefsettouse,
          incremental = TRUE,
          incrementalFolder = incrementalFolder
        )
    }

    # Collect data on cohort counts
    query <- "SELECT DISTINCT cohort_definition_id FROM sb_jtelford.jmt;"
    cohort_ids <- dbGetQuery(con, query)$cohort_definition_id

    # Step 2: Split cohort IDs into chunks of 50-100
    chunks <- split(cohort_ids, ceiling(seq_along(cohort_ids) / 50))

    # Step 3: Initialize an empty dataframe to store results
    cohort_counts <- data.frame()

    # Step 4: Loop through each chunk and execute the query
    for (chunk in chunks) {
      # Create a string of IDs for the WHERE clause
      ids <- paste(chunk, collapse = ",")
      print(ids)
      # Query for the current chunk
      query <- sprintf("
        SELECT cohort_definition_id, COUNT(DISTINCT subject_id) AS cohort_count
        FROM sb_jtelford.jmt
        WHERE cohort_definition_id IN (%s)
        GROUP BY cohort_definition_id
        ORDER BY cohort_definition_id;", ids)

      # Fetch data and append to the result dataframe
      temp_df <- dbGetQuery(con, query)
      cohort_counts <- rbind(cohort_counts, temp_df)
    }






    names(cohort_counts) = c("cohortId","count")

    # Get cohort info from csv
    cohort_names = read.csv(paste0(outputFolder,"/cohortDefinitionSetList.csv"))

    cohort_information <- merge(cohort_names, cohort_counts, by = "cohortId", all.x = TRUE)
    cohort_information$count[is.na(cohort_information$count)] <- 0
    write.csv(cohort_information,paste0(outputFolder,"/cohort_information.csv"), row.names = FALSE)
    DatabaseConnector::disconnect(connection = con)
}

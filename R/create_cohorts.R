source(here::here("R/study_subsetting.R"))
# source(here::here("R/collect_tables.R"))
# source(here::here("R/collect_locations.R"))
suppressMessages(suppressWarnings(library(tidyverse)))
generateCohorts <- function(
    executionSettings,
    outputFolder) {
  usethis::ui_info(
    "Cohorts generation in parallelizaion. You won't be able to see any generation messages"
  )
  cl <- parallel::makeCluster(parallel::detectCores())
  on.exit(parallel::stopCluster(cl))
  type <- "analysis"
  if (!dir.exists(outputFolder)) {
    dir.create(outputFolder)
  }
  cohortManifest <- getCohortManifest()
  cohortsToCreate <- cohortManifest$cohortsToCreate
  .cohortManifest <- cohortManifest$cohortManifest
  executionSettings <- executionSettings
  name <- executionSettings$cohortTable
  generateCohort <- function(cohort) {
    CohortGenerator::generateCohortSet(
      connectionDetails = executionSettings$connectionDetails,
      cdmDatabaseSchema = executionSettings$cdmDatabaseSchema,
      cohortDatabaseSchema = executionSettings$workDatabaseSchema,
      cohortTableNames = cohortTableNames,
      cohortDefinitionSet = cohort
    )
  }
  cohortTableNames <- list(
    cohortTable = paste0(name),
    cohortInclusionTable = paste0(name, "_inclusion"),
    cohortInclusionResultTable = paste0(name, "_inclusion_result"),
    cohortInclusionStatsTable = paste0(name, "_inclusion_stats"),
    cohortSummaryStatsTable = paste0(name, "_summary_stats"),
    cohortCensorStatsTable = paste0(name, "_censor_stats")
  )
  q <- purrr::quietly(CohortGenerator::createCohortTables)
  q(connectionDetails = executionSettings$connectionDetails,
    cohortDatabaseSchema = executionSettings$workDatabaseSchema,
    cohortTableNames = cohortTableNames)
  func_env <- new.env()
  func_env$cohortTableNames <- cohortTableNames
  func_env$generateCohort <- generateCohort
  func_env$executionSettings <- executionSettings
  parallel::clusterExport(
    cl, c("cohortTableNames", "generateCohort", "executionSettings"),
    envir = func_env
  )
  tik <- Sys.time()
  # 1 Create main cohorts (target and stratas)
  usethis::ui_info("Step 1: Main cohorts")
  pbapply::pbwalk(X = split(cohortsToCreate, seq(nrow(cohortsToCreate))), FUN = generateCohort, cl = cl)
  studySets <- studySubsetting(cohortManifest)
  generationSet <- studySets %>%
    dplyr::filter(!cohortId %in% cohortsToCreate$cohortId)

  # make sure subseting cohorts are created before new subsetting
  order1 <- generationSet %>% dplyr::filter(
    cohortId <= 50000
  )
  order2 <- generationSet %>% dplyr::filter(
    cohortId > 50000
  )
  # 1 Create subsettings cohorts (target and stratas)
  usethis::ui_info(
    "Step 2: 1 level Subset"
  )
  pbapply::pbwalk(X = split(order1, seq(nrow(order1))), FUN = generateCohort, cl = cl)
  usethis::ui_info(
    "Step 3: 2 level Subset"
  )
  pbapply::pbwalk(X = split(order2, seq(nrow(order2))), FUN = generateCohort, cl = cl)

  func_env <- NULL
  tok <- Sys.time()
  tdif <- tok - tik
  tok_format <- paste(scales::label_number(0.01)(as.numeric(tdif)), attr(tdif, "units"))
  cli::cat_line()
  cli::cat_bullet("Generation and subsetting took: ", crayon::red(tok_format), bullet = "info", bullet_col = "blue")
  # get cohort counts
  cohortCounts <- CohortGenerator::getCohortCounts(
    connectionDetails = executionSettings$connectionDetails,
    cohortDatabaseSchema = executionSettings$workDatabaseSchema,
    cohortTable = cohortTableNames$cohortTable,
    cohortDefinitionSet = studySets
  ) %>%
    dplyr::distinct() %>%
    dplyr::select(cohortId, cohortName, cohortEntries, cohortSubjects)
  savePath <- fs::path(outputFolder, "cohort_counts.csv")
  readr::write_csv(x = cohortCounts, file = savePath)
  cli::cat_bullet("Saving Generated Cohorts to ", crayon::cyan(savePath),
                  bullet = "tick", bullet_col = "green"  )
  return(cohortCounts[, 1:2])

}

getCohortManifest <- function() {
  inputPath <- fs::path("cohorts")
  # get cohort file paths
  cohortFiles <- fs::dir_ls(inputPath, recurse = TRUE, type = "file", glob = "*.json")
  # get cohort names
  cohortNames <- fs::path_file(cohortFiles) %>%
    fs::path_ext_remove()
  # get cohort type
  cohortType <- fs::path_dir(cohortFiles) %>%
    basename() %>%
    gsub(".*_", "", .)

  # future addition of hash
  hash <- purrr::map(cohortFiles, ~ readr::read_file(.x)) %>%
    purrr::map_chr(~ digest::digest(.x, algo = "sha1")) %>%
    unname()

  # return tibble with info
  tb <- tibble::tibble(
    name = cohortNames,
    type = cohortType,
    hash = hash,
    file = cohortFiles %>% as.character()
  ) %>%
    dplyr::mutate(
      id = dplyr::row_number(), .before = 1
    )
  cohortsToCreate <- tb %>%
    dplyr::mutate(
      json = purrr::map_chr(file, ~ readr::read_file(.x))
    ) %>%
    dplyr::select(id, name, json) %>%
    dplyr::rename(cohortId = id, cohortName = name)

  cohortsToCreate$sql <- purrr::map_chr(
    cohortsToCreate$json,
    ~ CirceR::buildCohortQuery(
      CirceR::cohortExpressionFromJson(.x),
      CirceR::createGenerateOptions(generateStats = F)
    )
  )
  return(
    list(
      cohortsToCreate = cohortsToCreate,
      cohortManifest = tb
    )
  )
}

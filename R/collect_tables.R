settingGroupPatients <- function(cohortNamesDf) {
  genders <- c("Female", "Male")
  frames <- c("Anytime", "365")
  opPerm <- c("182d", "365d", "728d")
  grid_op <- 2016:2023

  cohorts <- c(
    map_chr(grid_op, ~ paste("Cohort_3", .x, sep = "_")),
    "Cohort_4",
    purrr::map_chr(opPerm, ~ paste("HIV_Cohort_3", .x, sep = "_")),
    purrr::map_chr(opPerm, ~ paste("HIV_Cohort_4", .x, sep = "_"))
  )
  grid3Category <- tidyr::expand_grid(genders, frames, cohorts)

  regexes_3 <- purrr::pmap_chr(
    grid3Category, ~ glue::glue("{..3}.*\\&.*\\&.*{..2}.*{..1}"))

  ## collect cohorts with 3 factors and gender
  lstThreeGender <- purrr::map(
    regexes_3, ~ dplyr::filter(cohortNamesDf, grepl(.x, cohortName)))
  namesL <- purrr::pmap(
    grid3Category, ~ paste(..1, ..2, ..3, sep = "_"))
  names(lstThreeGender) <- namesL


  grid2Category <- tidyr::expand_grid(frames, cohorts)
  regexes_3 <- purrr::pmap_chr(
    grid2Category, ~ glue::glue("{..2}.*\\&.*\\&.*{..1}"))
  cohortNamesDfTrim_1 <- cohortNamesDf %>%
    dplyr::filter(
      !cohortId %in% purrr::map_dfr(lstThreeGender, data.frame)$cohortId)
  ## collect cohorts with 3 factors and no gender
  lstThreeWoGender <- purrr::map(
    regexes_3, ~ dplyr::filter(cohortNamesDfTrim_1, grepl(.x, cohortName)))

  namesL <- purrr::pmap(grid2Category, ~ paste(..1, ..2, sep = "_"))
  names(lstThreeWoGender) <- namesL


  cohortNamesDfTrim_2 <- cohortNamesDfTrim_1 %>%
    dplyr::filter(
      !cohortId %in% purrr::map_dfr(lstThreeWoGender, data.frame)$cohortId
      )



  regexes_2 <- purrr::pmap_chr(
    grid3Category, ~ glue::glue("{..3}.*\\&.*{..2}.*{..1}"))

  namesL <- purrr::pmap(grid3Category, ~ paste(..1, ..2, ..3, sep = "_"))


  ## collect cohorts with 2 factors and  gender
  lstTwoGender <- purrr::map(
    regexes_2, ~ dplyr::filter(cohortNamesDfTrim_2, grepl(.x, cohortName)))


  cohortNamesDfTrim_3 <- cohortNamesDfTrim_2 %>%
    dplyr::filter(
      !cohortId %in% purrr::map_dfr(lstTwoGender, data.frame)$cohortId)


  grid2Category <- tidyr::expand_grid(frames, cohorts)


  regexes_2 <- purrr::pmap_chr(
    grid2Category, ~ glue::glue("{..2}.*\\&.*{..1}"))
  ## collect cohorts with 2 factors and no gender
  lstTwoWoGender <- purrr::map(
    regexes_2,
    ~ dplyr::filter(cohortNamesDfTrim_3, grepl(.x, cohortName))
  )



  cohortNamesDfTrim_4 <- cohortNamesDfTrim_3 %>%
    dplyr::filter(
      !cohortId %in% purrr::map_dfr(lstTwoWoGender, data.frame)$cohortId
     )

  regexes_3 <- purrr::pmap_chr(
    grid3Category, ~ glue::glue("{..3}.*{..2}.*{..1}"))

  lstOneGender <- purrr::map(
    regexes_3, ~ dplyr::filter(cohortNamesDfTrim_4, grepl(.x, cohortName)))



  cohortNamesDfTrim_5 <- cohortNamesDfTrim_4 %>%
    dplyr::filter(
      !cohortId %in% purrr::map_dfr(lstOneGender, data.frame)$cohortId
)

  regexes_2 <- purrr::pmap_chr(grid2Category, ~ glue::glue("{..2}.*{..1}"))

  lstOneWoGender <- purrr::map(
    regexes_2, ~ dplyr::filter(cohortNamesDfTrim_5, grepl(.x, cohortName)))



  cohortNamesDfTrim_6 <- cohortNamesDfTrim_5 %>%
    dplyr::filter(
      !cohortId %in% purrr::map_dfr(lstOneWoGender, data.frame)$cohortId)

  return(
    list(
      noRiskStratas = cohortNamesDfTrim_6,
      threeFactorsGender = lstThreeGender,
      threeFactorsNoGender = lstThreeWoGender,
      twoFactorsGender = lstTwoGender,
      twoFactorsNoGender = lstTwoWoGender,
      oneFactorGender = lstOneGender,
      oneFactorNoGender = lstOneWoGender
    )
  )
}
collect3FactorCounts <- function(
    settingGroupPatientsLists,
    executionSettings,
    outputFolder) {
  sql <- "
    SELECT
        YEAR(cohort_start_date) AS year,
        COUNT(DISTINCT subject_id) AS patient_count
    FROM
        @work_schema.@cohort_table
    WHERE
        cohort_definition_id IN (  @cohort_ids )
    GROUP BY
        YEAR(cohort_start_date)
    ORDER BY
        year;
    "
  con <- DatabaseConnector::connect(executionSettings$connectionDetails)
  on.exit(DatabaseConnector::disconnect(con))
  withGender <-
    withGender <- lapply(
      purrr::pluck(settingGroupPatientsLists, "threeFactorsGender"),
      function(df) {
        if (nrow(df) > 0) {
          return(df)
        } else {
          return(NULL)
        }
      }
    ) %>% purrr::compact()
  withoutGender <-
    withoutGender <- lapply(
      purrr::pluck(
        settingGroupPatientsLists,
        "threeFactorsNoGender"
      ),
      function(df) {
        if (nrow(df) > 0) {
          return(df)
        } else {
          return(NULL)
        }
      }
    ) %>% purrr::compact()


  lst <- list(withGender, withoutGender)

  res <- purrr::map_dfr(lst, function(xx) {
    counts <- purrr::map2_dfr(
      xx, names(xx), ~ DatabaseConnector::renderTranslateQuerySql(
        con,
        sql,
        cohort_ids = .x$cohortId,
        work_schema = executionSettings$workDatabaseSchema,
        cohort_table = executionSettings$cohortTable
      ) %>%
        dplyr::rename_all(tolower) %>%
        dplyr::mutate(
          chName = .y
        )
    ) %>% dplyr::select(
      cohortName = chName, patient_count, year
    )
  })
  savePath <- fs::path(outputFolder, "Factors3_counts.csv")
  readr::write_csv(x = res, file = savePath)
  cli::cat_bullet("Saving 3 factors cohort info to ", crayon::cyan(savePath),
                  bullet = "tick", bullet_col = "green"
  )
  return(res)
}
collectOneAndTwoFctorCounts <- function(
    settingGroupPatientsLists,
    executionSettings,
    outputFolder) {
  sql <- "
    SELECT
    cohort_definition_id,
        YEAR(cohort_start_date) AS year,
        COUNT(DISTINCT subject_id) AS patient_count
    FROM
        @work_schema.@cohort_table
    WHERE
        cohort_definition_id IN (  @cohort_ids )
    GROUP BY
        YEAR(cohort_start_date), cohort_definition_id
    ORDER BY
        year;
    "
  con <- DatabaseConnector::connect(executionSettings$connectionDetails)
  on.exit(DatabaseConnector::disconnect(con))
  withGender <- do.call(rbind, purrr::pluck(settingGroupPatientsLists, "twoFactorsGender"))
  withoutGender <- do.call(rbind, purrr::pluck(settingGroupPatientsLists, "twoFactorsNoGender"))
  withGender1 <- do.call(rbind, purrr::pluck(settingGroupPatientsLists, "oneFactorGender"))
  withoutGender1 <- do.call(rbind, purrr::pluck(settingGroupPatientsLists, "oneFactorNoGender"))
  ds <- rbind(
    withGender,
    withoutGender,
    withGender1,
    withoutGender1
  )
  res <- DatabaseConnector::renderTranslateQuerySql(
    con,
    sql,
    cohort_ids = ds$cohortId,
    work_schema = executionSettings$workDatabaseSchema,
    cohort_table = executionSettings$cohortTable
  ) %>% dplyr::rename_all(tolower)

  pattern <- "(?<=with\\s)(.*?)(?=,)"
  frendlyNames <- ds %>%
    dplyr::inner_join(
      res,
      by = c("cohortId" = "cohort_definition_id")
    ) %>%
    dplyr::select(
      cohortName, patient_count, year
    )

  savePath <- fs::path(outputFolder, "Factors1_2_counts.csv")
  readr::write_csv(x = frendlyNames, file = savePath)
  cli::cat_bullet("Saving 1, 2 factors cohort info to ", crayon::cyan(savePath),
                  bullet = "tick", bullet_col = "green"
  )
  return(frendlyNames)
}
collectHighLevelStratas <- function(
    settingGroupPatientsLists,
    executionSettings,
    outputFolder) {
  con <- DatabaseConnector::connect(executionSettings$connectionDetails)
  on.exit(DatabaseConnector::disconnect(con))
  noRisk <- purrr::pluck(settingGroupPatientsLists, "noRiskStratas")
  sql <- "
    SELECT
    cohort_definition_id,
        YEAR(cohort_start_date) AS year,
        COUNT(DISTINCT subject_id) AS patient_count
    FROM
        @work_schema.@cohort_table
    WHERE
        cohort_definition_id IN (  @cohort_ids )
    GROUP BY
        YEAR(cohort_start_date), cohort_definition_id
    ORDER BY
        year;
    "
  counts <- DatabaseConnector::renderTranslateQuerySql(
    con,
    sql,
    cohort_ids = noRisk$cohortId,
    work_schema = executionSettings$workDatabaseSchema,
    cohort_table = executionSettings$cohortTable
  ) %>%
    dplyr::rename_all(tolower) %>%
    dplyr::inner_join(
      noRisk,
      by = c("cohort_definition_id" = "cohortId")
    ) %>%
    dplyr::select(
      cohortName, patient_count, year
    )
  savePath <- fs::path(outputFolder, "no_factors_counts.csv")
  readr::write_csv(x = counts, file = savePath)
  cli::cat_bullet("Saving no factors cohort info to ", crayon::cyan(savePath),
                  bullet = "tick", bullet_col = "green"
  )
  return(counts)
}
prepareCall <- function(fn_name, args) {
  rlang::call2(
    eval(str2lang(fn_name)), !!!purrr::keep_at(
      args, names(args) %in%
        rlang::fn_fmls_names(
          eval(str2lang(fn_name))
        )
    )
  )
}
prepForWide <- function(dat) {
  unique_data <- dat %>%
    reframe(
      patient_count = first(patient_count),
      year = first(year),
      .by = c(cohortName, year)
    ) %>%
    tidyr::pivot_wider(
      names_from = year,
      values_from = c("patient_count")
    )
}
filterByRegex <- function(data, pattern) {
  data %>%
    filter(grepl(pattern, cohortName)) %>%
    arrange(cohortName)
}
studyExportFunctions <- function() c('generateCohorts', "collectOneAndTwoFctorCounts", "collect3FactorCounts", "collectHighLevelStratas", "collectLocations")

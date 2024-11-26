collectLocations <- function(
    settingGroupPatientsLists,
    executionSettings,
    outputFolder) {
  con <- DatabaseConnector::connect(executionSettings$connectionDetails)
  on.exit(DatabaseConnector::disconnect(con))
  noRisk1LevelStratas <- settingGroupPatientsLists$noRiskStratas %>%
    dplyr::filter(
      stringr::str_detect(cohortName, ' - ', T) |
        stringr::str_detect(cohortName, '\\d -  Female|\\d -  Male'))
  res <- DatabaseConnector::renderTranslateQuerySql(
    con,
    sql = "
    SELECT
    cohort_definition_id,
    location_id,
    YEAR(cohort_start_date) AS year,
      COUNT(DISTINCT subject_id) AS patient_count
    FROM
        ( select * from @work_schema.@cohort_table
        join @cdm_schema.person
        on subject_id = person_id ) cte

    WHERE
        cohort_definition_id IN (  @cohort_ids )
    GROUP BY
        YEAR(cohort_start_date), cohort_definition_id, location_id
    ORDER BY
        year;
    ",
    cohort_ids = noRisk1LevelStratas$cohortId,
    work_schema = executionSettings$workDatabaseSchema,
    cohort_table = executionSettings$cohortTable,
    cdm_schema = executionSettings$cdmDatabaseSchema
  ) %>% dplyr::rename_all(tolower) %>%
    dplyr::inner_join(
      noRisk1LevelStratas, by = c('cohort_definition_id' = 'cohortId')
    ) %>% dplyr::select(
      cohortName,location_id,  patient_count, year
    )
  savePath <- fs::path(outputFolder, "locations_counts.csv")
  readr::write_csv(x = res, file = savePath)
  cli::cat_bullet("Saving location counts to ", crayon::cyan(savePath),
                  bullet = "tick", bullet_col = "green"  )
  if(nrow(res) > 0) {  loc <- DatabaseConnector::renderTranslateQuerySql(
    con,
    sql = "
    SELECT DISTINCT* FROM @cdm_schema.location
    WHERE location_id IN (  @location_id )
    ",
    location_id = res$location_id,
    cdm_schema = executionSettings$cdmDatabaseSchema
  ) %>% dplyr::rename_all(tolower)

  savePath <- fs::path(outputFolder, "location_spec.csv")
  readr::write_csv(x = loc, file = savePath)
  cli::cat_bullet("Saving location spec to ", crayon::cyan(savePath),
                  bullet = "tick", bullet_col = "green"  )
  return(list(
    result = res,
    locations_spec = loc
  ))
  }




}

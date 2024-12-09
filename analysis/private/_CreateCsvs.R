# Create racestratacsv
createStrataTable <- function(connectionDetails = connectionDetails,executionsettings = executionsettings){

    con <- DatabaseConnector::connect(connectionDetails)

    query <- paste0("

    SELECT
       c.concept_name AS race,
        p.race_concept_id as code


    FROM
        @cdmDatabaseSchema.person p
    LEFT JOIN
        @cdmDatabaseSchema.concept c
    ON
         p.race_concept_id = c.concept_id
    where race in ('White','African American','Asian','Hispanic')
    GROUP BY
        code, race
    ;
    ")


    rendered_sql <- SqlRender::render(
      sql = query,
      cdmDatabaseSchema = executionSettings$cdmDatabaseSchema

    )

    # Translate SQL to target database dialect if needed
    # Example: Translating to PostgreSQL
    translated_sql <- SqlRender::translate(
      sql = rendered_sql,
      targetDialect = executionSettings$dbms
    )
    # Fetch data and append to the result dataframe
    races <- dbGetQuery(con, translated_sql)

    names(races)= c('race','code')
    if (!"Hispanic" %in% races$race) {
      # Create a new row with "Hispanic" and an empty code
      new_row <- data.frame(race = "Hispanic", code = 9999)

      # Add the new row to the data frame
      races <- rbind(races, new_row)
    }
    write.csv(races,paste0("analysis/settings/racestrata.csv"))
    DatabaseConnector::disconnect(connection = con)
}


getLocationIDTable <- function(connectionDetails = connectionDetails, cohorttoRun = cohort,filename = filename,executionsettings){
  con <- DatabaseConnector::connect(connectionDetails)
  sql_query <- "
SELECT
     l.state,
    COUNT(*) AS count
FROM
    @cohortDatabaseSchema.@cohortTable c
JOIN
    @cdmDatabaseSchema.person p
ON
    c.subject_id = p.person_id
LEFT JOIN
    @cdmDatabaseSchema.location l
ON
    p.location_id = l.location_id
WHERE
    c.cohort_definition_id = @cohorttoRun
GROUP BY
    l.state
ORDER BY
    count DESC;
"
  rendered_sql <- SqlRender::render(
  sql = sql_query,
  cdmDatabaseSchema = executionSettings$cdmDatabaseSchema,
  cohorttoRun = cohorttoRun,
  cohortDatabaseSchema = executionSettings$cohortDatabaseSchema,
  cohortTable = executionSettings$cohortTable,

)
  translated_sql <- SqlRender::translate(
    sql = rendered_sql,
    targetDialect = executionSettings$dbms
  )
  # Fetch data and append to the result dataframe
  locations <- dbGetQuery(con, translated_sql)
  outputlocation = paste0(outputFolder,"/location_results/locations_",filename,".csv")
  write.csv(locations,outputlocation,row.names = FALSE)
  DatabaseConnector::disconnect(connection = con)


}



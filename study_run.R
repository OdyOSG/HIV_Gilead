source(here::here('R/create_cohorts.R'))
outputFolder <- 'Par2'
executionSettings <- AlexSettings::returnExecutionSettings("hiv_new_ed4")
args <- list(
  executionSettings = executionSettings,
  outputFolder = outputFolder
)
funcs <- studyExportFunctions()
args$settingGroupPatientsLists <- eval(prepareCall(funcs[1], args)) %>%
  settingGroupPatients
results <- prepForWide(map_dfr(funcs[-c(1, 5)], ~ eval(prepareCall(.x, args))))
resList <- map(1:4, ~ filterByRegex(results, glue::glue("Cohort_{.x}")))
locations <- eval(prepareCall(funcs[5], args))


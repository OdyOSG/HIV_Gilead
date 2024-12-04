dates <- 2016:2023
path <- "cohorts/1_target"
grid <- tidyr::expand_grid(
  op = c(365, 728),
  dt = dates,
  ch = list.files(path, pattern = "Cohort_1.json|Cohort_3.json")
)
pwalk(grid, function(op = ..1, dt = ..2, ch = ..3) {
  nm <- gsub(".json", "", ch)
  .f <- file.path(path, ch)
  cohortExpr <- jsonlite::read_json(.f)
  cohortExpr$PrimaryCriteria$ObservationWindow$PriorDays <- op
  cohortExpr$PrimaryCriteria$CriteriaList[[1]]$DrugExposure$OccurrenceStartDate$Value <- paste(dt, "01", "01", sep = "-")
  cohortExpr$PrimaryCriteria$CriteriaList[[1]]$DrugExposure$OccurrenceStartDate$Extent <- paste(dt, "12", "31", sep = "-")
  cohortExpr$PrimaryCriteria$CriteriaList[[1]]$DrugExposure$OccurrenceStartDate$Op <-  "bt"
  write(
    RJSONIO::toJSON(cohortExpr, indent = 2, digits = 10),
    file.path(
      path, glue::glue("{nm}_{op}d_{dt}.json"))
  )
})
grid <- tidyr::expand_grid(
  op = c(365, 728), ch = list.files(path, pattern = "Cohort_2.json|Cohort_4.json")
)
pwalk(grid, function(op = ..1, ch = ..3) {
  nm <- gsub(".json", "", ch)
  .f <- file.path(path, ch)
  cohortExpr <- jsonlite::read_json(.f)
  cohortExpr$PrimaryCriteria$ObservationWindow$PriorDays <- op
  write(
    RJSONIO::toJSON(cohortExpr, indent = 2, digits = 10),
    file.path(
      path, glue::glue("{nm}_{op}d.json"))
  )
})
for(coh in list.files(path, pattern = "Cohort_1.json|Cohort_2.json|Cohort_3.json|Cohort_4.json", full.names = T)) {

  unlink(coh, recursive = T)


}

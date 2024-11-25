# library(Capr)
# library(tidyverse)
# con_ss <- list.files(here::here('concept_sets'), full.names = T, pattern = 'csv')
# PWID <- read.csv(con_ss[[1]])
#
# con_set <- Capr::cs(
#   Capr::descendants(PWID$concept_id),
#   name = "PWID"
# )
# cohortTeamplate <- Capr::cohort(
#   entry = Capr::entry(Capr::conditionOccurrence(con_set),
#                       primaryCriteriaLimit = c("All")
#   ),
#   exit = Capr::exit(
#     endStrategy = Capr::fixedExit(
#       index = c("startDate"),
#       offsetDays = 7
#     )
#   )
# )
# Capr::writeCohort(cohortTeamplate, 'cohorts/strata/PWID.json')
#
# other <- read.csv(con_ss[[2]]) %>%
#   dplyr::filter(X != '') %>%
#   dplyr::nest_by(X)
#
# walk2(other$X, other$data, function(.nm = .x, .expr = .y) {
#   con_set <- Capr::cs(
#     Capr::descendants(as.integer(.expr$concept_id)),
#     name = .nm
#   )
#   cohortTeamplate <- Capr::cohort(
#     entry = Capr::entry(Capr::conditionOccurrence(con_set),
#                         primaryCriteriaLimit = c("All")
#     ),
#     exit = Capr::exit(
#       endStrategy = Capr::fixedExit(
#         index = c("startDate"),
#         offsetDays = 7
#       )
#     )
#   )
#   Capr::writeCohort(cohortTeamplate, file.path(
#     'cohorts',
#     'strata',
#     glue::glue(
#       "{.nm}.json"
#     )
#   ))
# })

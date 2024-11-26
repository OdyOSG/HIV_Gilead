cohortDf <- resList[[1]]
library(dplyr)
library(glue)
outputFormat <- function(cohortDf, cohortN) {

  age <- cohortDf %>%
    mutate(
      cohortName = gsub('Cohort_\\d_|\\d{4}', '', cohortName)
    ) %>%
    dplyr::filter(stringr::str_detect(
      cohortName, 'age'
    )) %>%
    mutate(
      cohortName = gsub('age_', '', cohortName),
      cohortName = gsub('_', '-', cohortName),
      age = gsub('d{3}d-  ', '', cohortName),
      cohortName = paste(dplyr::if_else(
        stringr::str_detect(cohortName, '365d'), 'original', 'sensitivity'
      ), age, sep = ' '),
      cohortName = gsub(' \\d{3}d- - ', '', cohortName)
      )  %>%
        dplyr::rename(!!sym(glue::glue('cohort_{cohortN}')) := cohortName) %>%
    select(-age)

  gender <- cohortDf %>%
    mutate(
      cohortName = gsub('Cohort_\\d_|\\d{4}', '', cohortName)
    ) %>%
    dplyr::filter(stringr::str_detect(
      cohortName, 'Male|Female'
    )) %>%
    mutate(
      gender = dplyr::if_else(
        stringr::str_detect(cohortName, 'Male'), 'Male', 'Female'
      ),
      cohortName = paste(dplyr::if_else(
        stringr::str_detect(cohortName, '365d'), 'original', 'sensitivity'
      ), gender, sep = ' ')
    ) %>%
      dplyr::rename(!!sym(glue::glue('cohort_{cohortN}')) := cohortName) %>%
      select(-gender)

  race <- cohortDf %>%
    mutate(
      cohortName = gsub('Cohort_\\d_|\\d{4}', '', cohortName)
    ) %>%
    dplyr::filter(stringr::str_detect(
      cohortName, 'NHW|NHB|Hispanic|Asian'
    ) & !stringr::str_detect(
      cohortName, '-.*-'
    )
    ) %>%
    mutate(
      race = case_when(
        stringr::str_detect(cohortName, 'NHW') ~ 'NHW',
        stringr::str_detect(cohortName, 'NHB') ~ 'NHB',
        stringr::str_detect(cohortName, 'Hispanic') ~ 'Hispanic',
        stringr::str_detect(cohortName, 'Asian') ~ 'Asian'
      ),
      cohortName = paste(dplyr::if_else(
        stringr::str_detect(cohortName, '365d'), 'original', 'sensitivity'
      ), race, sep = ' ')
    ) %>%
      dplyr::rename(!!sym(glue::glue('cohort_{cohortN}')) := cohortName) %>%
      select(-race)

  return(
    rbind(age, gender, race)
  )

}
aa <- outputFormat(cohortDf, 1)



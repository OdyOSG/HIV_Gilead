studySubsetting <- function(
    cohortManifest
) {
  cohortManifest <- getCohortManifest()
  .cohortManifest <- cohortManifest$cohortManifest
  target4Factors <- cohortManifest$cohortManifest %>%
    dplyr::filter(stringr::str_detect(name, 'Cohort_4') | stringr::str_detect(name, 'Cohort_3') | stringr::str_detect(name, 'Cohort_5')) %>%
    dplyr::pull(id)
  targetHighLevel <- cohortManifest$cohortManifest %>%
    dplyr::filter(stringr::str_detect(name, 'Cohort_2') | stringr::str_detect(name, 'Cohort_1')) %>%
    dplyr::pull(id)

  factorOne <- cohortManifest$cohortManifest %>%
    dplyr::filter(type %in% c('factorOne'))
  origin <- cohortManifest$cohortsToCreate %>%
    dplyr::filter(
      cohortId  %in% c(target4Factors, targetHighLevel)
    )

  originTarget4Factors <- cohortManifest$cohortsToCreate %>%
    dplyr::filter(
      cohortId  %in% target4Factors
      )

  permutationsOne <- tibble::tibble(
    first = factorOne$id,
    second = NA,
    third = NA
  )
  factorTwo <- cohortManifest$cohortManifest %>%
    dplyr::filter(type %in% c('factorTwo'))
  uniqueCombinations <- t(combn(factorTwo $id, 2))
  permutationsTwo <- tibble::tibble(
    first = uniqueCombinations[,1],
    second = uniqueCombinations[,2],
    third = NA
  )
  uniqueCombinations <- t(combn(factorTwo$id, 3))
  permutationsThree <- tibble::tibble(
    first = uniqueCombinations[,1],
    second = uniqueCombinations[,2],
    third = uniqueCombinations[,3]
  )

  permutation <- rbind(permutationsOne, permutationsTwo, permutationsThree)
  permutation <- permutation %>% mutate(
    second = ifelse(is.na(second), 0,  second),
    third = ifelse(is.na(third), 0,  third)
  ) %>% distinct()

  permutation$definitionId <- 1:nrow(permutation)

  cohortSetAny <- unique(purrr::pmap_dfr(
    permutation, ~ createCohortSubsetSpec(
      strataCohortIds = c(..1, ..2, ..3),
      definitionId = ..4,
      cohortDefinitionSet = originTarget4Factors,
      .cohortManifest = .cohortManifest
    )
  ))

  mx <- max(permutation$definitionId)
  permutation$definitionId <- mx+1:nrow(permutation)
  cohortSetYear <- unique(purrr::pmap_dfr(
    permutation, ~ createCohortSubsetSpec(
      strataCohortIds = c(..1, ..2, ..3),
      definitionId = ..4,
      cohortDefinitionSet = originTarget4Factors,
      .cohortManifest = .cohortManifest,
      anyTime = FALSE
    )
  ))
  mx <- permutation$definitionId %>% max()
  specification <- strataSpec(
    ageBuckets = list(
      list(l = 15,
           u = 29),
      list(l = 30,
           u = 44),
      list(l = 45,
           u = 55),
      list(l = 56,
           u = 150)
    ),
    male = TRUE,
    female = TRUE,
    noGender = TRUE
  )
  specification$definitionId <- specification$definitionId + mx
  ageSet <- unique(purrr::pmap_dfr(
    specification[c(1:4, 7), ], ~  executeSubsetPreparation(
      type = ..1,
      spec = ..2,
      definitionId = ..3,
      targetSet = origin,
      targetCohortIds = origin$cohortId
    )))

  genderSet <- unique(purrr::pmap_dfr(
    specification[-c(1:4, 7), ], ~  executeSubsetPreparation(
      type = ..1,
      spec = ..2,
      definitionId = ..3 * 10,
      targetSet = unique(rbind(cohortSetAny, cohortSetYear)),
      targetCohortIds = unique(rbind(cohortSetAny, cohortSetYear))$cohortId
    )))

  setForRace <- ageSet %>% filter(!stringr::str_detect(cohortName, ' -  '))
  raceSet_ <- unique(rbind(CohortGenerator::addCohortSubsetDefinition(
    cohortDefinitionSet = setForRace,
    cohortSubsetDefintion = NHWStrataDef(definitionId = 699)),
    CohortGenerator::addCohortSubsetDefinition(
    cohortDefinitionSet = setForRace,
    cohortSubsetDefintion = NHBStrataDef(definitionId = 799)),
  CohortGenerator::addCohortSubsetDefinition(
    cohortDefinitionSet = setForRace,
    cohortSubsetDefintion = AsianStrataDef(definitionId = 899)),
  CohortGenerator::addCohortSubsetDefinition(
    cohortDefinitionSet = setForRace,
    cohortSubsetDefintion = HispanicEthStrataDef(definitionId = 999))
  ))
  raceSet <- purrr::pmap_dfr(
    specification[-c(1:4, 7), ], ~  executeSubsetPreparation(
      type = ..1,
      spec = ..2,
      definitionId = ..3 + 3000,
      targetSet = raceSet_,
      targetCohortIds = raceSet_$cohortId
    )) %>% dplyr::filter(!cohortId %in% c(
      1:25)
      )


  setToCreate <- rbind(ageSet, genderSet, raceSet) %>% unique() %>%
    filter(! (stringr::str_detect(
      cohortName, 'Cohort_1|Cohort_2'
    ) & stringr::str_detect(
      cohortName, '-.*-'
    )) )
  set.seed(123)
  random_numbers <- sample(20:500000, nrow(setToCreate), replace = FALSE)
  rpl_sub <- rep(c('#cohort_sub_base'), nrow(setToCreate))
  rpl_S_1 <- rep(c('#S_1'), nrow(setToCreate))
  cng <- pmap(list(rpl_sub,  setToCreate$sql, random_numbers),  ~ gsub(..1, glue::glue('{..1}_{..3}'), ..2))
  cng <- pmap(list(rpl_S_1,  cng, random_numbers),  ~ gsub(..1, glue::glue('{..1}_{..3}'), ..2))

  setToCreate$sql <- unlist(cng)


  return(setToCreate)
}






createCohortSubsetSpec <- function(
    strataCohortIds,
    cohortDefinitionSet,
    definitionId,
    .cohortManifest,
    anyTime = TRUE
    #,by2Factors = FALSE
    ) {
  strataCohortIds <- strataCohortIds[which(strataCohortIds != 0)]
  names <- .cohortManifest %>% filter(id %in% strataCohortIds) %>%
    pull(name) %>% paste(collapse = '&')
  subsetDefByCohort <- CohortGenerator::createCohortSubsetDefinition(
    name = '',
    definitionId = definitionId,
    subsetOperators = list(
      CohortGenerator::createCohortSubset(
        name = glue::glue("with {names},{ifelse(anyTime, 'Anytime', '365')}"),
        cohortIds = strataCohortIds,
        cohortCombinationOperator = 'all',
        negate  = FALSE,
        startWindow = CohortGenerator::createSubsetCohortWindow(
          startDay = -1 * ifelse(anyTime, 9999, 365),
          endDay = ifelse(anyTime, 9999, 365),
          targetAnchor = "cohortStart"
        ),
        endWindow = CohortGenerator::createSubsetCohortWindow(
          startDay = -1 * ifelse(anyTime, 9999, 365),
          endDay = 9999,
          targetAnchor = "cohortStart"
        )
      )
    )
  )
  .cohortDefinitionSet <- cohortDefinitionSet %>%
    CohortGenerator::addCohortSubsetDefinition(subsetDefByCohort)
  if(nrow(.cohortDefinitionSet) > 0) {
    return(.cohortDefinitionSet)
  }
}





ageStrataDef <- function(
    lowerAge,
    upperAge,
    definitionId
) {
  CohortGenerator::createCohortSubsetDefinition(
    name = '',
    definitionId = definitionId,
    subsetOperators = list(
      CohortGenerator::createDemographicSubset(
        name = glue::glue("age_{lowerAge}_{upperAge}"),
        ageMin = lowerAge,
        ageMax = upperAge
      )
    ))
}
maleStrataDef <- function(
    definitionId
) {
  CohortGenerator::createCohortSubsetDefinition(
    name = '',
    definitionId = definitionId,
    subsetOperators = list(
      CohortGenerator::createDemographicSubset(
        name = 'Male',
        gender = 'male'
      )
    ))}
femaleStrataDef <- function(
    definitionId
) {
  CohortGenerator::createCohortSubsetDefinition(
    name = '',
    definitionId = definitionId,
    subsetOperators = list(
      CohortGenerator::createDemographicSubset(
        name = 'Female',
        gender = 'female'
      )
    ))}
noGenderStrataDef <- function(
    definitionId
) {
  CohortGenerator::createCohortSubsetDefinition(
    name = '',
    definitionId = definitionId,
    subsetOperators = list(

      CohortGenerator::createDemographicSubset(
        name = 'no_gender',
        gender = 'no_gender'
      )
    ))}






strataSpec <- function(
    ageBuckets = list(
      list(l = 15,
           u = 29),
      list(l = 30,
           u = 44),
      list(l = 45,
           u = 55),
      list(l = 56,
           u = 150)
    ),
    male = TRUE,
    female = TRUE,
    noGender = NULL) {
  specification <- data.frame()
  startaList <- list(
    ageBuckets = ageBuckets,
    male = male,
    female = female,
    noGender = noGender
  )
  if(!is.null(startaList$ageBuckets)) {
    ageStrata <- tidyr::expand_grid(
      type = 'strataAge',
      spec = startaList$ageBuckets)

    specification <- rbind(specification, ageStrata)
  }

  if(!is.null(startaList$male)) {
    maleStrata <- tidyr::expand_grid(
      type = 'strataMale',
      spec = startaList$male
    )
    specification <- rbind(specification, maleStrata)
  }
  if(!is.null(startaList$female)) {
    femaleStrata <- tidyr::expand_grid(
      type = 'strataFemale',
      spec = startaList$female
    )
    specification <- rbind(specification, femaleStrata)
  }
  if(!is.null(startaList$noGender)) {
    noGenderStrata <- tidyr::expand_grid(
      type = 'strataNoGender',
      spec = startaList$noGender
    )
    specification <- rbind(specification, noGenderStrata)
  }
  if(nrow(specification) > 0) {
    specification$definitionId <- 1:nrow(specification)
    return(specification)
  }

}


NHWStrataDef <- function(
    definitionId
) {
  CohortGenerator::createCohortSubsetDefinition(
    name = '',
    definitionId = definitionId,
    subsetOperators = list(
      CohortGenerator::createDemographicSubset(
        name = 'NHW',
        race = 8527
      )
    ))}
NHBStrataDef <- function(
    definitionId
) {
  CohortGenerator::createCohortSubsetDefinition(
    name = '',
    definitionId = definitionId,
    subsetOperators = list(
      CohortGenerator::createDemographicSubset(
        name = 'NHB',
        race = c(38003598, 8516)
      )
    ))}
AsianStrataDef <- function(
    definitionId
) {
  CohortGenerator::createCohortSubsetDefinition(
    name = '',
    definitionId = definitionId,
    subsetOperators = list(
      CohortGenerator::createDemographicSubset(
        name = 'Asian',
        race = c(8515, 38003574)
      )
    ))}

HispanicEthStrataDef <- function(
    definitionId
) {
  CohortGenerator::createCohortSubsetDefinition(
    name = '',
    definitionId = definitionId,
    subsetOperators = list(
      CohortGenerator::createDemographicSubset(
        name = 'HispanicEth',
        ethnicity = 38003563
      )
    ))}


executeSubsetPreparation <- function(
    type,
    spec,
    definitionId,
    targetSet,
    targetCohortIds) {
  outputSet <- data.frame()
  if(type == 'strataAge') {
    tmp <- CohortGenerator::addCohortSubsetDefinition(
      cohortDefinitionSet = targetSet,
      cohortSubsetDefintion = ageStrataDef(
        lowerAge = spec$l,
        upperAge = spec$u,
        definitionId = definitionId
      ))
    outputSet <- unique(rbind(tmp, outputSet))
  }
  if(type == 'strataMale' & spec[[1]] == TRUE) {
    tmp <- CohortGenerator::addCohortSubsetDefinition(
      cohortDefinitionSet = targetSet,
      cohortSubsetDefintion = maleStrataDef(definitionId = definitionId),
      targetCohortIds = targetCohortIds)
    outputSet <- unique(rbind(tmp, outputSet))
  }
  if(type == 'strataFemale' & spec[[1]] == TRUE) {
    tmp <- CohortGenerator::addCohortSubsetDefinition(
      cohortDefinitionSet = targetSet,
      cohortSubsetDefintion = femaleStrataDef(definitionId = definitionId),
      targetCohortIds = targetCohortIds)
    outputSet <- unique(rbind(tmp, outputSet))

  }
  if(type == 'strataNoGender' & spec[[1]] == TRUE) {
    tmp <- CohortGenerator::addCohortSubsetDefinition(
      cohortDefinitionSet = targetSet,
      cohortSubsetDefintion = noGenderStrataDef(definitionId = definitionId))
    outputSet <- unique(rbind(tmp, outputSet))
  }
  return(outputSet)
}










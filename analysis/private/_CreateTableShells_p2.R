# Begin Building Table shells
# Pass connection details
# Tables 51 and 53 not in use.
createTableShells2 <- function(con = con, outputFolder = outputFolder,executionSettings = executionSettings){
    cohortDatabaseSchema = executionSettings$cohortDatabaseSchema
    cohortTable     = executionSettings$cohortTable
    dbms = executionSettings$dbms
    # Get cohort counts from DB

    cohort_information = read.csv(paste0(outputFolder,"/cohort_information.csv"))
    # Create a list of row names
    row_names12 <- c("All","15 - 29","30 - 44","45 - 55","55+","Unknown age",
                     "male","female","Unknown Sex",
                     "White","African American","Hispanic","Asian","Other or Unknown"
    )




    # Set the column names to 2016 to 2023


    # Set the row names
    table1 <- data.frame(matrix(0, nrow = length(row_names12), ncol = 8))
    rownames(table1) <- row_names12
    colnames(table1) <- 2016:2023

    table51 <- data.frame(matrix(0, nrow = length(row_names12), ncol = 8))
    rownames(table51) <- row_names12
    colnames(table51) <- 2016:2023

    pop5cohorts = data.frame(matrix(0, nrow = length(1), ncol = 8))
    colnames(pop5cohorts) <- 2016:2023

    cohortIDTable <- data.frame(matrix(0, nrow = length(row_names12), ncol = 8))
    colnames(cohortIDTable) <- 2016:2023
    rownames(cohortIDTable) <- row_names12
    # Create table 1

    Pop5tables = cohort_information[grepl("Pop5", cohort_information$cohortName), ]

    #Create table with Pop5 cohorts
    for (i in 1:8) {
      # Extract year from the source name
      year <- gsub(".*Pop5(\\d{4}).*", "\\1", Pop5tables$cohortName[i])
      print(year)
      # Check if the year is in the target table's column names
      if (year %in% colnames(table1)) {
        pop5cohorts["All", year] <- Pop5tables$cohortId[i]
        cohort = Pop5tables$cohortId[i]
        filename = paste0("Positive",Pop5tables$cohortName[i])
        getLocationIDTable(con = con, cohorttoRun = cohort,filename,executionsettings = executionsettings)
      }
    }

    #Begin creating table1
    Pop1tables = cohort_information[grepl("Pop1", cohort_information$cohortName), ]
    for (i in 1:8) {
      # Extract year from the source name
      year <- gsub(".*Pop1(\\d{4}).*", "\\1", Pop1tables$cohortName[i])
      print(year)
      # Check if the year is in the target table's column names
      if (year %in% colnames(table1)) {
        table1["All", year] <- Pop1tables$cohortId[i]
        cohort = Pop1tables$cohortId[i]
        filename = paste0("Positive",Pop1tables$cohortName[i])
        getLocationIDTable(con = con, cohorttoRun = cohort,filename,executionsettings = executionsettings)
      }
    }

    # Combine counts for male and female for each 'Pop' (same Pop value)
    Pop1tables$year <- gsub(".*Pop1(\\d{4}).*", "\\1", Pop1tables$cohortName)
    Pop1tables$gender <- ifelse(grepl(" male",  Pop1tables$cohortName), "male", "female")
    allgroups = c("15 - 29","30 - 44","45 - 55","Age 55+", "White","African American","Hispanic","Asian")
    group ="Age 55+"
    for (group in allgroups){
      filtered_rows <- Pop1tables[grepl(group, Pop1tables$cohortName), ]
      if (group == "Age 55+"){group = "55+"}
      aggregated_data <- aggregate(cohortId ~ year + gender, data = filtered_rows, sum)
      sum_counts <- paste(aggregated_data$cohortId[aggregated_data$year == i], collapse = " ")

      for (i in 2016:2023) {
        sum_counts <- paste(aggregated_data$cohortId[aggregated_data$year == i], collapse = " ")
        # Ensure that the column (year i) exists in table1
        if (as.character(i) %in% colnames(table1)) {
          # Assign the sum to the appropriate row and column in table1
          table1[group, as.character(i)] <- sum_counts
        } else {
          # Handle the case where the column does not exist
          cat("Column", i, "not found in table1")
        }
      }
    }

    agetables = Pop1tables[grepl("Age", Pop1tables$cohortName), ]
    for (group in c("male","female")){
      filtered_rows <- agetables[agetables$gender == group, ]

      for (i in 2016:2023) {
        sum_counts <- paste(filtered_rows$cohortId[filtered_rows$year == i], collapse = " ")
        # Ensure that the column (year i) exists in table1
        if (as.character(i) %in% colnames(table1)) {
          # Assign the sum to the appropriate row and column in table1
          table1[group, as.character(i)] <- sum_counts
        } else {
          # Handle the case where the column does not exist
          cat("Column", i, "not found in table1.")
        }
      }
    }

    # Manage unknowns
    # table1["Unknown age",] = table1["All",] - table1["15 - 29",] - table1["30 - 44",] - table1["45 - 55",] - table1["55+",]
    #
    # table1["Unknown Sex",] = table1["All",] - table1["male",] - table1["female",]
    # table1["Other or Unknown",] = table1["All",] - table1["NHW",] - table1["NHB",] - table1["Hispanic",] - table1["AAPI",]
    write.csv(table1,paste0(outputFolder,"/table1cohortID.csv"))
    #table1$combined <- apply(table1[, -1], 1, function(row) list(row))
    table1 <- table1[, !names(table1) %in% "modified_string"]
    table1$modified_string <- apply(table1, 1, function(row) {
      first <- paste(unique(row), collapse = " ")  # Combine unique values with a space
      modified_string <- gsub(" ", ", ", first)    # Replace spaces with commas
      return(modified_string)
    })
    # For each row
    i=1
    for (i in (1:nrow(table1))){
        currentcohorts = table1$modified_string[i]
        dbExecute(con, "DROP TABLE IF EXISTS temporaryrow;")
        query <- paste0("

        CREATE TEMP TABLE temporaryrow AS
        SELECT DISTINCT subject_id
         FROM @cohortDatabaseSchema.@cohortTable
        WHERE cohort_definition_id IN (@cohort_ids);")


    rendered_sql <- SqlRender::render(
      sql = query,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = cohortTable,
      cohort_ids = currentcohorts
    )

    # Translate SQL to target database dialect if needed
    # Example: Translating to PostgreSQL
    translated_sql <- SqlRender::translate(
      sql = rendered_sql,
      targetDialect = dbms
    )
        # Fetch data and append to the result dataframe
        dbExecute(con, translated_sql)

       for (x in 1:8){
        query <- paste0("
        SELECT COUNT(DISTINCT tr.subject_id) AS distinct_subject_count
    FROM temporaryrow tr
    WHERE tr.subject_id IN (
      SELECT DISTINCT subject_id
      FROM sb_jtelford.jmt
      WHERE cohort_definition_id = ",Pop5tables$cohortId[x],");")
        temp_df <- dbGetQuery(con, query)
        print(temp_df[1,1])
        table51[i,x]= temp_df[1,1]
    }
    }
    #write.csv(table51,paste0(outputFolder,"/table51.csv"))
















    # Create table 2

    table2 <- data.frame(matrix(0, nrow = length(row_names12), ncol = 8))
    rownames(table2) <- row_names12
    colnames(table2) <- 2016:2023
    table52 = table2
    Pop2tables = cohort_information[grepl("Pop2", cohort_information$cohortName), ]

    #All years
    for (i in 1:8) {
      # Extract year from the source name
      year <- gsub(".*Pop2(\\d{4}).*", "\\1", Pop2tables$cohortName[i])
      print(year)
      # Check if the year is in the target table's column names
      if (year %in% colnames(table2)) {
        table2["All", year] <- Pop2tables$cohortId[i]
        cohort = Pop2tables$cohortId[i]
        filename = paste0("Positive",Pop2tables$cohortName[i])
        getLocationIDTable(con = con, cohorttoRun = cohort,filename,executionsettings = executionsettings)
      }
    }

    #Ages

    # Combine counts for male and female for each 'Pop' (same Pop value)
    Pop2tables$year <- gsub(".*Pop2(\\d{4}).*", "\\1", Pop2tables$cohortName)
    Pop2tables$gender <- ifelse(grepl(" male",  Pop2tables$cohortName), "male", "female")
    allgroups = c("15 - 29","30 - 44","45 - 55","Age 55+", "White","African American","Hispanic","Asian")
    group ="15 - 29"

    for (group in allgroups){
      filtered_rows <- Pop2tables[grepl(group, Pop2tables$cohortName), ]
      if (group == "Age 55+"){group = "55+"}
      aggregated_data <- aggregate(cohortId ~ year + gender, data = filtered_rows, sum)
      sum_counts <- paste(aggregated_data$cohortId[aggregated_data$year == i], collapse = " ")

      for (i in 2016:2023) {
        sum_counts <- paste(aggregated_data$cohortId[aggregated_data$year == i], collapse = " ")
        # Ensure that the column (year i) exists in table2
        if (as.character(i) %in% colnames(table2)) {
          # Assign the sum to the appropriate row and column in table2
          table2[group, as.character(i)] <- sum_counts
        } else {
          # Handle the case where the column does not exist
          cat("Column", i, "not found in table2")
        }
      }
    }

    agetables = Pop2tables[grepl("Age", Pop2tables$cohortName), ]
    for (group in c("male","female")){
      filtered_rows <- agetables[agetables$gender == group, ]

      for (i in 2016:2023) {
        sum_counts <- paste(filtered_rows$cohortId[filtered_rows$year == i], collapse = " ")
        # Ensure that the column (year i) exists in table1
        if (as.character(i) %in% colnames(table2)) {
          # Assign the sum to the appropriate row and column in table1
          table2[group, as.character(i)] <- sum_counts
        } else {
          # Handle the case where the column does not exist
          cat("Column", i, "not found in table1.")
        }
      }
    }

    # Manage unknowns

    write.csv(table2,paste0(outputFolder,"/table2cohortID.csv"))
    table2 <- table2[, !names(table2) %in% "modified_string"]
    table2$modified_string <- apply(table2, 1, function(row) {
      first <- paste(unique(row), collapse = " ")  # Combine unique values with a space
      modified_string <- gsub(" ", ", ", first)    # Replace spaces with commas
      return(modified_string)
    })
    # For each row
    for (i in (1:nrow(table2))){
      currentcohorts = table2$modified_string[i]
      dbExecute(con, "DROP TABLE IF EXISTS temporaryrow;")
      query <- paste0("

        CREATE TEMP TABLE temporaryrow AS
        SELECT DISTINCT subject_id
         FROM @cohortDatabaseSchema.@cohortTable
        WHERE cohort_definition_id IN (@cohort_ids);")


      rendered_sql <- SqlRender::render(
        sql = query,
        cohortDatabaseSchema = cohortDatabaseSchema,
        cohortTable = cohortTable,
        cohort_ids = currentcohorts
      )

      # Translate SQL to target database dialect if needed
      # Example: Translating to PostgreSQL
      translated_sql <- SqlRender::translate(
        sql = rendered_sql,
        targetDialect = dbms
      )
      # Fetch data and append to the result dataframe
      dbExecute(con, translated_sql)

      for (x in 1:8){
        query <- paste0("
        SELECT COUNT(DISTINCT tr.subject_id) AS distinct_subject_count
    FROM temporaryrow tr
    WHERE tr.subject_id IN (
      SELECT DISTINCT subject_id
      FROM sb_jtelford.jmt
      WHERE cohort_definition_id = ",Pop5tables$cohortId[x],");")
        temp_df <- dbGetQuery(con, query)
        print(temp_df[1,1])
        table52[i,x]= temp_df[1,1]
      }
    }
    write.csv(table52,paste0(outputFolder,"/table52.csv"))


    # Create table 3



    rownames3 <- c("All","15 - 29","30 - 44","45 - 55","Age 55+",
                   "White","African American","Hispanic","Asian",
                   "HIVTest2","AnySTI2","Syph","Chlamydia","Gonorrhea","Trichomoniasis",
                   "OtherSTI","PWID2","Other2","HIVany2","STIany2", "2HR2","3HR2",
                   "HIVTestlookback","AnySTIlookback","PWIDlookback", "Otherlookback",
                   "HIVanylookback","STIanylookback","2HRlookback", "3HRlookback"
    )
    genders = c("male","female")
    table3 <- data.frame(matrix(0, nrow = length(rownames3), ncol = 8))

    rownames(table3) <- rownames3
    colnames(table3) <- 2016:2023
    table53 = table3


    Pop3tables = cohort_information[grepl("Pop3", cohort_information$cohortName), ]
    Pop3tables$year <- ifelse(
      grepl("\\d{4}", Pop3tables$cohortName),  # Check if a 4-digit number exists
      gsub(".*?(\\d{4}).*", "\\1", Pop3tables$cohortName),  # Extract the year
      NA  # Assign NA if no year is found
    )
    Pop3tables$gender <- ifelse(
      grepl(" male", Pop3tables$cohortName), "male",
      ifelse(grepl(" female", Pop3tables$cohortName), "female", NA) # NA if neither found
    )
    #Start with male
    currentgender = "male"
    for (currentgender in genders){
      Pop3subset = subset(Pop3tables, Pop3tables$gender ==  currentgender)
      #All years
      for (currentyear in 2016:2023) {
        # Extract year from the source name
        currentyear = as.character(currentyear)
      print(currentyear)
        cohortname <- paste0("[HIV] Pop3Template",currentyear," - ",currentgender," patients in cohort 3 ",currentgender,"s")
        # Check if the year is in the target table's column names
      print((Pop3subset$count[Pop3subset$cohortName == cohortname]))
         # table3["All", year] <- Pop3subset$count[Pop3subset$name == cohortname]
        table3["All", currentyear] = (Pop3subset$cohortId[Pop3subset$cohortName == cohortname])
        cohort = Pop3subset$cohortId[Pop3subset$cohortName == cohortname]
        filename = paste0("Positive",Pop3subset$cohortName[Pop3subset$cohortName == cohortname])
        getLocationIDTable(con = con, cohorttoRun = cohort,filename,executionsettings = executionsettings)
      }

      #Ages

      # Combine counts for male and female for each 'Pop' (same Pop value)

      allgroups = c("15 - 29","30 - 44","45 - 55","Age 55+",
                    "White","African American","Hispanic","Asian",
                    "HIVTest2","AnySTI2","Syph","Chlamydia","Gonorrhea","Trichomoniasis",
                    "OtherSTI","PWID2","Other2","HIVany2","STIany2", "2HR2","3HR2",
                    "HIVTestlookback","AnySTIlookback","PWIDlookback", "Otherlookback",
                    "HIVanylookback","STIanylookback","2HRlookback", "3HRlookback"
      )


      for (group in allgroups){
        filtered_rows <- Pop3subset[grepl(group, Pop3subset$cohortName), ]
       # if (group == "Age 55+"){group = "55+"}

        for (i in 2016:2023) {
          sum_counts <- (filtered_rows$cohortId[filtered_rows$year == i])
          # Ensure that the column (year i) exists in table3
          if (as.character(i) %in% colnames(table3)) {
            # Assign the sum to the appropriate row and column in table3
            table3[group, as.character(i)] <- sum_counts
          } else {
            # Handle the case where the column does not exist
            cat("Column", i, "not found in table3")
          }
        }
      }

      # Manage unknowns

      write.csv(table3,paste0(outputFolder,"/table3",currentgender,"cohortID.csv"))
      table3 <- table3[, !names(table3) %in% "modified_string"]
      table3$modified_string <- apply(table3, 1, function(row) {
        first <- paste(unique(row), collapse = " ")  # Combine unique values with a space
        modified_string <- gsub(" ", ", ", first)    # Replace spaces with commas
        return(modified_string)
      })
      # For each row
      for (i in (1:nrow(table3))){
        currentcohorts = table3$modified_string[i]
        dbExecute(con, "DROP TABLE IF EXISTS temporaryrow;")
        query <- paste0("

        CREATE TEMP TABLE temporaryrow AS
        SELECT DISTINCT subject_id
         FROM @cohortDatabaseSchema.@cohortTable
        WHERE cohort_definition_id IN (@cohort_ids);")


        rendered_sql <- SqlRender::render(
          sql = query,
          cohortDatabaseSchema = cohortDatabaseSchema,
          cohortTable = cohortTable,
          cohort_ids = currentcohorts
        )

        # Translate SQL to target database dialect if needed
        # Example: Translating to PostgreSQL
        translated_sql <- SqlRender::translate(
          sql = rendered_sql,
          targetDialect = dbms
        )
        # Fetch data and append to the result dataframe
        dbExecute(con, translated_sql)

        for (x in 1:8){
          query <- paste0("
        SELECT COUNT(DISTINCT tr.subject_id) AS distinct_subject_count
    FROM temporaryrow tr
    WHERE tr.subject_id IN (
      SELECT DISTINCT subject_id
      FROM sb_jtelford.jmt
      WHERE cohort_definition_id = ",Pop5tables$cohortId[x],");")
          temp_df <- dbGetQuery(con, query)
          print(temp_df[1,1])
          table53[i,x]= temp_df[1,1]
        }
      }
      #write.csv(table53,paste0(outputFolder,"/table53",currentgender,".csv"))



    }


    # Write Table4


    rownames4 <- c("All","15 - 29","30 - 44","45 - 55","Age 55+",
                   "White","African American","Hispanic","Asian",
                   "HIVTest2","anySTI2","Syph","Chlamydia","Gonorrhea","Trichomoniasis",
                   "OtherSTI","PWID2","Other2","HIVany2","STIany2", "2HR2","3HR2"
    )
    table4 <- data.frame(matrix(0, nrow = length(rownames4), ncol = 8))

    rownames(table4) <- rownames4
    colnames(table4) <- 2016:2023
    table54 = table4


    Pop4tables = cohort_information[grepl("Pop4", cohort_information$cohortName), ]
    Pop4tables$year <- ifelse(
      grepl("\\d{4}", Pop4tables$cohortName),  # Check if a 4-digit number exists
      gsub(".*?(\\d{4}).*", "\\1", Pop4tables$cohortName),  # Extract the year
      NA  # Assign NA if no year is found
    )
    Pop4tables$gender <- ifelse(
      grepl(" male", Pop4tables$cohortName), "male",
      ifelse(grepl("female", Pop4tables$cohortName), "female", NA) # NA if neither found
    )
    #Start with male
    currentgender = "male"
    for (currentgender in genders){
      Pop4subset = subset(Pop4tables, Pop4tables$gender ==  currentgender)
      #All years
      for (currentyear in 2016:2023) {
        # Extract year from the source name
        currentyear = as.character(currentyear)
        print(currentyear)
        print(group)
        cohortname <- paste0("[HIV] Pop4Template",currentyear," - ",currentgender," patients in cohort 4 ",currentgender,"s")
        # Check if the year is in the target table's column names
        print((Pop4subset$count[Pop4subset$cohortName == cohortname]))
        # table4["All", year] <- Pop4subset$count[Pop4subset$name == cohortname]
        table4["All", currentyear] = (Pop4subset$cohortId[Pop4subset$cohortName == cohortname])
        cohort = Pop4subset$cohortId[Pop4subset$cohortName == cohortname]
        filename = paste0("Positive",Pop4subset$cohortName[Pop4subset$cohortName == cohortname])
        getLocationIDTable(con = con, cohorttoRun = cohort,filename,executionsettings = executionsettings)
      }

      #Ages

      # Combine counts for male and female for each 'Pop' (same Pop value)

      allgroups = c("15 - 29","30 - 44","45 - 55","Age 55+",
                    "White","African American","Hispanic","Asian",
                    "HIVTest2","anySTI2","Syph","Chlamydia","Gonorrhea","Trichomoniasis",
                    "OtherSTI","PWID2","Other2","HIVany2","STIany2", "2HR2","3HR2"
      )
      for (group in allgroups){
        filtered_rows <- Pop4subset[grepl(group, Pop4subset$cohortName), ]
        # if (group == "Age 55+"){group = "55+"}

        for (i in 2016:2023) {
          sum_counts <- (filtered_rows$cohortId[filtered_rows$year == i])
          print(sum_counts)
          # Ensure that the column (year i) exists in table4
          if (as.character(i) %in% colnames(table4)) {
            # Assign the sum to the appropriate row and column in table4
            table4[group, as.character(i)] <- sum_counts
          } else {
            # Handle the case where the column does not exist
            cat("Column", i, "not found in table4")
          }
        }
      }

      # Manage unknowns
      write.csv(table4,paste0(outputFolder,"/table4",currentgender,"cohortID.csv"))
      table4 <- table4[, !names(table4) %in% "modified_string"]
      table4$modified_string <- apply(table4, 1, function(row) {
        first <- paste(unique(row), collapse = " ")  # Combine unique values with a space
        modified_string <- gsub(" ", ", ", first)    # Replace spaces with commas
        return(modified_string)
      })
      # For each row
      for (i in (1:nrow(table4))){
        currentcohorts = table4$modified_string[i]
        dbExecute(con, "DROP TABLE IF EXISTS temporaryrow;")
        query <- paste0("

        CREATE TEMP TABLE temporaryrow AS
        SELECT DISTINCT subject_id
         FROM @cohortDatabaseSchema.@cohortTable
        WHERE cohort_definition_id IN (@cohort_ids);")


        rendered_sql <- SqlRender::render(
          sql = query,
          cohortDatabaseSchema = cohortDatabaseSchema,
          cohortTable = cohortTable,
          cohort_ids = currentcohorts
        )

        # Translate SQL to target database dialect if needed
        # Example: Translating to PostgreSQL
        translated_sql <- SqlRender::translate(
          sql = rendered_sql,
          targetDialect = dbms
        )
        # Fetch data and append to the result dataframe
        dbExecute(con, translated_sql)

        for (x in 1:8){
          query <- paste0("
        SELECT COUNT(DISTINCT tr.subject_id) AS distinct_subject_count
    FROM temporaryrow tr
    WHERE tr.subject_id IN (
      SELECT DISTINCT subject_id
      FROM sb_jtelford.jmt
      WHERE cohort_definition_id = ",Pop5tables$cohortId[x],");")
          temp_df <- dbGetQuery(con, query)
          print(temp_df[1,1])
          table54[i,x]= temp_df[1,1]
        }
      }
      write.csv(table54,paste0(outputFolder,"/table54",currentgender,".csv"))


    }
}

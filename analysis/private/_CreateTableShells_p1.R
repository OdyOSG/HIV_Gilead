# Begin Building Table shells
# Pass connection details



# 2016:2023 changed from 2016:2023
# i in 1:8 changed from i in 1:8



# Get cohort counts from DB
createTableShells1 <- function(con = con, outputFolder = outputFolder){
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

  # Create table 1

  Pop1tables = cohort_information[grepl("Pop1", cohort_information$cohortName), ]

  #All years
  for (i in 1:8) {
    # Extract year from the source name
    year <- gsub(".*Pop1(\\d{4}).*", "\\1", Pop1tables$cohortName[i])
    print(Pop1tables$cohortName[i])
    # Check if the year is in the target table's column names
    if (year %in% colnames(table1)) {
      table1["All", year] <- Pop1tables$count[i]
      cohort = Pop1tables$cohortId[i]
      filename = Pop1tables$cohortName[i]
      getLocationIDTable(con = con, cohorttoRun = cohort,filename,executionsettings = executionsettings)
    }
  }

  #Ages

  # Combine counts for male and female for each 'Pop' (same Pop value)
  Pop1tables$year <- gsub(".*Pop1(\\d{4}).*", "\\1", Pop1tables$cohortName)
  Pop1tables$gender <- ifelse(grepl(" male",  Pop1tables$cohortName), "male", "female")
  allgroups = c("15 - 29","30 - 44","45 - 55","Age 55+", "White","African American","Hispanic","Asian")
  group ="Age 55+"
  for (group in allgroups){
    filtered_rows <- Pop1tables[grepl(group, Pop1tables$cohortName), ]
    if (group == "Age 55+"){group = "55+"}
    aggregated_data <- aggregate(count ~ year + gender, data = filtered_rows, sum)
    sum_counts <- sum(aggregated_data$count[aggregated_data$year == i])

    for (i in 2016:2023) {
      sum_counts <- sum(aggregated_data$count[aggregated_data$year == i])
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

  agetables = Pop1tables[grepl("Age", Pop1tables$cohortName), ]
  for (group in c("male","female")){
    filtered_rows <- agetables[agetables$gender == group, ]

    for (i in 2016:2023) {
      sum_counts <- sum(filtered_rows$count[filtered_rows$year == i])
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
  table1["Unknown age",] = table1["All",] - table1["15 - 29",] - table1["30 - 44",] - table1["45 - 55",] - table1["55+",]

  table1["Unknown Sex",] = table1["All",] - table1["male",] - table1["female",]
  table1["Other or Unknown",] = table1["All",] - table1["White",] - table1["African American",] - table1["Hispanic",] - table1["Asian",]
  write.csv(table1,paste0(outputFolder,"/table1.csv"))


  # Create table 2

  table2 <- data.frame(matrix(0, nrow = length(row_names12), ncol = 8))
  rownames(table2) <- row_names12
  colnames(table2) <- 2016:2023
  Pop2tables = cohort_information[grepl("Pop2", cohort_information$cohortName), ]

  #All years
  for (i in 1:8) {
    # Extract year from the source name
    year <- gsub(".*Pop2(\\d{4}).*", "\\1", Pop2tables$cohortName[i])
    print(year)
    # Check if the year is in the target table's column names
    if (year %in% colnames(table2)) {
      table2["All", year] <- Pop2tables$count[i]
      cohort = Pop2tables$cohortId[i]
      filename = Pop2tables$cohortName[i]
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
    aggregated_data <- aggregate(count ~ year + gender, data = filtered_rows, sum)
    sum_counts <- sum(aggregated_data$count[aggregated_data$year == i])

    for (i in 2016:2023) {
      sum_counts <- sum(aggregated_data$count[aggregated_data$year == i])
      # Ensure that the column (year i) exists in table2
      if (as.character(i) %in% colnames(table1)) {
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
      sum_counts <- sum(filtered_rows$count[filtered_rows$year == i])
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

  # Manage unknowns
  table2["Unknown age",] = table2["All",] - table2["15 - 29",] - table2["30 - 44",] - table2["45 - 55",] - table2["55+",]

  table2["Unknown Sex",] = table2["All",] - table2["male",] - table2["female",]
  table2["Other or Unknown",] = table2["All",] - table2["White",] - table2["African American",] - table2["Hispanic",] - table2["Asian",]

  write.csv(table2,paste0(outputFolder,"/table2.csv"))



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
      table3["All", currentyear] = (Pop3subset$count[Pop3subset$cohortName == cohortname])
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
        sum_counts <- (filtered_rows$count[filtered_rows$year == i])
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
    table3["Unknown age",] = table3["All",] - table3["15 - 29",] - table3["30 - 44",] - table3["45 - 55",] - table3["55+",]
    table3["Other or Unknown",] = table3["All",] - table3["White",] - table3["African American",] - table3["Hispanic",] - table3["Asian",]
    write.csv(table3,paste0(outputFolder,"/table3",currentgender,".csv"))
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
      table4["All", currentyear] = (Pop4subset$count[Pop4subset$cohortName == cohortname])
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
        sum_counts <- (filtered_rows$count[filtered_rows$year == i])
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
    table4["Unknown age",] = table4["All",] - table4["15 - 29",] - table4["30 - 44",] - table4["45 - 55",] - table4["55+",]
    table4["Other or Unknown",] = table4["All",] - table4["White",] - table4["African American",] - table4["Hispanic",] - table4["Asian",]
    write.csv(table4,paste0(outputFolder,"/table4",currentgender,".csv"))
  }
}

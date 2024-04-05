
# Read the custom label mappings
label_mappings <- read_csv("documents/labels_mapping.csv")


#function: load and combine parquet files
load_and_combine_parquet_files <- function(dir_path) {
  # Get a list of all Parquet files
  parquet_files <- list.files(dir_path, pattern = "\\.parquet$", full.names = TRUE)
  
  # Check if files are found
  if (length(parquet_files) == 0) {
    stop("No Parquet files found in the specified directory.")
  } else {
    message("Found the following Parquet files:")
    print(parquet_files)
  }
  
  # Read and combine into a single dataframe
  all_data <- map_dfr(parquet_files, read_parquet)
  
  # Optional: Print the first few rows and summary of the combined data
  print(head(all_data))
  print(summary(all_data))
  print("Data loaded successfully.")
  
  return(all_data)
}


#function: custom label
apply_custom_labels <- function(sampling_point, mappings) {
  custom_label <- "Other" # Default label
  for(i in 1:nrow(mappings)) {
    if(grepl(mappings$Samplingpoint[i], sampling_point)) {
      custom_label <- mappings$CustomLabel[i]
      break
    }
  }
  return(custom_label)
}

# Vectorize the apply_custom_labels function for efficient use with vectors of sampling points
vectorized_labeling <- Vectorize(apply_custom_labels, vectorize.args = "sampling_point")




#function: seed out invalid dataentries
filter_invalid_measurements <- function(data) {
  
  # Filter out invalid measurements and store them in a separate data frame
  invalid_measurements <- data %>%
    filter(!Validity %in% c(1, 2, 3))
  
  # Print out the number of invalid measurements found, if any
  if(nrow(invalid_measurements) > 0) {
    message("Invalid Measurements Found:")
    print(nrow(invalid_measurements))
    # Optionally, you can print the invalid measurements themselves
    print(head(invalid_measurements))
  } else {
    message("No invalid measurements found.")
  }
  
  # Filter the dataset to keep only valid measurements
  valid_data <- data %>%
    filter(Validity %in% c(1, 2, 3))
  
  # Print the number of rows before and after filtering
  message(paste("Before filtering invalid measurements:", nrow(data)))
  message(paste("After filtering invalid measurements:", nrow(valid_data)))
  
  # Return the filtered dataset
  return(valid_data)
}



#function: analyze exceedances
analyze_exceedances <- function(data, threshold, aggregation = "daily") {
  print("Starting analysis...")
  
  # Convert 'End' column to POSIXct, ensuring correct datetime format
  data$End <- as.POSIXct(data$End)
  print("Datetime conversion completed.")
  
  # Adjust for aggregation type: daily or 8-hourly
  if(aggregation == "8hourly") {
    data <- data %>%
      mutate(Date = as.Date(End),
             ValueExceeds = Avg_8hr > threshold)
    print("Data filtered for 8-hourly exceedances.")
  } else { # Default to daily
    data <- data %>%
      mutate(Date = as.Date(End),
             ValueExceeds = Value > threshold)
    print("Data filtered for daily exceedances.")
  }
  
  # Checking the first few rows after filtering
  print(head(data))
  
  # Total exceedances per month across all years
  total_exceedances <- data %>%
    group_by(Month = floor_date(Date, "month")) %>%
    summarize(TotalExceedCount = sum(ValueExceeds, na.rm = TRUE)) %>%
    ungroup()
  
  print("Calculated total monthly exceedances.")
  
  # Plot total monthly exceedances
  p1 <- ggplot(total_exceedances, aes(x = Month, y = TotalExceedCount)) +
    geom_line() + geom_point() +
    labs(title = paste("Total Monthly Exceedances -", aggregation), x = "Date", y = "Total Exceedances") +
    theme_minimal()
  
  print("Total monthly exceedances plot prepared.")
  
  days_exceeded <- data %>%
    mutate(Date = as.Date(End), 
           MonthOnly = format(Date, "%m"), 
           MonthName = month.abb[as.numeric(MonthOnly)]) %>%
    group_by(Date) %>%
    summarize(DayExceeded = any(ValueExceeds), .groups = 'drop') %>%
    ungroup() %>%
    mutate(Month = floor_date(Date, "month")) %>%
    group_by(Month) %>%
    summarize(TotalExceedanceDays = sum(DayExceeded), .groups = 'drop') %>%
    ungroup() %>%
    mutate(MonthOnly = as.numeric(format(Month, "%m")), # Ensure MonthOnly is numeric for sorting
           MonthName = month.abb[MonthOnly]) %>%
    group_by(MonthName) %>%
    summarize(TotalExceedanceDays = sum(TotalExceedanceDays), .groups = 'drop') %>%
    mutate(MonthName = factor(MonthName, levels = month.abb)) # Ensure correct order by month abbreviations
  
  print("Calculated total exceedance days per month.")
  
  # Plot total exceedance days per month with months in chronological order
  p2 <- ggplot(days_exceeded, aes(x = MonthName, y = TotalExceedanceDays)) +
    geom_bar(stat = "identity", fill = "coral") +
    labs(title = "Total Exceedance Days per Month", x = "Month", y = "Total Exceedance Days") +
    theme_minimal()
  
  print("Total exceedance days per month plot prepared.")
  
  print("Total exceedance days per month plot prepared.")
  
  # Display plots
  print("Displaying plots...")
  print(p1)
  print(p2)
}
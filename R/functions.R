
# Read the custom label mappings
label_mappings <- read_csv("documents/labels_mapping.csv")

# Load the timeframes and colors
  timeframes_colors <- read_csv("documents/timeframes_colors.csv")
# Convert dates from character to Date type
timeframes_colors$start <- as.Date(timeframes_colors$start)
timeframes_colors$end <- as.Date(timeframes_colors$end)

timeframes_colors <- timeframes_colors %>%
  mutate(start_year = year(start),
         end_year = year(end))
# Order by the start column
timeframes_colors <- timeframes_colors %>%
  arrange(start)

# Ensure the 'label' column is a factor with levels in the desired order
timeframes_colors <- timeframes_colors %>%
  mutate(label = factor(label, levels = label))


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
  print("Summary:")
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
  
  print("Filtering invalid measurements...")
  
  # Filter out invalid measurements and store them in a separate data frame
  invalid_measurements <- data %>%
    filter(!Validity %in% c(1, 2, 3))
  
  # Print out the number of invalid measurements found, if any
  if(nrow(invalid_measurements) > 0) {
    message("Invalid Measurements Found:")
    print(nrow(invalid_measurements))
    print(summary(invalid_measurements))
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


# Function to plot the number of measuring stations with at least one valid measurement over the years
plot_measurement_stations <- function(data, pollutant_name) {
  # Count the number of unique sampling points with at least one valid measurement per year
  yearly_station_counts <- data %>%
    mutate(Year = year(Start)) %>%
    group_by(Year) %>%
    summarize(StationCount = n_distinct(Samplingpoint), .groups = 'drop')
  
  # Ensure all years are represented, even those with 0 stations
  all_years <- data.frame(Year = seq(min(yearly_station_counts$Year), max(yearly_station_counts$Year)))
  yearly_station_counts <- full_join(all_years, yearly_station_counts, by = "Year")
  yearly_station_counts$StationCount[is.na(yearly_station_counts$StationCount)] <- 0
  
  # Plot the yearly station counts
  plot <- ggplot(yearly_station_counts, aes(x = Year, y = StationCount)) +
    geom_line() +
    geom_point() +
    theme_minimal() +
    labs(title = paste("Anzahl der Messtationen für", pollutant_name, "mit mindestens einem validen Messwert pro Jahr in Wien"),
         x = "Jahr",
         y = "Anzahl Stationen") +
    theme(legend.position = "none") +
    ylim(0, 20)
  
  # Return the plot
  return(plot)
}









#function: analyze exceedance days
analyze_exceedance_days <- function(data, threshold, pollutant_name, aggregation = "daily", specific_month = 7) {
  
  print("Starting analysis...")
  

  # Adjust based on the type of aggregation: daily or 8-hourly
  if(aggregation == "8hourly") {
    # First, ensure 'End' is in Date format for daily grouping
    data <- data %>%
      mutate(Date = as.Date(End)) %>%
      filter(year(Date) <= 2023)  # Filter out dates from 2024 onward
    print("Data filtered to exclude entries from 2024 onward.")
    # Group by 'Date' to process data day by day
    data <- data %>%
      group_by(Date) %>%
      # Determine if at least one 8-hour average exceeds the threshold for each day
      summarize(ValueExceeds = any(Avg_8hr > threshold, na.rm = TRUE), .groups = 'drop')
    
    print("Aggregation completed for 8-hourly exceedances. Checked if at least one 8-hour average per day exceeds the threshold.")
  }
  if (aggregation == "daily") {
    # First, ensure 'Start' is in Date format for daily grouping
    data <- data %>%
      mutate(Date = as.Date(Start)) %>%
      filter(year(Date) <= 2023) %>% # Filter out dates from 2024 onward
      # Group by 'Date' to process data day by day
      group_by(Date) %>%
      # Determine if at least one value exceeds the threshold for each day
      summarize(ValueExceeds = any(Value > threshold, na.rm = TRUE), .groups = 'drop')
    
    print("Data filtered to exclude entries from 2024 onward.")
    
    print("Data filtered for daily exceedances. Checking if daily values exceed the threshold.")
  }
  
  
  
  # Create a summary to identify days with at least one exceedance
  daily_summary <- data %>%
    group_by(Date) %>%
    summarize(AnyExceedance = max(ValueExceeds), .groups = 'drop') %>%
    mutate(Year = year(Date), Month = month(Date), MonthName = month.abb[Month])
  print("Created a daily summary indicating days with any exceedance.")
  
  print(summary(daily_summary))
  
  
  # Count exceedance days per month
  total_exceedance_days_per_month <- daily_summary %>%
    # Group by month
    group_by(Year, Month) %>%
    # Sum the exceedance days for each month
    summarize(TotalExceedDays = sum(AnyExceedance, na.rm = TRUE)) %>%
    # Remove the grouping structure
    ungroup() %>%
    # Creating a Date variable for plotting
    mutate(Date = make_date(Year, Month, 1))
  
  print("Calculated total monthly exceedances.")
  
  
  print(summary(total_exceedance_days_per_month))
  
  
  # Plot total monthly exceedances
  p1 <- ggplot(total_exceedance_days_per_month, aes(x = Date, y = TotalExceedDays)) +
    geom_line() + geom_point() +
    labs(title = paste("Total Monthly Exceedances of", pollutant_name, "with threshold", threshold), x = "Date", y = "Number of Exceedance Days") +
    theme_minimal()
  
  print("Total monthly exceedances plot prepared.")
  
  
  
  # Data preparation with explanatory print statements
  print("Starting data preparation for plotting...")
  
  monthly_yearly_exceedances <- daily_summary %>%
    mutate(
      Year = year(Date),
      Month = month(Date),
      MonthName = month.abb[Month] # Abbreviated month name for readability in the plot
    ) %>%
    group_by(Year, Month, MonthName) %>%
    summarize(AnyExceedance = sum(AnyExceedance, na.rm = TRUE), .groups = 'drop') %>%
    ungroup()
  
  print("Days with exceedances counted per month, per year.")
  
  avg_monthly_exceedances <- monthly_yearly_exceedances %>%
    group_by(MonthName) %>%
    summarize(AvgExceedanceDays = mean(AnyExceedance), .groups = 'drop') %>%
    ungroup() %>%
    mutate(MonthName = factor(MonthName, levels = month.abb)) # Ensure proper month order for plotting
  
  print("Average exceedance days per month across all years calculated.")
  
  # Verifying the structure and summary of the prepared data
  print("Preview of the prepared daily summary for plotting:")
  print(head(daily_summary))
  
  # Plot creation with a print statement to indicate the process
  print("Creating plot to visualize the average exceedance days per month across all years...")
  
  p2 <- ggplot(avg_monthly_exceedances, aes(x = MonthName, y = AvgExceedanceDays)) +
    geom_bar(stat = "identity", fill = "darkgreen", width = 0.7, alpha = 0.8) +
    labs(title = paste("Durchschnittliche Anzahl der Tage mit mindestens\neiner gemessenen Überschreitung des Schwellenwerts", threshold, "von", pollutant_name, "\npro Monat im betrachteten Zeitraum in Wien"),
         x = "Monat", y = "Durchschnittliche Anzahl der Tage") +
    theme_minimal() +
    theme(plot.title = element_text(hjust = 0.5),
          plot.margin = margin(t = 30, r = 30, b = 30, l = 30),
          axis.text.x = element_text(angle = 45, hjust = 1)) + # Added angle to x-axis text for better readability
    ylim(0, 25)
  
  print("Total exceedance days per month plot prepared.")
  
  
  
  
  # Aggregate total yearly exceedances
  
  print("Calculating total yearly exceedances.")
  
  total_exceedances_per_year <- daily_summary %>%
    group_by(Year) %>%
    summarize(TotalExceedDays = sum(AnyExceedance, na.rm = TRUE)) %>%
    ungroup()
  
  print("Calculated total yearly exceedances.")
  
  print("Calculating linear model...")
  
  # Fit a linear model
  model <- lm(TotalExceedDays ~ Year, data = total_exceedances_per_year)

  print("Preparing Total yearly exceedances plot")
  
  # Plot total yearly exceedances
  p3 <- ggplot(total_exceedances_per_year, aes(x = Year, y = TotalExceedDays)) +
    geom_rect(data = timeframes_colors, inherit.aes = FALSE,
              aes(xmin = start_year, xmax = end_year, ymin = -Inf, ymax = Inf, fill = label),
              alpha = 0.3) +
    geom_line(size = 1) + 
    geom_point(size = 3) +
    geom_abline(intercept = coef(model)[1], slope = coef(model)[2], color = "red", size = 1) +
    labs(title = paste("Anzahl der Tage mit mindestens einer gemessenen\nÜberschreitungen des Schwellenwerts", threshold,"von", pollutant_name, "in Wien pro Jahr"),
         x = "Jahr", 
         y = "Tage",
         fill = "Zeitreihen") +
    scale_fill_manual(values = setNames(timeframes_colors$color, timeframes_colors$label)) +
    theme_minimal() +
    theme(plot.title = element_text(hjust = 0.5),
          legend.position = "bottom", # Position legend below the plot
          legend.direction = "horizontal", # Arrange legend items horizontally
          legend.box = "horizontal",
          plot.margin = margin(t = 30, r = 30, b = 30, l = 30)) +
    ylim(0, 300)
  
  
  print("Total yearly exceedances plot prepared.")
  
  
  
  
  
  # Display plots
  #print("Displaying plots...")
  #print("Displaying the total monthly exceedances plot...")
  #print(p1)
  print("Displaying the average monthly exceedances plot...")
  print(p2)
  print("Displaying the yearly exceedances plot...")
  print(p3)
  
  
  return(list(exceedance_plot = p3,
              model = model,
              exceedance_data = total_exceedances_per_year,
              monthly_exceedance_plot = p2))
  
}



#NEW FOR POSTER
combine_exceedance_plots <- function(exceedance_results, pollutant_name, thresholds) {
  if (is.null(exceedance_results) || length(exceedance_results) == 0) {
    stop("Exceedance results cannot be NULL or empty.")
  }
  
  combined_data <- bind_rows(lapply(seq_along(exceedance_results), function(i) {
    result <- exceedance_results[[i]]$exceedance_data
    if (is.null(result)) {
      stop(paste("Exceedance data at index", i, "is NULL."))
    }
    result$Threshold <- thresholds[i]
    return(result)
  }))
  
  print("Combined data created:")
  print(head(combined_data))
  
  # Fit linear models for each threshold
  models <- lapply(seq_along(thresholds), function(i) {
    model_data <- combined_data[combined_data$Threshold == thresholds[i], ]
    if (nrow(model_data) == 0) {
      stop(paste("No data available for threshold", thresholds[i]))
    }
    lm(TotalExceedDays ~ Year, data = model_data)
  })
  
  # Define the colors for the thresholds
  threshold_colors <- setNames(c("#038ac2", "#00008B"), thresholds) # Light blue for WHO, Dark blue for EU
  
  # Plotting
  plot <- ggplot(combined_data, aes(x = Year, y = TotalExceedDays, color = as.factor(Threshold))) +
    geom_line(size = 1.5) +
    geom_point(size = 3) +
    labs(title = paste("Überschreitungstage von", pollutant_name, "in Wien"),
         x = "Jahr", y = "Jährliche Anzahl der Überschreitungen", color = "Schwellenwert") +
    theme_minimal() +
    theme(
      plot.title = element_text(size = 24, face = "bold", family = "Arial",, hjust = 0.5),
      axis.title.x = element_text(size = 20, face = "bold", family = "Arial"),
      axis.title.y = element_text(size = 18, face = "bold", family = "Arial"),
      axis.text = element_text(size = 16, family = "Arial"),
      legend.title = element_text(size = 18, family = "Arial"),
      legend.text = element_text(size = 20, family = "Arial"),
      legend.position = "bottom", 
      legend.box = "horizontal",
      panel.grid.major = element_line(size = 0.5, linetype = 'dashed', color = 'gray'),
      panel.grid.minor = element_blank()
    ) +
    scale_color_manual(values = threshold_colors)
  
  # Adding trend lines
  for (i in seq_along(models)) {
    model <- models[[i]]
    intercept <- coef(model)[1]
    slope <- coef(model)[2]
    threshold <- as.character(thresholds[i])
    plot <- plot + 
      geom_abline(intercept = intercept, slope = slope, color = threshold_colors[threshold], linetype = "dashed", size = 1)
  }
  
  # Print the plot
  print("Displaying the yearly exceedances plot...")
  print(ggplotly(plot))
  
  # Save the plot
  plot_filename <- paste0("combined_exceedance_plot_", pollutant_name, ".png")
  plot_save_path <- file.path("results", plot_filename)
  ggsave(plot_save_path, plot = plot, width = 10, height = 10, dpi = 300)
  
  cat("Combined exceedance plot saved to:", plot_save_path, "\n")
}



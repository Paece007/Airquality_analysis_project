# Load Libraries -----------------------------------------------------------
library(arrow)
library(purrr)
library(dplyr)
library(fs)
library(lubridate)
library(ggplot2)
library(readr)
library(plotly)


# Read from other files ---------------------------------------------------

#source functions file
source("R/functions.R")

# Read the custom label mappings
label_mappings <- read_csv("documents/labels_mapping.csv")

# Set Variables -----------------------------------------------------------



# Set threshold value
threshold_value <- 15

# Path to your files
dir_path <- "data/Vienna_PM2.5/all_together"




# Split the directory path by the "/" separator
path_parts <- strsplit(dir_path, "/")[[1]]

# Extract the last two parts of the directory path
if(length(path_parts) >= 2) {
  dir_name <- paste(path_parts[(length(path_parts)-1):length(path_parts)], collapse = "_")
} else {
  dir_name <- path_parts[length(path_parts)]  # Fallback to the last part if less than 2 parts
}

# Prepend the threshold value to the directory name
dir_name <- paste0("threshold", threshold_value, "_", dir_name)

# Inserting Data ----------------------------------------------------------

all_data <- load_and_combine_parquet_files(dir_path)

# Filter out invalid measurements -----------------------------------------

valid_data <- filter_invalid_measurements(all_data)

print("From now on only valid data is used.")

# Print the different sampling points
unique_sampling_points <- unique(all_data$Samplingpoint)
print("All of the different sampling stations:")
print(unique_sampling_points)

# Check AggTypes for Daily ----------------------------------------------------------

# Step 1: Check if only daily data is present
# Get all the different AggTypes
unique_AggTypes <- unique(valid_data$AggType)

print(paste0("Unique AggType in data: ", unique_AggTypes))

# Check if the only unique AggType is "day"
if (length(unique_AggTypes) == 1 && unique_AggTypes == "day") {
  print("AggType 'day' is the only aggregation type present.")
}

if (length(unique_AggTypes) > 1) {
  print("Multiple AggTypes present in the data.")
}



# Step 2: Pre-process Hourly Data to Daily Averages
if("hour" %in% unique(valid_data$AggType)) {
  # Process hourly data
  hourly_data <- valid_data %>%
    filter(AggType == "hour") %>%
    mutate(Date = as.Date(Start),
           # Adjust Start for the beginning of the aggregation period
           Start = make_datetime(year(Date), month(Date), day(Date), 1),
           # Adjust End for the end of the aggregation period
           End = make_datetime(year(Date), month(Date), day(Date), 1) + days(1)) %>%
    group_by(Date, Samplingpoint) %>%
    summarize(Start = first(Start),  # Keep Start as adjusted for daily aggregation
              End = first(End),  # Keep End as adjusted for daily aggregation
              Value = mean(Value, na.rm = TRUE),  # Calculate daily average
              AggType = "day",  # Change AggType to 'day'
              .groups = 'drop') %>%
    select(-Date)  # Remove temporary Date column
  
  print("Hourly data aggregated to daily averages. (Not yet integrated in the dataset)")
  print(summary(hourly_data))
  
  # Filter out the hourly data from the original dataset
  daily_data <- valid_data %>%
    filter(AggType == "day")
  
  # Combine daily and converted hourly data
  combined_data <- bind_rows(daily_data, hourly_data)
  print("Combined data of daily averages and original daily data.")
  
} else {
  # If no hourly data, proceed with original dataset
  combined_data <- valid_data
  print("No hourly data present. Proceeding with original dataset.")
}

print(paste0("After procedure: Unique AggTypes in the combined data: ", unique(combined_data$AggType)))

# Label the Samplingpoints ------------------------------------------------

# Function to apply custom labels based on the mapping
apply_custom_labels <- function(sampling_point, mappings)

# Use vectorization for efficiency
vectorized_labeling <- Vectorize(apply_custom_labels, vectorize.args = "sampling_point")

print(vectorized_labeling(unique_sampling_points, label_mappings))
print("Custom labels applied.")

# Measuring Stations Plot -------------------------------------------------

# Call the function for plotting measurement stations over time
measurement_stations_plot <- plot_measurement_stations(valid_data, "PM2.5")

# Print the plot
print(ggplotly(measurement_stations_plot))

# Save the plot as a PNG file
plot_filename <- paste0("measuring_stations_", dir_name, ".png")
plot_save_path <- file.path("results", plot_filename)

# Use ggsave() to save the plot
ggsave(plot_save_path, plot = measurement_stations_plot, width = 10, height = 6, dpi = 300)

# Inform the user
cat("Measuring stations plot saved to:", plot_save_path, "\n")


# Analysing Data by sampling point (NOT ACTIVATED)  ----------------------------------------------------------

# # Main analysis with custom labeling
# analysis_results <- combined_data %>%
#   mutate(Month = floor_date(as.Date(Start), "month"),
#          ValueExceeds = Value > threshold_value) %>%
#   group_by(Month, Samplingpoint) %>%
#   summarize(ExceedCount = sum(ValueExceeds, na.rm = TRUE), .groups = 'drop') %>%
#   mutate(CustomLabel = vectorized_labeling(Samplingpoint, label_mappings))
# 
# # Filter for "Other" labeled sampling points and print them
# sampling_points_labeled_other <- analysis_results %>%
#   filter(CustomLabel == "Other") %>%
#   select(Samplingpoint) %>%
#   distinct()
# 
# if(nrow(sampling_points_labeled_other) > 0) {
#   print("Sampling Points Labeled as 'Other':")
#   print(sampling_points_labeled_other)
# }
#   
# # Print and Save the Analysis Results
# print("Analysis Results:")
# print(analysis_results)
# 
# print(summary(analysis_results$ExceedCount))
# print(summary(analysis_results$Month))


# Save results (NOT ACTIVATED) ---------------------------------------------------------------

# # Create a filename with the directory name included
# filename <- paste0("analysis_results_", dir_name, ".csv")
# 
# # Full path where the file will be saved
# save_path <- file.path("results", filename)
# 
# 
# # Save the analysis results as a CSV file
# write_csv(analysis_results, save_path)
# 
# # Inform the user
# cat("Analysis results saved to:", save_path, "\n")

# Plotting Data (NOT ACITVATED) -----------------------------------------------------------


# all_time_plot <- ggplot(analysis_results, aes(x = Month, y = ExceedCount, color = CustomLabel)) +
#   geom_line() +
#   geom_point() +
#   theme_minimal() +
#   labs(title = "Monthly Exceedance Counts by Sampling Point (PM2.5)",
#        x = "Month",
#        y = "Exceedance Count",
#        color = "Sampling Point") +
#   theme(legend.position = "bottom",
#         legend.title.align = 0.5)
# 
# 
# # Convert ggplot object to plotly object
# all_time_plotly <- ggplotly(all_time_plot)
# 
# # Display the plot
# all_time_plotly


# Save Plot (NOT ACTIVATED)---------------------------------------------------------------

# # Save the plot as a PNG file
# plot_filename <- paste0("exceedance_counts_plot_", dir_name, ".png")
# plot_save_path <- file.path("results", plot_filename)
# 
# # Use ggsave() to save the plot
# ggsave(plot_save_path, plot = all_time_plot, width = 10, height = 6, dpi = 300)
# 
# # Inform the user
# cat("Plot saved to:", plot_save_path, "\n")

# Plotting Data for a Specific Year (NOT ACTIVATED) ----------------------------------------

# # Define the plotting function
# plot_year_data <- function(data, target_year) {
#   # Filter the dataset for the specified year
#   filtered_data <- data %>%
#     filter(year(Month) == target_year)
# 
#   # Plotting for the specified year
#   plot <- ggplot(filtered_data, aes(x = Month, y = ExceedCount, color = CustomLabel)) +
#     geom_line() +
#     geom_point() +
#     theme_minimal() +
#     labs(title = paste("Monthly Exceedance Counts by Sampling Point in", target_year),
#          x = "Month",
#          y = "Exceedance Count",
#          color = "Sampling Point") +
#     theme(legend.position = "bottom",
#           legend.title.align = 0.5)
# 
#   # Return the plot
#   return(plot)
# }
# 
# 
# 
# # Call the function for the year 2013
# plot <- plot_year_data(analysis_results, 2023)
# 
# # Print the plot
# print(ggplotly(plot))




# Only Data from 2004 onwards ---------------------------------------------

# Filter data to include only entries from 2004 onward
combined_data <- combined_data %>%
  filter(year(Start) >= 2004)

print("From now on, only data from 2004 onward is used.")


# Exceedances Days Plots  --------------------------------------

results_exceedance_days <- analyze_exceedance_days(combined_data, threshold_value,"PM2.5" ,"daily")


exceedance_plot <- results_exceedance_days$exceedance_plot
linear_model <- results_exceedance_days$model
monthly_exceedance_plot <- results_exceedance_days$monthly_exceedance_plot


# Saving ------------------------------------------------------------------

# Save the plot as a PNG file
plot_filename <- paste0("exceedance_plot_", dir_name, ".png")
plot_save_path <- file.path("results", plot_filename)

# Use ggsave() to save the plot
ggsave(plot_save_path, plot = exceedance_plot, width = 8, height = 6, dpi = 300)

# Inform the user
cat("Plot saved to:", plot_save_path, "\n")

#Linear model

# Save the plot as a PNG file
test_filename <- paste0("lm_", dir_name, ".txt")
test_save_path <- file.path("results", test_filename)

# Assuming `model` is your fitted linear model
capture.output(summary(linear_model), file = test_save_path)

# Inform the user
cat("LM result saved to:", test_save_path, "\n")

# Save the monthly plot as png
plot_filename <- paste0("monthly_exceedance_plot_", dir_name, ".png")
plot_save_path <- file.path("results", plot_filename)

# Use ggsave() to save the plot
ggsave(plot_save_path, plot = monthly_exceedance_plot, width = 10, height = 6, dpi = 300)

# Inform the user
cat("Plot saved to:", plot_save_path, "\n")


# New for Poster ----------------------------------------------------------

threshold_values <- c(15, 25)

exceedance_results <- lapply(threshold_values, function(threshold) {
  analyze_exceedance_days(combined_data, threshold, "PM2.5", "daily")
})

# Combine and plot results
combine_exceedance_plots(exceedance_results, "PM2.5", threshold_values)


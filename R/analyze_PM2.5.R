library(arrow)
library(purrr)
library(dplyr)
library(fs)
library(lubridate)
library(ggplot2)
library(readr)


# Set Variables -----------------------------------------------------------

# Set threshold value
threshold_value <- 15

# Path to your files
dir_path <- "data/Vienna_PM2.5/Airbase"

# Inserting Data ----------------------------------------------------------

# Get a list of all Parquet files
parquet_files <- list.files(dir_path, pattern = "\\.parquet$", full.names = TRUE)

# Print statement to check if files are found
if (length(parquet_files) == 0) {
  print("No Parquet files found in the specified directory.")
} else {
  print("Found the following Parquet files:")
  print(parquet_files)
}


# Read and combine into a single dataframe
all_data <- map_dfr(parquet_files, read_parquet)


# Print the different sampling points
unique_sampling_points <- unique(all_data$Samplingpoint)
print(unique_sampling_points)

# Look at sampling points
unique_AggTypes <- unique(all_data$AggType)

# Check if the only unique AggType is "day"
if (length(unique_AggTypes) == 1 && unique_AggTypes == "day") {
  print("AggType 'day' is the only aggregation type present.")
} else {
  stop("Error: AggType 'day' is not the sole aggregation type in the dataset.")
}

# Analysing Data ----------------------------------------------------------

#EVENTUELL FÜR VERIFICATION (oder Validity) CHECKEN

# Filter out invalid measurements and print them
invalid_measurements <- all_data %>%
  filter(!Validity %in% c(1, 2, 3))

# If you want to see these invalid measurements, print them out
if(nrow(invalid_measurements) > 0) {
  print("Invalid Measurements Found:")
  print(invalid_measurements)
}

# Proceed with only valid measurements
valid_data <- all_data %>%
  filter(Validity %in% c(1, 2, 3))

# Main analysis with custom labeling
analysis_results <- valid_data %>%
  mutate(Month = floor_date(as.Date(Start), "month"),
         ValueExceeds = Value > threshold_value) %>%
  group_by(Month, Samplingpoint) %>%
  summarize(ExceedCount = sum(ValueExceeds, na.rm = TRUE), .groups = 'drop') %>%
  filter(ExceedCount > 0) %>%
  mutate(CustomLabel = case_when(
    grepl("AKC", Samplingpoint) ~ "AKH (Allgemeines Krankenhaus)",
    grepl("KEND", Samplingpoint) ~ "Kendlerstraße",
    grepl("LOB", Samplingpoint) ~ "Lobau",
    grepl("STAD", Samplingpoint) ~ "Stadlau",
    grepl("TAB", Samplingpoint) ~ "Taborstraße",
    TRUE ~ "Other"
  ))

# Filter for "Other" labeled sampling points and print them
sampling_points_labeled_other <- analysis_results %>%
  filter(CustomLabel == "Other") %>%
  select(Samplingpoint) %>%
  distinct()

if(nrow(sampling_points_labeled_other) > 0) {
  print("Sampling Points Labeled as 'Other':")
  print(sampling_points_labeled_other)
}
  
# Print and Save the Analysis Results
print("Analysis Results:")
print(analysis_results)

# Save results ---------------------------------------------------------------

# Split the directory path by the "/" separator
path_parts <- strsplit(dir_path, "/")[[1]]

# Extract the last two parts of the directory path
if(length(path_parts) >= 2) {
  dir_name <- paste(path_parts[(length(path_parts)-1):length(path_parts)], collapse = "_")
} else {
  dir_name <- path_parts[length(path_parts)]  # Fallback to the last part if less than 2 parts
}

# Create a filename with the directory name included
filename <- paste0("analysis_results_", dir_name, ".csv")

# Full path where the file will be saved
save_path <- file.path("results", filename)


# Save the analysis results as a CSV file
write_csv(analysis_results, save_path)

# Inform the user
cat("Analysis results saved to:", save_path, "\n")
# Plotting Data -----------------------------------------------------------


all_time_plot <- ggplot(analysis_results, aes(x = Month, y = ExceedCount, color = CustomLabel)) +
  geom_line() +
  geom_point() +
  theme_minimal() +
  labs(title = "Monthly Exceedance Counts by Sampling Point",
       x = "Month",
       y = "Exceedance Count",
       color = "Sampling Point") +
  theme(legend.position = "bottom",
        legend.title.align = 0.5)

# Save the plot as a PNG file
plot_filename <- paste0("exceedance_counts_plot_", dir_name, ".png")
plot_save_path <- file.path("results", plot_filename)

# Use ggsave() to save the plot
ggsave(plot_save_path, plot = all_time_plot, width = 10, height = 6, dpi = 300)

# Inform the user
cat("Plot saved to:", plot_save_path, "\n")

# Plotting Data for a Specific Year ----------------------------------------


#GPT LAST YEAR

# Define the plotting function
plot_year_data <- function(data, target_year) {
  # Filter the dataset for the specified year
  filtered_data <- data %>%
    filter(year(Month) == target_year)
  
  # Plotting for the specified year
  plot <- ggplot(filtered_data, aes(x = Month, y = ExceedCount, color = CustomLabel)) +
    geom_line() +
    geom_point() +
    theme_minimal() +
    labs(title = paste("Monthly Exceedance Counts by Sampling Point in", target_year),
         x = "Month",
         y = "Exceedance Count",
         color = "Sampling Point") +
    theme(legend.position = "bottom",
          legend.title.align = 0.5)
  
  # Return the plot
  return(plot)
}


# Example usage
last_year <- year(max(analysis_results$Month))

# Plot for the last year in the dataset
plot_year_data(analysis_results, last_year)

#year before that
plot_year_data(analysis_results, last_year - 1)
#year before that
plot_year_data(analysis_results, last_year - 2)

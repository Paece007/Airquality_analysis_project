# Load Libraries -----------------------------------------------------------

library(arrow)
library(dplyr)
library(purrr)
library(zoo)
library(readr)
library(ggplot2)
library(plotly)
library(lubridate)


# Read from other files ---------------------------------------------------

#source functions file
source("R/functions.R")

# Read the custom label mappings
label_mappings <- read_csv("documents/labels_mapping.csv")

# Set Variables -----------------------------------------------------------

# Set threshold value
threshold_value <- 120

# Path to your files
dir_path <- "data/Vienna_O3/all_together"


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

# Print the different sampling points
unique_sampling_points <- unique(all_data$Samplingpoint)
print(unique_sampling_points)

# Check AggTypes ----------------------------------------------------------

# Look at AggTypes
unique_AggTypes <- unique(all_data$AggType)

print(unique_AggTypes)

if(unique_AggTypes != "hour") {
  stop("The data is not aggregated by hour.")
}
if(length(unique_AggTypes) > 1) {
  stop("The data contains multiple aggregation types.")
}

print("AggType only hour.")


# Custom Label ------------------------------------------------------------

# Function to apply custom labels based on the mapping
apply_custom_labels <- function(sampling_point, mappings)
  
# Use vectorization for efficiency
vectorized_labeling <- Vectorize(apply_custom_labels, vectorize.args = "sampling_point")

print(vectorized_labeling(unique_sampling_points, label_mappings))
print("Custom labels applied.")

# Filter out invalid data entries ------------------------------------------

# Filter out invalid measurements and store them in a separate data frame
valid_data <- filter_invalid_measurements(all_data)


# Measuring Stations Plot -------------------------------------------------

# Call the function for plotting measurement stations over time
measurement_stations_plot <- plot_measurement_stations(valid_data, "Ozon")

# Print the plot
print(ggplotly(measurement_stations_plot))

# Save the plot as a PNG file
plot_filename <- paste0("measuring_stations_", dir_name, ".png")
plot_save_path <- file.path("results", plot_filename)

# Use ggsave() to save the plot
ggsave(plot_save_path, plot = measurement_stations_plot, width = 10, height = 6, dpi = 300)

# Inform the user
cat("Measuring stations plot saved to:", plot_save_path, "\n")

# Calculate 8hr average ---------------------------------------------------


# Define a function to calculate 8-hour averages for air quality data
calculate_8hr_averages <- function(data) {
  # Ensure the 'End' column is in POSIXct datetime format for proper sequencing
  data$End <- as.POSIXct(data$End)
  print("Converted 'End' to POSIXct datetime format.")
  
  # Sort the data by 'Samplingpoint' and 'End' to ensure chronological order within each sampling point
  sorted_data <- data %>% arrange(Samplingpoint, End)
  print(paste("Data sorted. Number of rows:", nrow(sorted_data)))
  print(head(sorted_data))
  
  # Calculate the 8-hour rolling averages within each sampling point group
  print("Calculating 8-hour averages...")
  averaged_data <- sorted_data %>%
    group_by(Samplingpoint) %>%
    mutate(Avg_8hr = rollapply(Value, width = 8, FUN = mean, by = 1, align = "right", partial = TRUE, na.rm = TRUE)) %>%
    # Remove the grouping structure for further general manipulation or analysis
    ungroup()
  
  print(paste("Calculated 8-hour averages. Number of rows with NA in Avg_8hr:", sum(is.na(averaged_data$Avg_8hr))))
  
  # Return the data frame with an added column for 8-hour averages
  return(averaged_data)
}

averaged_data <- calculate_8hr_averages(valid_data)
print(summary(averaged_data))
print(paste("8-hour averages calculated. Number of unique sampling points:", length(unique(averaged_data$Samplingpoint))))



# Analysis ----------------------------------------------------------------

# Function to analyze exceedance days of a given threshold of the 8-hour averages
results_exceedance_days <- analyze_exceedance_days(averaged_data, threshold_value, "Ozon" , "8hourly")


exceedance_plot <- results_exceedance_days$exceedance_plot
linear_model <- results_exceedance_days$model


# Saving ------------------------------------------------------------------

# Save the plot as a PNG file
plot_filename <- paste0("exceedance_plot_", dir_name, ".png")
plot_save_path <- file.path("results", plot_filename)

# Use ggsave() to save the plot
ggsave(plot_save_path, plot = exceedance_plot, width = 10, height = 6, dpi = 300)

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


# New for Poster ----------------------------------------------------------
threshold_values <- c(120, 100)

exceedance_results <- lapply(threshold_values, function(threshold) {
  analyze_exceedance_days(averaged_data, threshold, "Ozone", "8hourly")
})

# Combine and plot results
combine_exceedance_plots(exceedance_results, "Ozone", threshold_values)


# Load Libraries -----------------------------------------------------------

library(arrow)
library(dplyr)
library(purrr)
library(zoo)
library(readr)
library(ggplot2)


# Read from other files ---------------------------------------------------

#source functions file
source("R/functions.R")

# Read the custom label mappings
label_mappings <- read_csv("documents/labels_mapping.csv")

# Set Variables -----------------------------------------------------------

# Set threshold value
threshold_value <- 100

# Path to your files
dir_path <- "data/Vienna_O3/Airbase"


# Split the directory path by the "/" separator
path_parts <- strsplit(dir_path, "/")[[1]]

# Extract the last two parts of the directory path
if(length(path_parts) >= 2) {
  dir_name <- paste(path_parts[(length(path_parts)-1):length(path_parts)], collapse = "_")
} else {
  dir_name <- path_parts[length(path_parts)]  # Fallback to the last part if less than 2 parts
}


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


# Calculate 8hr average ---------------------------------------------------


# Define a function to calculate 8-hour averages for air quality data
calculate_8hr_averages <- function(data) {
  # Ensure the 'End' column is in POSIXct datetime format for proper sequencing
  data$End <- as.POSIXct(data$End)
  print("Converted 'End' to POSIXct datetime format.")
  
  # Sort the data by 'Samplingpoint' and 'End' to ensure chronological order within each sampling point
  sorted_data <- data %>% arrange(Samplingpoint, End)
  print(paste("Data sorted. Number of rows:", nrow(sorted_data)))
  
  # Calculate the 8-hour rolling averages within each sampling point group
  averaged_data <- sorted_data %>%
    group_by(Samplingpoint) %>%
    mutate(Avg_8hr = rollapply(Value, width = 8, FUN = mean, by = 1, align = "right", partial = TRUE, na.rm = TRUE)) %>%
    ungroup() # Remove the grouping structure for further general manipulation or analysis
  
  print(paste("Calculated 8-hour averages. Number of rows with NA in Avg_8hr:", sum(is.na(averaged_data$Avg_8hr))))
  
  # Return the data frame with an added column for 8-hour averages
  return(averaged_data)
}

averaged_data <- calculate_8hr_averages(valid_data)
print(paste("8-hour averages calculated. Number of unique sampling points:", length(unique(averaged_data$Samplingpoint))))



# Analysis ----------------------------------------------------------------

# Function to analyze exceedances of a given threshold of the 8-hour averages
analyze_exceedances(averaged_data, 100, "8hourly")



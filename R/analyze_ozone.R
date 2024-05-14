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
threshold_value <- 100

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
results_exceedance_days <- analyze_exceedance_days(averaged_data, threshold_value, "Ozone" , "8hourly")

exceedance_plot <- results_exceedance_days$exceedance_plot
pearson_result <- results_exceedance_days$pearson_result
spearman_result <- results_exceedance_days$spearman_result

# Saving ------------------------------------------------------------------

# Save the plot as a PNG file
plot_filename <- paste0("exceedance_plot_", dir_name, ".png")
plot_save_path <- file.path("results", plot_filename)

# Use ggsave() to save the plot
ggsave(plot_save_path, plot = exceedance_plot, width = 10, height = 6, dpi = 300)

# Inform the user
cat("Plot saved to:", plot_save_path, "\n")




# Assuming 'result' is your htest object from cor.test
create_dataframe_from_htest <- function(result) {
  data.frame(
    statistic = ifelse(!is.null(result$statistic), result$statistic, NA),
    p.value = ifelse(!is.null(result$p.value), result$p.value, NA),
    parameter = ifelse(!is.null(result$parameter), result$parameter, NA),
    estimate = ifelse(!is.null(result$estimate), result$estimate, NA),
    null.value = ifelse(!is.null(result$null.value), result$null.value, NA),
    alternative = ifelse(!is.null(result$alternative), result$alternative, NA),
    method = ifelse(!is.null(result$method), result$method, NA),
    data.name = ifelse(!is.null(result$data.name), result$data.name, NA),
    conf.int.low = ifelse(!is.null(result$conf.int), result$conf.int[1], NA),
    conf.int.high = ifelse(!is.null(result$conf.int), result$conf.int[2], NA),
    conf.level = ifelse(!is.null(result$conf.level), result$conf.level, NA)
  )
}


# Apply this function to your Pearson and Spearman results
pearson_df <- create_dataframe_from_htest(pearson_result)
spearman_df <- create_dataframe_from_htest(spearman_result)





# Save the plot as a PNG file
test_filename <- paste0("test_pearson_", dir_name, ".csv")
test_save_path <- file.path("results", test_filename)

# Assuming `test_results_exceedance_days` is a data frame of results
write.table(pearson_df, file = test_save_path, row.names = FALSE)

# Inform the user
cat("Pearson result saved to:", test_save_path, "\n")


# Save the plot as a PNG file
test_filename <- paste0("test_spearman_", dir_name, ".csv")
test_save_path <- file.path("results", test_filename)

# Assuming `test_results_exceedance_days` is a data frame of results
write.table(spearman_df, file = test_save_path, row.names = FALSE)

# Inform the user
cat("Spearman result saved to:", test_save_path, "\n")
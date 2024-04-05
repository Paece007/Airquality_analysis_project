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
dir_path <- "data/Vienna_PM2.5/E1a"




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


# Check AggTypes (TODO: IMPLEMENT FUNCTION) ----------------------------------------------------------


#FUNCTION

# Look at AggTypes
unique_AggTypes <- unique(all_data$AggType)

print(unique_AggTypes)

# Check if the only unique AggType is "day"
if (length(unique_AggTypes) == 1 && unique_AggTypes == "day") {
  print("AggType 'day' is the only aggregation type present.")
}

if (length(unique_AggTypes) > 1) {
  print("Multiple AggTypes present in the data.")
}

# Step 2: Pre-process Hourly Data to Daily Averages
if("hour" %in% unique(all_data$AggType)) {
  hourly_data <- all_data %>% 
    filter(AggType == "hour") %>%
    # Convert Start to Date format directly, avoiding temporary Date column
    mutate(Date = as.Date(Start)) %>%
    group_by(Date, Samplingpoint) %>%
    summarize(DailyAvgValue = mean(Value, na.rm = TRUE), .groups = 'drop') %>%
    # Directly use the new Date column for further processing
    mutate(AggType = "day", Start = Date) %>%
    # Now, we can safely remove the Date column as it's been merged into Start
    select(-Date)
  
  if("hour" %in% unique(all_data$AggType)) {
    # (existing aggregation code)
    print("Hourly data aggregated to daily averages.")
    print(head(hourly_data))
  }
  
  # Filter out the hourly aggregated rows from the original dataset
  daily_data <- all_data %>%
    filter(AggType == "day")
  
  # Combine daily and converted hourly data
  combined_data <- bind_rows(daily_data, hourly_data)
  
  
} else {
  # If no hourly data, proceed with original dataset
  combined_data <- all_data
}

print("Combined data:")
print(head(combined_data))


print(unique(combined_data$AggType))


# Label the Samplingpoints ------------------------------------------------

# Function to apply custom labels based on the mapping
apply_custom_labels <- function(sampling_point, mappings)

# Use vectorization for efficiency
vectorized_labeling <- Vectorize(apply_custom_labels, vectorize.args = "sampling_point")


# Filter out invalid measurements -----------------------------------------

valid_data <- filter_invalid_measurements(combined_data)

# Analysing Data BUG: ONLY GRAVI!! ----------------------------------------------------------

# Main analysis with custom labeling
analysis_results <- valid_data %>%
  mutate(Month = floor_date(as.Date(Start), "month"),
         ValueExceeds = Value > threshold_value) %>%
  group_by(Month, Samplingpoint) %>%
  summarize(ExceedCount = sum(ValueExceeds, na.rm = TRUE), .groups = 'drop') %>%
  mutate(CustomLabel = vectorized_labeling(Samplingpoint, label_mappings))

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

print(summary(analysis_results$ExceedCount))
print(summary(analysis_results$Month))


# Save results ---------------------------------------------------------------

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
  labs(title = "Monthly Exceedance Counts by Sampling Point (PM2.5)",
       x = "Month",
       y = "Exceedance Count",
       color = "Sampling Point") +
  theme(legend.position = "bottom",
        legend.title.align = 0.5)


# Convert ggplot object to plotly object
all_time_plotly <- ggplotly(all_time_plot)

# Display the plot
all_time_plotly


# Save Plot ---------------------------------------------------------------

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



# Addon: Total Exceedances per Month NEED TO CHECK!! --------------------------------------

# Function to calculate and plot total and average exceedances

#WAS PASSIERT MIT ABGESCHNITTENEN MONATEN???

analyze_exceedances(valid_data, threshold_value, "daily")






# Load the required libraries

library(arrow)
library(dplyr)
library(purrr)

# Set the path to the data

# Load the data

load_data <- function(dir_path) {
  parquet_files <- list.files(dir_path, pattern = "\\.parquet$", full.names = TRUE)
  data <- map_dfr(parquet_files, read_parquet)
  return(data)
}

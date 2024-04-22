# Data Utility For Large Scale Speech Data Processing

This repository contains scripts to process speech data in large scale. 

## Repository Structure

This repository includes a variety of scripts and directories each serving a different purpose in the workflow of the project.

### Scripts and Folders

- `benchmark/` - Contains scripts and resources to benchmark the performance of the models or algorithms developed in this project.
- `create_hf/` - Scripts related to organizing and setting up models for the Hugging Face hub.
- `data_anno/` - Tools and scripts for data annotation processes.
- `hf_download/` - Utility scripts to download datasets from the Hugging Face hub.
- `in_house_ds/` - In-house data scripts that may include cleaning, preprocessing, or dataset-specific operations.
- `inference_tool/` - Scripts for running inference with the models trained in this project.
- `libri-light/` - Related to processing or using the Libri-Light dataset.
- `logging_tool/` - Utilities for logging the output of scripts and processes.
- `post_process/` - Scripts for post-processing the outputs of models or data transformation steps.
- `youtube/` - Scripts to handle downloading and processing data from YouTube.

### Utility Scripts

- `data_check.sh` - A shell script to validate the integrity and consistency of the data used in the project.
- `data_loader_sample.py` - A Python script that serves as an example of how to load data within the project framework.
- `data_process_template.py` - A template Python script for standardizing data processing tasks.
- `data_stats.py`, `data_stats_fast.py`, `data_stats_fusion.py` - Python scripts for generating statistical summaries of the datasets.
- `get_total_hours.py` - A Python script for calculating the total hours of data or processing time.
- `mount_synology_nas.sh` - A shell script to mount Synology NAS drives, typically used for data storage and access.
- `my_dataloader.py` - Custom script for loading data in a specific way required by the project.

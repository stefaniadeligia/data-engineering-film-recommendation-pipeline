# Data Engineering Film Recommendation Pipeline

This repository contains a data engineering project that processes and transforms a dataset of films for a recommendation service. The project is implemented using Python, PySpark, Luigi, and Pytest, demonstrating skills in data transformation, pipeline automation, and testing.

## Project Overview

The goal of this project is to clean and transform a dataset of films, making it more accessible for a recommendation service. The tasks involved include:

1. **Data Import and Conversion**: Importing the dataset, converting data types, and saving the results as a Parquet file.
2. **Genre-based Segmentation**: Segmenting the dataset into individual genres and saving each genre's data as a separate Parquet file.
3. **Pipeline Automation**: Automating the entire process using a pipeline API.

## Directory Structure

- **code/**: Contains the source code for data transformation and pipeline automation.
  - `pipeline.py`: Defines the pipeline stages using Luigi.
  - `stage1.py`
  - `stage2.py`
  - `task2.py`
  - `task3.py`
- **resources/**: Contains the input and output data.
  - `csv/`: Place the initial dataset here.
  - `json/`: Contains the schema file used to load and validate the dataset.
- **output/**: The transformed data is saved here.
- **tests/**: Contains unit tests for the project.
  - `test.py`: Tests for the pipeline.

## Getting Started

### Prerequisites

- Python 3
- PySpark
- Luigi
- Pytest
- Great Expectations

### Installation

Clone this repository and install the dependencies:

```bash
git clone https://github.com/yourusername/data-engineering-film-recommendation-pipeline.git
cd data-engineering-film-recommendation-pipeline
pip install -r requirements.txt

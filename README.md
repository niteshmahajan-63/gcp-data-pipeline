# GCP DATA PIPELINE

## Description
This project is designed to manage and process patient data using Google Cloud services, specifically BigQuery and Cloud Storage. It includes functionalities for loading data into BigQuery tables and managing schemas.

## Installation
To install the required dependencies, run:
```
pip install -r requirements.txt
```

## Configuration
The project requires a configuration file `config.ini` with the following settings:
- `project_id`: Your Google Cloud project ID.
- `credentials_path`: Path to your Google Cloud credentials JSON file.
- `location`: The location for your Google Cloud resources.
- `runmode`: Mode in which the project runs (e.g., start).
- Various flags for reloading data.

## Usage
To run the project, execute the following command:
```
python3 runner.py
```

## Data Loading
The `loadmasters.py` script is responsible for loading data into BigQuery tables. It supports loading data from Excel and CSV files, transforming the data as necessary to match the target schema.

## Contributing
Contributions are welcome! Please submit a pull request or open an issue for discussion.

## License
This project is licensed under the MIT License.

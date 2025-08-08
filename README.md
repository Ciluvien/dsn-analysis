# DSN Analysis
This project contains processing scripts and configurations files for the purpose of analyzing and visualizing [DSN Now](https://eyes.nasa.gov/apps/dsn-now/dsn.html) data.

It does not contain all of the required data due to size constraints.

## Prerequisites
The installation was tested on Ubuntu Desktop 24.10

You will require:

- git
- python/ pip
- docker

Contact me for a copy of the collected data.

## Installation
Clone the repository to your device:
```bash
git clone https://github.com/Ciluvien/dsn-analysis.git
```

Navigate to the project scripts directory and install the required Python libraries (xmltodict, polars):
```bash
pip install -r requirements.txt
```


## Usage
### Load datasets
If you have received prometheus-data and grafana-storage folders, place them under ./data/ and start the project.

Should you want to run the pipeline yourself, follow the next sections.
You will not need the prometheus-data directory in that case. 
You can find exported Grafana dashboard files in the ./dashboards/ folder, although I recommend using the grafana-storage folder instead.

### Prepare directories
Create a data directory in the project root if not already existing:

```bash
mkdir -p ./data/openmetric ./data/to_be_converted ./data/prometheus-data ./data/grafana-storage
```

### Start Grafana & Prometheus
To start the project, execute the following in the project root directory to use the provided docker compose script: 
```bash
docker compose up
```

Grafana is now accessible on [port 3000](http://localhost:3000) and Prometheus on [port 9090](http://localhost:9090).

### General
Most utilities provided in the scripts folder come with their own help text, e.g.:
```bash
python contact.py -h
```

#### Import
The generated OpenMetrics files for import are not deleted automatically from the ./data/openmetrics directory.
It is advised to do so manually, should you not require them anymore.
Otherwise they will be considered every time you run promtool_wrapper.

### DSN Now
#### Collection
To run the scraper without interruptions and save its logs to a file execute:
```bash
nohup ./scraper.sh >> ./scraper.log&
```

Daily archives will be created in the ./data/raw directory.

#### Parsing & Import
Place all the daily XML archives you want to import into the ./data/to_be_converted directory.
Make sure Prometheus is running and execute:

```bash
python parser.py
```

This will process and import all given archives.

### NASA NAIF SPICE distances
#### Distance calculation
A list of sources for SPICE kernels can be found [here](./SPICE Kernels.txt).
Which kernels you select from these sources depends on the time frame of your analysis.

Place all kernels into the ./data/kernels directory.

Edit the MISSIONS list in the distances.py script to included all NAIF IDs of the spacecraft you want to calculate distances for, then execute:

```bash
python distances.py --start START_DATE --end END_DATE

```

Should you have received a collection of kernels, you do not need to modifier the MISSIONS list or set start and end time.
For a period between 2025-05 and 2025-08, kernels for 17 spacecraft have been collected and the MISSIONS list already includes their IDs.

#### Conversion & Import

The generated CSV can now be converted to OpenMetrics by executing:

```bash
python distToOM.py
```

These can then be imported into Prometheus using:

```bash
python promtool_wrapper.py -b 14d
```

### SOMP2B
The SOMP2B logs can be converted to OpenMetrics using:
```bash
python somp2bToOM.py
```

Import into Prometheus can then be done by executing:
```bash 
python promtool_wrapper.py -b 1d
```

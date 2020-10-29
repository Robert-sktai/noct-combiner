# noct-combiner

- Processing NOCT data files which are uploaded to NFS every five minutes and uploading the parsed data into Google Bigtable.

## Setup

- Install Python3 and Python packages related with Google Bigtable.

- Export an environment variable for GCP credential: `export GOOGLE_APPLICATION_CREDENTIALS=${HOME}/noct-combiner/gcp-credential.json`

- Initialize a local DB (Requires SQLite3 package): `./init.db.sh`

## Run

- Currently coordinator.py is a main module. Before you start the program, you might need to modify some configurations in the file.
The configuration will be moved to a config file written by YAML. The program requires Python3: `python3 coordinator.py`

# noct-combiner

- Processing NOCT data files which are uploaded to NFS every five minutes and uploading the parsed data into Google Bigtable.

## Setup

- Install Python3 and Python packages related with Google Bigtable.

- Export an environment variable for GCP credential: `export GOOGLE_APPLICATION_CREDENTIALS=${HOME}/noct-combiner/gcp-credential.json`

- Initialize a local DB (Requires SQLite3 package): `./init.db.sh`

## Run

- A main module is `coordinator.py`. Before you start the program, you might need to modify some configurations in the file `conf/config.yml` written by YAML. The program can start with Python3 like `python3 coordinator.py`.

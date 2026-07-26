# Ouroboros metrics

A collection of observability tools for exporting and
visualising metrics collected from Ouroboros.

The exporter pushes metrics to InfluxDB or writes them as InfluxDB
line protocol to a local file, and provides additional visualization
via grafana.

## Requirements:

The influxdb-client Python package is required in both modes. It is
installed automatically via `pip install .`

InfluxDB OSS 2.0, https://docs.influxdata.com/influxdb/v2.0/
(only required when pushing metrics directly to InfluxDB, not
needed for file mode)

## Optional requirements:

Grafana, https://grafana.com/

## Setup

Install and run InfluxDB and create a bucket in influxDB for exporting
Ouroboros metrics, and a token for writing to that bucket. Consult the
InfluxDB documentation on how to do this,
https://docs.influxdata.com/influxdb/v2.0/get-started/#set-up-influxdb.

To use grafana, install and run grafana open source,
https://grafana.com/grafana/download
https://grafana.com/docs/grafana/latest/?pg=graf-resources&plcmt=get-started

Go to the grafana UI (usually http://localhost:3000) and set up
InfluxDB as your datasource:
Go to Configuration -> Datasources -> Add datasource and select InfluxDB
Set "flux" as the Query Language, and
under "InfluxDB Details" set your Organization as in InfluxDB and set
the copy/paste the token for the bucket to the Token field.

To add the Ouroboros dashboard,
select Dashboards -> Manage -> Import

and then either upload the json file from this repository in

dashboards/general.json

or copy the contents of that file to the "Import via panel json"
textbox and click "Load".

## Run the exporter:

Clone this repository.

Create and activate a virtual environment and install the dependencies:

```
python -m venv venv
source venv/bin/activate
pip install .
```

### InfluxDB mode (default)

Edit the config.ini.example file and fill out the InfluxDB
information (token, org). Save it as config.ini.

Run oexport:

```
oexport
```

Extra tags can be added to every metric:

```
oexport --tag env=production --tag region=eu
```

### File mode

Write metrics as InfluxDB line protocol to a local file, without
pushing to a running InfluxDB instance. This requires no InfluxDB
connection and no config.ini:

```
oexport --output-file metrics.lp
```

Tags can be injected in file mode as well:

```
oexport --output-file metrics.lp \
    --tag test=oping_unimpaired \
    --tag run=20260401_143022
```

The resulting file can be bulk-imported into InfluxDB later:

```
influx write --bucket ouroboros-metrics --file metrics.lp
```

### CLI options

| Flag                | Description                                     |
|---------------------|-------------------------------------------------|
| `-i, --interval`    | FRCT interval, ms (default: 1000)               |
| `--slow-interval`   | IPCP/LSDB/DHT/FA/DT interval, ms                |
| `-b, --bucket`      | InfluxDB bucket (default: ouroboros-metrics)    |
| `-o, --output-file` | Write InfluxDB line protocol to a file          |
| `-t, --tag`         | Extra tag as key=value (repeatable)             |
| `-c, --config`      | Path to config.ini (default: ./config.ini)      |
| `--url`             | InfluxDB server URL                             |
| `--org`             | InfluxDB organization                           |
| `--token`           | InfluxDB authentication token                   |

### Config file

All settings can also be specified in config.ini. CLI flags take
precedence over config file values. See config.ini.example for
details.

| Section      | Key        | Equivalent CLI flag |
|--------------|------------|---------------------|
| `[influx2]`  | `url`      | `--url`             |
| `[influx2]`  | `org`      | `--org`             |
| `[influx2]`  | `token`    | `--token`           |
| `[exporter]` | `interval` | `--interval`        |
| `[exporter]` | `bucket`   | `--bucket`          |
| `[output]`   | `file`     | `--output-file`     |
| `[output]`   | `tags`     | `--tag`             |

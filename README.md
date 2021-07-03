# Ouroboros metrics

A collection of observability tools for exporting and
visualising metrics collected from Ouroboros.

Currently has one very simple exporter for InfluxDB, and provides
additional visualization via grafana.

More features will be added over time.

## Requirements:

InfluxDB 0SS 2.0, https://docs.influxdata.com/influxdb/v2.0/

python influxdb-client, install via

```
pip install 'influxdb-client[ciso]'
```

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

dashboards-grafana/general.json

or copy the contents of that file to the "Import via panel json"
textbox and click "Load".

## Run the exporter:

Clone this repository and go to the pyExporter directory.

Edit the config.ini.example file and fill out the InfluxDB
information (token, org). Save it as config.ini.

and run oexport.py

```
cd exporters-influxdb/pyExporter/
python oexport.py
```

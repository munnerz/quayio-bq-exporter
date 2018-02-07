# quayio-bq-exporter

This tool is used to export [Quay.io](https://quay.io/) usage logs to BigQuery
for analysis.

## Installation

Release binaries are not published. To install, use `go get`:

```
$ go get github.com/munnerz/quayio-bg-exporter
```

## Usage

```
Usage of quayio-bq-exporter:
  -auth-token string
    	quay.io api bearer token to use for requests
  -dataset string
    	the bigquery dataset name to write to
  -end string
    	end date to gather logs to (e.g. 01/02/2006) (default "02/07/2018")
  -namespace string
    	quay.io repository namespace
  -project-id string
    	the project ID for the bigquery dataset
  -repo string
    	quay.io repository name
  -start string
    	start date to gather logs from (e.g. 01/02/2006) (default "02/07/2018")
  -table string
    	the bigquery table id to write to
```

**Notes**:

* Quay.io appears to slow down considerably when requesting large date ranges.
It's advised to invoke the application multiple times with small ranges to
speed up grabbing logs.

# Introduction

This tool reads sql-converted mysql binlog files and inserts selected data form insert statements into InfluxDB.

## Usage examples:

```
for f in DBSRV-bin.00*.gz; do
echo $f ; time zcat $f | mysqlbinlog - | mysqlinflux ;
done
```
or
```
parallel 'echo {} ; zcat {} | mysqlbinlog - | mysqlinflux' ::: DBSRV-bin.00*.gz
```

## Configuration

A file 'conf.json' must be in folder of the binary
```
{
    "db": {
        "address": "http://localhost:8086",
        "database": "z",
        "username": "",
        "password": "",
        "precision": "ms"
    },
    "binlog": {
        "mintime": "2015-01-01T00:00:00Z",
        "maxtime": "2015-03-01T00:00:00Z"
    },
    "debug": false,

    "extract": {
        "point": {
            "tags": {"name": ""},
            "fields": {"my_value": ""},
            "time": {"field": "time_stamp",
                     "format": "2006-01-02T15:04:05.0000"}
        },
        "logs": {
            "tags": {"level": "",
                     "bundle": ""},
            "fields": {"type": ""},
            "time": {"field": "log_date",
                     "format": "2006-01-02 15:04:05"}
        }
    }
}
```

# Installation

* Golang
* InfluxDB: https://github.com/influxdata/influxdb
* Mysql Server (only for mysqlbinlog tool)
* mysqlinflux: https://github.com/chschock/mysqlinflux
* Grafana: https://github.com/grafana/grafana
* (GNU parallel)

# Issues

* Currently the SQL Datatype descides if a numeric or string value is used for InfluxDB.
* NULL values are ignored
* The binlog line seperator is hardcoded as '/*!*/;\n'
* binlog time is taken as UTC
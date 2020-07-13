# Hazelcast Diag viewer

Scripts that visualize diagnostics output on Grafana using Influx DB backend.
The scripts can also parse Dstat CSV files as produced by a simulator run.

# Requirements

## Influx DB

Run `docker run -p 8086:8086 -v $PWD:/var/lib/influxdb influxdb` to start an instance inside a docker container.
The default port is exposed in the host OS, and the data directory is persisted in the current director you run the command from.

## Grafana

Run `docker run -d -p 3000:3000 --name grafana grafana/grafana` which will create a Grafana instance in the background.
The instance should be available at http://localhost:3000

Once you open the page, you can import the dashboard available under the 'grafana' directory of this repository.

## How to run

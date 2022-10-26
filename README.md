# ClickLine (Not For Production)

This is a sample project for ingesting LineProtocol data into ClickHouse for metrics reporting / charting 
using the Grafana plugin. 

## Building

**Build Project**
```shell
$ ./gradlew clean build
```

**Unpack The Build**
```shell
$ cd build/distributions
$ unzip clickine-0.0.1.zip
...
```
**Run the CLI**
```shell
$ ./clickline-0.0.1/bin/clickline
Usage: click-line [OPTIONS] COMMAND [ARGS]...

Options:
  -h, --help  Show this message and exit

Commands:
  server            Run ClickLine server
  generate_metrics  Generates a stream of random metrics
```

## Configuring 
```shell
$ vim clickline-0.0.1/conf/server.conf
```

```hocon
# CLICKLINE CONFIG
server {
  tcp {
    # Port to run the TCP server on
    port = 8086
  }
  http {
    # Port to run the HTTP server on
    port = 8087
  }
}

clickhouse {
  hostname = localhost
  username = default
  password = ""
  port = 8123
  ssl = false
  database = default
  table = <REPLACE_ME>
  # Batch Write Settings
  batch {
    # Maximum size of batch before flush
    size = 100000
    # Maximum seconds to wait before flushing a batch, regardless of the size (unless zero)
    intervalMillis = 1500
    # Number of concurrent writers
    writers = 5
  }
}
```

## Running The Server
Our binary comes with a pre-built TCP / HTTP server for accepting LineProtocol data. 

After editing the configuration, can you simply run the server.

```shell
$ ./clickline-0.0.1/bin/clickline server --config clickline-0.0.1/conf/server.conf
```

## Running The Simulator

If you don't have a bunch of LineProtocol data sitting around, or have a live service to post to. Don't worry, 
because we've included a simulator that for demonstration purposes.

```
./clickline-0.0.1/bin/clickline generate_metrics --help
Usage: click-line generate_metrics [OPTIONS]

  Generates a stream of random metrics

Options:
  --numEnvironments INT    Number of environments (e.g. prod/dev)
  --numHosts INT           Number of hosts per-environment
  --numMetrics INT         Number of metrics to generate per-host, per-env
  --valueRange TEXT...     Range of possible values to generate. These should
                           be doubles (e.g. 0.0 or 100.0)
  --valueJitter FLOAT      Max amount of value change between intervals. This
                           helps make the simulation look less chaotic.
  --intervalLength TEXT    Duration of time between the metrics (e.g. 5s)
  --socketConcurrency INT  Number of concurrent sockets to open for writing.
  --clicklineHost TEXT     Host of ClickLine server
  --clicklinePort INT      Port of ClickLine server
  -h, --help               Show this message and exit
```

**Sample Usage**
```shell
$ ./clickline-0.0.1/bin/clickline-server generate_metrics --numEnvironments=2 --numHosts=100 --numMetrics=5 --valueRange 0.0 100.0 --intervalLength=2s --clicklineHost=localhost --clicklinePort=8086
```
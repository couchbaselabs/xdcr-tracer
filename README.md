# XDCRTracer

XDCRTracer is a script for testing XDCR links between clusters and can handle
varying topologies (Although is not a free in scope as Couchbase itself is
yet).

XDCRTracer achieves this by using a list of source buckets and a list of
destination buckets (Which includes all the source buckets). XDCRTracer will
generate 1024 keys (One for each vBucket) into every source bucket and will
check that they make it to every destination bucket.

## Running

XDCRTracer requires a JSON Configuration file, an example of the parameters is
given in example.json. The path to the JSON file must be given as the first
parameter to the script. e.g.

    java -jar xdcr_tracer example.json

## Configuration

 - `Number interval` How long to leave between upserting keys and checking for
 propogation.

 - `Number ttl` How long should the tracing documents exist for. Minimum should be
 `interval + time taken to insert keys + time taken to check keys`. Recommend
 doubling `interval`.

 - `Boolean repeat` Should the XDCR checker just run once or repeat

 - `Boolean debug` Should debug messages be printed

 - `Object sources` The associative array of source clusters to insert keys
 into and check keys on.
    - `Array connection` Array of connection strings to the cluster
    - `Object buckets` Associative array of bucket names to bucket passwords

- `Object replicas` The associative array of replica clusters to additionally
check keys on.

## Build Instructions

It is recommended that you install maven, this can usually be handled by a
package manager. For OSX Homebrew you can use

    brew install maven

When using maven, building is simple:

    mvn compile

If you require a JAR then:

    mvn package
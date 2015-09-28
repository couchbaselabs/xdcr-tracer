import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.*;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;

import rx.Observable;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 * XDCRTracer creates a document in every vBucket in every Bucket in every
 * cluster at a specified time interval and ensures that they get to the
 * destination clusters.
 */
public class XDCRTracer
{
    protected int interval;
    protected int ttl;
    protected boolean debug;

    protected Map<String, ArrayList<Bucket>> origins;
    protected Map<String, ArrayList<Bucket>> destinations;

    /**
     * Creates and XDCR Tracer
     * @param interval Time interval between checks in seconds
     * @param ttl Length of time for document expiry
     * @param sources List of cluster lists for creating documents on
     * @param replicas List of cluster lists for additional monitoring
     */
    XDCRTracer(
            int interval,
            int ttl,
            Map<String, ArrayList<Bucket>> sources,
            Map<String, ArrayList<Bucket>> replicas,
            boolean debug
    )
    {
        this.interval = interval;
        this.ttl = ttl;
        this.debug = debug;

        if (ttl <= interval) throw new AssertionError("TTL must be greater than interval!");

        this.origins = sources;

        this.destinations = new HashMap<>();
        this.destinations.putAll(sources);
        this.destinations.putAll(replicas);

    }

    /**
     * Creates keys in every vBucket in every bucket in every
     * source cluster and checks that all the keys make it to all the
     * clusters.
     */
    public void runOnce()
    {
        // Future TODO: Use separate key lists for different buckets/clusters
        // for XDCR topologies with varying buckets on different clusters
        ArrayList<String> keys = new ArrayList<>();

        // Add all the keys
        origins.forEach((clusterName, buckets) -> {
            debug("Adding keys to cluster: " + clusterName);
            String[] clusterKeys = KeyGen.fillVBuckets(
                    "XDCRTracer_" + clusterName + "_" + System.currentTimeMillis() / 1000l + "_"
            );
            keys.addAll(Arrays.asList(clusterKeys));
            buckets.forEach(bucket -> seedBucket(bucket, clusterKeys));
        });

        debug("Sleeping for " + interval + " seconds.");
        try {
            Thread.sleep(interval * 1000);
        } catch(InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        destinations.forEach((clusterName, buckets) -> {
                debug("Checking cluster: " + clusterName);
                buckets.forEach(bucket -> {
                    debug("Checking bucket: " + clusterName + "/" + bucket.name());
                    ArrayList<Integer> badvBuckets = checkBucket(bucket, keys);
                    if (badvBuckets.size() > 0) {
                        System.out.println("Missing documents in " + clusterName + "/" + bucket.name() + ": " + badvBuckets);
                    }
                });
            }
        );
    }

    public void run() {
        while(true) {
            runOnce();
        }
    }

    /**
     * Checks the contents of the bucket to ensure all the keys are there
     * otherwise returns a list of vBuckets that didn't make it.
     * @param bucket
     * @param keys
     * @return vBucket List
     */
    protected ArrayList<Integer> checkBucket(final Bucket bucket, final Collection<String> keys) {
        List<String> result = Observable
                                .from(keys)
                                .flatMap(id -> bucket.async().get(id).map(doc -> doc.id()).onErrorReturn(err -> ""))
                                .toList()
                                .toBlocking()
                                .single();


        ArrayList<Integer> vBuckets = new ArrayList<>();
        for(String x : keys) {
            if(!result.contains(x)) {
                vBuckets.add(KeyGen.vBucket(x));
            }
        }
        return vBuckets;
    }

    /**
     * Fills the bucket with the list of keys so that every vBucket in the
     * cluster is hit for this bucket.
     * @param bucket
     * @param keys
     */
    protected void seedBucket(final Bucket bucket, String[] keys) {
        ArrayList<JsonDocument> documents = new ArrayList<>();
        for(String key : keys) {
            JsonObject content = JsonObject.create();
            documents.add(JsonDocument.create(key, ttl, content));
        }
        Observable
                .from(documents)
                .flatMap(docToInsert -> bucket.async().insert(docToInsert))
                .last()
                .toBlocking()
                .single();
    }

    protected void debug(String str) {
        if(debug) {
            System.out.println(str);
        }
    }

    /**
     * Handles CLI entry
     * @param args
     */
    public static void main(String [] args)
    {

        JSONParser parser = new JSONParser();


        try {
            FileReader file = new FileReader(args[0]);
            Object object = parser.parse(file);

            JSONObject jsonObject = (JSONObject) object;

            int interval = ((Long) jsonObject.get("interval")).intValue();
            int ttl = ((Long) jsonObject.get("ttl")).intValue();
            boolean repeat = (boolean) jsonObject.get("repeat");
            boolean debug = (boolean) jsonObject.get("debug");


            JSONObject sourceList = (JSONObject) jsonObject.get("sources");
            JSONObject replicaList = (JSONObject) jsonObject.get("replicas");

            CouchbaseEnvironment env = DefaultCouchbaseEnvironment.create();
            Map<String, ArrayList<Bucket>> sources = new HashMap<>();
            Map<String, ArrayList<Bucket>> replicas = new HashMap<>();

            List<Cluster> clusters = new ArrayList<>();

            sourceList.forEach((k, v) -> {
                String clusterName = (String) k;
                JSONObject clusterConfig = (JSONObject) v;

                List<String> connection = (JSONArray) clusterConfig.get("connection");
                Map<String, String> bucketConfigs = (JSONObject) clusterConfig.get("buckets");

                Cluster cluster = CouchbaseCluster.create(env, connection);
                clusters.add(cluster);

                ArrayList<Bucket> buckets = new ArrayList<>();
                bucketConfigs.forEach((name, password) -> {
                    Bucket bucket = cluster.openBucket(name, password);
                    buckets.add(bucket);
                });

                sources.put(clusterName, buckets);
            });

            replicaList.forEach((k, v) -> {
                String clusterName = (String) k;
                JSONObject clusterConfig = (JSONObject) v;

                List<String> connection = (JSONArray) clusterConfig.get("connection");
                Map<String, String> bucketConfigs = (JSONObject) clusterConfig.get("buckets");

                Cluster cluster = CouchbaseCluster.create(env, connection);
                clusters.add(cluster);

                ArrayList<Bucket> buckets = new ArrayList<>();
                bucketConfigs.forEach((name, password) -> {
                    Bucket bucket = cluster.openBucket(name, password);
                    buckets.add(bucket);
                });

                replicas.put(clusterName, buckets);
            });

            XDCRTracer tracer = new XDCRTracer(interval, ttl, sources, replicas, debug);

            if(repeat) {
                tracer.run();
            } else {
                tracer.runOnce();
            }

            clusters.forEach(Cluster::disconnect);
            env.shutdown();



        } catch(FileNotFoundException e) {
            System.err.println("JSON configuration file '" + args[0] + "' does not exist.");
        } catch(ParseException e) {
            System.err.println("JSON configuration file '" + args[0] + " is not JSON.'");
        } catch(NullPointerException e) {
            System.err.println("NullPointerException: Your JSON config file is probably missing stuff.");
            e.printStackTrace();
        } catch(ArrayIndexOutOfBoundsException e) {
            System.err.println("You must specify a JSON configuration file!");
        } catch(Exception e) {
            System.err.println("Unexpected error:");
            e.printStackTrace();
        }
    }
}

import java.util.*;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;

import com.couchbase.client.java.error.TemporaryFailureException;
import com.sun.deploy.util.StringUtils;

import rx.Observable;
import rx.functions.Func1;

/**
 * XDCRTracer creates a document in every vBucket in every Bucket in every
 * cluster at a specified time interval and ensures that they get to the
 * destination clusters.
 */
public class XDCRTracer
{
    protected int interval;

    protected CouchbaseEnvironment env;
    protected ArrayList<Cluster> clusters;

    protected HashMap<String, HashMap<String, Bucket>> origins;
    protected HashMap<String, HashMap<String, Bucket>> destinations;

    /**
     * Creates and XDCR Tracer
     * @param interval Time interval between checks in seconds
     * @param masterClusters List of cluster lists for creating documents on
     * @param replicaClusters List of cluster lists for additional monitoring
     * @param bucketNames List of buckets for monitoring
     */
    XDCRTracer(
            int interval,
            ArrayList<ArrayList<String>> masterClusters,
            ArrayList<ArrayList<String>> replicaClusters,
            ArrayList<String> bucketNames
    )
    {
        this.interval = interval;
        this.env = DefaultCouchbaseEnvironment.create();
        this.clusters = new ArrayList<Cluster>();
        this.origins = new HashMap<String, HashMap<String, Bucket>>();
        this.destinations = new HashMap<String, HashMap<String, Bucket>>();

        // Start-up the cluster connections and bucket connections for master clusters
        for(ArrayList<String> masterCluster : masterClusters) {
            CouchbaseCluster cluster = CouchbaseCluster.create(env, masterCluster);
            clusters.add(cluster);

            HashMap<String, Bucket> buckets = openBuckets(cluster, bucketNames);
            origins.put(masterCluster.get(0), buckets);
            destinations.put("{" + StringUtils.join(masterCluster, ",") + "}", buckets);
        }
        // Start-up the cluster connections and bucket connections for destinationc clusters
        for(ArrayList<String> replicaCluster : replicaClusters) {
            CouchbaseCluster cluster = CouchbaseCluster.create(env, replicaCluster);
            clusters.add(cluster);

            HashMap<String, Bucket> buckets = openBuckets(cluster, bucketNames);
            destinations.put("{" + StringUtils.join(replicaCluster, ", ") + "}", buckets);
        }
    }

    /**
     * Creates a list of bucket connections to a cluster
     * @param cluster Cluster to open bucket connections in
     * @param bucketNames List of bucket names to open
     * @return List of buckets
     */
    protected HashMap<String, Bucket> openBuckets(Cluster cluster, ArrayList<String> bucketNames)
    {
        HashMap<String, Bucket> buckets = new HashMap<String, Bucket>();
        for(String bucketName : bucketNames) {
            buckets.put(bucketName, cluster.openBucket(bucketName));
        }
        return buckets;
    }

    /**
     * Continually creates keys in every vBucket in every bucket in every
     * source cluster and checks that all the keys make it to all the
     * clusters.
     */
    public void run()
    {
        //while(true) {
            ArrayList<String> keys = new ArrayList<String>();

            // Add all the keys
            for(Map.Entry<String, HashMap<String, Bucket>> cluster : origins.entrySet()) {
                String[] clusterKeys = KeyGen.fillVBuckets(
                        "XDCRTracer_" + cluster.getKey() + "_" + System.currentTimeMillis() / 1000l + "_");
                keys.addAll(Arrays.asList(clusterKeys));
                for(Bucket bucket : cluster.getValue().values()) {
                    seedBucket(bucket, clusterKeys);
                }
            }
            try {
                Thread.sleep(interval * 1000);
            } catch(InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            for(Map.Entry<String, HashMap<String, Bucket>> cluster : destinations.entrySet()) {
                for(Map.Entry<String, Bucket> bucket : cluster.getValue().entrySet()) {
                    System.out.println("Checking bucket: " + cluster.getKey() + "/" + bucket.getKey());
                    ArrayList<Integer> badvBuckets = checkBucket(bucket.getValue(), keys);
                    if(badvBuckets.size() > 0) {
                        System.out.println("Missing documents in " + cluster.getKey() + "/" + bucket.getKey() + ": " + badvBuckets);
                    }
                }
            }

        //}
    }

    /**
     * Checks the contents of the bucket to ensure all the keys are there
     * otherwise returns a list of vBuckets that didn't make it.
     * @param bucket
     * @param keys
     * @return vBucket List
     */
    protected ArrayList<Integer> checkBucket(final Bucket bucket, final Collection<String> keys) {
        List<JsonDocument> result;
        try {
            result = Observable
                    .from(keys)
                    .flatMap(new Func1<String, Observable<JsonDocument>>() {
                        public Observable<JsonDocument> call(String id) {
                            return bucket.async().get(id);
                        }
                    })
                    .toList()
                    .toBlocking()
                    .single();
        } catch(TemporaryFailureException e) {
            System.err.println("Couldn't get keys from bucket (TemporaryFailureException, vBucket file may have been deleted)");
            return new ArrayList<Integer>();
        }


        ArrayList<Integer> vbuckets = new ArrayList<Integer>();
        for(String x : keys) {
            boolean skip = false;
            for(JsonDocument y : result) {

                if(y.id().equals(x)) {
                    skip = true;
                    break;
                }
            }
            if(!skip) {
                vbuckets.add(KeyGen.vBucket(x));
            }
        }
        return vbuckets;
    }

    /**
     * Fills the bucket with the list of keys so that every vBucket in the
     * cluster is hit for this bucket.
     * @param bucket
     * @param keys
     */
    protected void seedBucket(final Bucket bucket, String[] keys) {
        ArrayList<JsonDocument> documents = new ArrayList<JsonDocument>();
        for(String key : keys) {
            JsonObject content = JsonObject.create();
            documents.add(JsonDocument.create(key, interval * 4, content));
        }
        Observable
                .from(documents)
                .flatMap(new Func1<JsonDocument, Observable<JsonDocument>>() {
                    public Observable<JsonDocument> call(final JsonDocument docToInsert) {
                        return bucket.async().insert(docToInsert);
                    }
                })
                .last()
                .toBlocking()
                .single();
    }

    /**
     * Handles shutdown of the cluster
     */
    public void shutdown()
    {
        System.out.println("Shutting down");
        for(Cluster cluster : clusters) {
            cluster.disconnect();
        }
        env.shutdown();
    }

    /**
     * Handles CLI entry
     * @param args
     */
    public static void main(String [] args)
    {
        int interval = 5; // Delays

        // Clusters to put documents into
        ArrayList<String> CHO = new ArrayList<String>(Arrays.asList("192.168.75.101", "192.168.75.102"));
        ArrayList<String> SGL = new ArrayList<String>(Arrays.asList("192.168.75.103"));

        ArrayList<ArrayList<String>> masterClusters = new ArrayList<ArrayList<String>>();
        masterClusters.add(SGL);
        masterClusters.add(CHO);

        // Clusters to also check
        ArrayList<ArrayList<String>> replicaClusters = new ArrayList<ArrayList<String>>();

        // Buckets to check
        ArrayList<String> bucketNames = new ArrayList<String>(Arrays.asList("default"));

        XDCRTracer tracer = new XDCRTracer(interval, masterClusters, replicaClusters, bucketNames);
        tracer.run();
        tracer.shutdown();
    }
}

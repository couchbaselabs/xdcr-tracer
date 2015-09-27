import java.util.*;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;

import rx.Observable;

/**
 * XDCRTracer creates a document in every vBucket in every Bucket in every
 * cluster at a specified time interval and ensures that they get to the
 * destination clusters.
 */
public class XDCRTracer
{
    protected int interval;
    protected int ttl;

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
            Map<String, ArrayList<Bucket>> replicas
    )
    {
        this.interval = interval;
        this.ttl = ttl;

        if (ttl <= interval) throw new AssertionError("TTL must be greater than interval!");

        this.origins = sources;

        this.destinations = new HashMap<>();
        this.destinations.putAll(sources);
        this.destinations.putAll(replicas);

    }

    /**
     * Creates a list of bucket connections to a cluster
     * @param cluster Cluster to open bucket connections in
     * @param bucketNames List of bucket names to open
     * @return List of buckets
     */
    protected HashMap<String, Bucket> openBuckets(Cluster cluster, ArrayList<String> bucketNames)
    {
        HashMap<String, Bucket> buckets = new HashMap<>();
        bucketNames.forEach(bucketName -> buckets.put(bucketName, cluster.openBucket(bucketName)));

        return buckets;
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
            String[] clusterKeys = KeyGen.fillVBuckets(
                    "XDCRTracer_" + clusterName + "_" + System.currentTimeMillis() / 1000l + "_"
            );
            keys.addAll(Arrays.asList(clusterKeys));
            buckets.forEach(bucket -> seedBucket(bucket, clusterKeys));
        });

        try {
            Thread.sleep(interval * 1000);
        } catch(InterruptedException e) {
            Thread.currentThread().interrupt();
        }


        destinations.forEach((clusterName, buckets) ->
            buckets.forEach(bucket -> {
                System.out.println("Checking bucket: " + clusterName + "/" + bucket.name());
                ArrayList<Integer> badvBuckets = checkBucket(bucket, keys);
                if (badvBuckets.size() > 0) {
                    System.out.println("Missing documents in " + clusterName + "/" + bucket.name() + ": " + badvBuckets);
                }
            })

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

    /**
     * Handles CLI entry
     * @param args
     */
    public static void main(String [] args)
    {
        int interval = 5; // Delays
        int ttl = 15;

        CouchbaseEnvironment env = DefaultCouchbaseEnvironment.create();

        /* CHO Bucket Connections */
        CouchbaseCluster CHO = CouchbaseCluster.create(env, Arrays.asList("192.168.75.101", "192.168.75.102"));
        Bucket CHO_Default = CHO.openBucket("default");
        ArrayList<Bucket> CHOBuckets = new ArrayList<>(Arrays.asList(CHO_Default));

        /* SGL Bucket Connections */
        CouchbaseCluster SGL = CouchbaseCluster.create(env, "192.168.75.103");
        Bucket SGL_Default = SGL.openBucket("default");
        ArrayList<Bucket> SGLBuckets = new ArrayList<>(Arrays.asList(SGL_Default));

        /* Buckets maps */
        Map<String, ArrayList<Bucket>> sources = new HashMap<>();
        Map<String, ArrayList<Bucket>> replicas = new HashMap<>();

        sources.put("CHO", CHOBuckets);
        replicas.put("SGL", SGLBuckets);

        XDCRTracer tracer = new XDCRTracer(interval, ttl, sources, replicas);
        tracer.run();

        CHO.disconnect();
        SGL.disconnect();
        env.shutdown();
    }
}

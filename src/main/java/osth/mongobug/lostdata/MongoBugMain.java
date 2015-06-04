package osth.mongobug.lostdata;

import java.util.*;
import java.util.concurrent.*;

import org.bson.Document;

import com.mongodb.*;
import com.mongodb.client.*;

/**
 * The main method that will kick off multiple threads
 * in order to insert many documents into MongoDB at the same time
 */
public class MongoBugMain {

    public static final String DEFAULT_HOST = "localhost";
    public static final String DEFAULT_PORT = "27017";
    public static final String DEFAULT_DATABASE_NAME = "mongobug";
    public static final String DEFAULT_COLLECTION_NAME = "lostdata";
    public static final String DEFAULT_THREAD_COUNT = "10";
    public static final String DEFAULT_INSERTS_PER_THREAD = "40000";
    public static final String DEFAULT_MESSAGE = "default message";

    public static void main(String[] args) {
        for (String arg : args) {
            if ("--help".equals(arg)) {
                printHelpAndExit();
            }
        }

        String host = getOptionOrDefault(args, "-h", "--host", DEFAULT_HOST);
        int port = Integer.parseInt(getOptionOrDefault(args, "-p", "--port", DEFAULT_PORT));
        String databaseName = getOptionOrDefault(args, "-d", "--database", DEFAULT_DATABASE_NAME);
        String collectionName = getOptionOrDefault(args, "-c", "--collection", DEFAULT_COLLECTION_NAME);
        int threadCount = Integer.parseInt(getOptionOrDefault(args, "-t", "--threadcount", DEFAULT_THREAD_COUNT));
        int insertsPerThread = Integer.parseInt(getOptionOrDefault(args, "-i", "--insertsperthread", DEFAULT_INSERTS_PER_THREAD));
        String message = getOptionOrDefault(args, "-m", "--message", DEFAULT_MESSAGE);

        MongoClientOptions mco = MongoClientOptions.builder().writeConcern(WriteConcern.MAJORITY).build();
        MongoClient mongoClient = new MongoClient(new ServerAddress(host, port), mco);
        MongoDatabase database = mongoClient.getDatabase(databaseName);
        MongoCollection<Document> dbCollection = database.getCollection(collectionName);

        ExecutorService es = Executors.newFixedThreadPool(threadCount);
        long insertStart = System.currentTimeMillis();
        try {
            CountDownLatch threadInitializationLatch = new CountDownLatch(threadCount);
            CountDownLatch threadsMayStartLatch = new CountDownLatch(1);

            List<Future<String>> results = new ArrayList<Future<String>>();
            for (int i = 0; i < threadCount; i++) {
                results.add(es.submit(new MongoDocumentInserter(insertsPerThread, threadInitializationLatch, threadsMayStartLatch, "Thread " + i, message, dbCollection)));
            }

            threadInitializationLatch.await(10, TimeUnit.SECONDS);
            threadsMayStartLatch.countDown();

            for (Future<String> result : results) {
                System.out.println(result.get());
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            mongoClient.close();
            es.shutdown();
            double durationInSeconds = (System.currentTimeMillis() - insertStart) / 1000.0;
            System.out.println("DONE: Inserted doucments in " + durationInSeconds + " seconds");
        }
    }

    private static String getOptionOrDefault(String[] args, String optionFlag, String optionText, String defaultOption) {
        for (int i = 0; i < args.length; i++) {
            if ( (args[i].equals(optionFlag) || args[i].equals(optionText)) 
                    && i+1 < args.length) {
                return args[i+1];
            }
        }
        return defaultOption;
    }

    private static void printHelpAndExit() {
        System.out.println("Usage: MongoBugMain -h <arg> -p <arg> -d <arg> -c <arg> -t<arg> -i <arg> -m arg");
        System.out.println("    A utility to insert many documents into MongoDB in a multi-threaded manner");
        System.out.println("    All arguments are optional");
        System.out.println("Arguments:");
        System.out.println("  -h  --host <arg>              The host for the mongos, default: " + DEFAULT_HOST);
        System.out.println("  -p  --port <arg>              The port for the mongos, default: " + DEFAULT_PORT);
        System.out.println("  -d  --database <arg>          The database name, default: " + DEFAULT_DATABASE_NAME);
        System.out.println("  -c  --collection <arg>        The collection name, default: " + DEFAULT_COLLECTION_NAME);
        System.out.println("  -t  --threadcount <arg>       The number of threads to start, default: " + DEFAULT_THREAD_COUNT);
        System.out.println("  -i  --insertsperthread <arg>  The number of inserts per thread, default: " + DEFAULT_INSERTS_PER_THREAD);
        System.out.println("  -m  --message <arg>           The message inserted for this run, default: " + DEFAULT_MESSAGE);
        System.out.println("      --help                    Prints this message");
        System.exit(1);
    }
}

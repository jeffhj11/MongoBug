package osth.mongobug.lostdata;

import java.util.Objects;
import java.util.concurrent.*;

import org.bson.Document;
import com.mongodb.client.MongoCollection;

/**
 * The class that will insert documents into MongoDB
 */
public class MongoDocumentInserter implements Callable<String> {

    private final int numberOfInserts;
    private final CountDownLatch initializationLatch;
    private final CountDownLatch waitToStartLatch;
    private final String threadName;
    private final String message;
    private final MongoCollection<Document> dbCollection;

    public MongoDocumentInserter(int numberOfInserts,
            CountDownLatch initializationLatch,
            CountDownLatch waitToStartLatch,
            String threadName,
            String message,
            MongoCollection<Document> dbCollection) {
        this.numberOfInserts = numberOfInserts;
        this.initializationLatch = Objects.requireNonNull(initializationLatch);
        this.waitToStartLatch = Objects.requireNonNull(waitToStartLatch);
        this.threadName = Objects.requireNonNull(threadName);
        this.message = Objects.requireNonNull(message);
        this.dbCollection = Objects.requireNonNull(dbCollection);
    }


    public String call() throws Exception {
        Document data = new Document();
        data.put("threadName", threadName);
        data.put("message", message);
        
        initializationLatch.countDown();
        waitToStartLatch.await(10, TimeUnit.SECONDS);

    int currentCount = 0;
        for (; currentCount < numberOfInserts; currentCount++) {
            data.remove("_id");
            String uuid = java.util.UUID.randomUUID().toString();
            //data.put("_id", uuid); // uncomment this line if you want the _id field to be set as the uuid
            data.put("uuid", uuid);
            data.put("count", currentCount);

            dbCollection.insertOne(data);

            if (currentCount % 10000 == 0) {
                System.out.println("Thread " + threadName + " has attempted to insert " + currentCount + " documents so far");
            }
        }

        return "Thread " + threadName + " inserted " + currentCount + " documents";
    }
}

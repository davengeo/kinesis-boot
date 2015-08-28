package demo;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.*;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Controller;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

@Order(1)
@Controller
public class CliController implements CommandLineRunner {

    private static final Logger LOG = LoggerFactory.getLogger(CliController.class);

    private static AWSCredentialsProvider credentialsProvider;

    static  {
        credentialsProvider = new ProfileCredentialsProvider();
        try {
            credentialsProvider.getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException("Cannot load the credentials from the credential profiles file. "
                    + "Please make sure that your credentials file is at the correct "
                    + "location (~/.aws/credentials), and is in valid format.", e);
        }
    }

    @Override
    public void run(String... args) throws Exception {
        LOG.info("Preparing INGESTING...");
        kinesisProducer(createKinesisProducerConfiguration());
        LOG.info("INGESTION DONE");
        readingKinesis();
    }

    private void readingKinesis() {
        AmazonKinesisClient client = new AmazonKinesisClient();
        client.setEndpoint("kinesis.eu-west-1.amazonaws.com", "kinesis", "eu-west-1");
        DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
        describeStreamRequest.setStreamName("test");
        List<Shard> shards = new ArrayList<>();
        String exclusiveStartShardId = null;
        do {
            describeStreamRequest.setExclusiveStartShardId( exclusiveStartShardId );
            DescribeStreamResult describeStreamResult = client.describeStream( describeStreamRequest );
            shards.addAll( describeStreamResult.getStreamDescription().getShards() );
            if (describeStreamResult.getStreamDescription().getHasMoreShards() && shards.size() > 0) {
                exclusiveStartShardId = shards.get(shards.size() - 1).getShardId();
            } else {
                exclusiveStartShardId = null;
            }
        } while ( exclusiveStartShardId != null );

        String shardIterator;
        GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest();
        getShardIteratorRequest.setStreamName("test");
        getShardIteratorRequest.setShardId(shards.get(0).getShardId());
        getShardIteratorRequest.setShardIteratorType("LATEST");

        GetShardIteratorResult getShardIteratorResult = client.getShardIterator(getShardIteratorRequest);
        shardIterator = getShardIteratorResult.getShardIterator();
        LOG.info("shard length:" + shardIterator.length());
        List<Record> records;

        //noinspection InfiniteLoopStatement
        while (true) {

            // Create a new getRecordsRequest with an existing shardIterator
            // Set the maximum records to return to 25
            GetRecordsRequest getRecordsRequest = new GetRecordsRequest();
            getRecordsRequest.setShardIterator(shardIterator);
            getRecordsRequest.setLimit(25);

            GetRecordsResult result = client.getRecords(getRecordsRequest);

            // Put the result into record list. The result can be empty.
            records = result.getRecords();
            LOG.info("Incoming records:" + records.size());
            for(Record record: records) {
                LOG.info("record:" + new String(record.getData().array()) +
                        " partition:" + record.getPartitionKey());
            }

            try {
                Thread.sleep(1000);
            }
            catch (InterruptedException exception) {
                throw new RuntimeException(exception);
            }

            shardIterator = result.getNextShardIterator();
        }
    }


    private void kinesisProducer(KinesisProducerConfiguration config) throws UnsupportedEncodingException {
        KinesisProducer kinesis = new KinesisProducer(config);
        for (int i = 0; i < 100; ++i) {
            ByteBuffer data = ByteBuffer.wrap("myDataAndDenissIsGiantwarf".getBytes("UTF-8"));
            kinesis.addUserRecord("test", "myPartitionKey", data);
        }
    }

    private KinesisProducerConfiguration createKinesisProducerConfiguration() {
        return new KinesisProducerConfiguration()
                    .setRecordMaxBufferedTime(3000)
                    .setMaxConnections(1)
                    .setRequestTimeout(60000)
                    .setRegion("eu-west-1");
    }
}

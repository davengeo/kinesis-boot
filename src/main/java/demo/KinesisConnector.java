package demo;

import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.PutRecordsResultEntry;
import com.amazonaws.services.s3.model.Region;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.hibernate.validator.internal.util.CollectionHelper.newArrayList;


@Component
public class KinesisConnector {

    private final Logger log = Logger.getLogger(KinesisConnector.class);
    private final int maxRecordSize = 400;

    private PutRecordsRequest putRecordsRequest;

    /**
     * constructor
     */
    public KinesisConnector() {
        putRecordsRequest = new PutRecordsRequest();

        putRecordsRequest.setStreamName("test");
    }

    public boolean sendToKinesis(String dispatcherMessage) {

        List<PutRecordsRequestEntry> putRecordsRequestEntryList = newArrayList(generateRecordsRequestEntryList(dispatcherMessage));
        putRecordsRequest.setRecords(putRecordsRequestEntryList);

        AmazonKinesisClient amazonKinesisClient = new AmazonKinesisClient();
        amazonKinesisClient.setRegion(Region.EU_Ireland.toAWSRegion());

        PutRecordsResult putRecordsResult = amazonKinesisClient.putRecords(putRecordsRequest);

        try {
            while (putRecordsResult.getFailedRecordCount() > 0) {
                final List<PutRecordsRequestEntry> failedRecordsList = new ArrayList<>();
                final List<PutRecordsResultEntry> failedRecordsListFromKinesis = new ArrayList<>();
                final List<PutRecordsResultEntry> putRecordsResultEntryList = putRecordsResult.getRecords();
                final Random random = new Random();

                for (int i = 0; i < putRecordsResultEntryList.size(); i++) {
                    final PutRecordsRequestEntry putRecordRequestEntry = putRecordsRequestEntryList.get(i);
                    final PutRecordsResultEntry putRecordsResultEntry = putRecordsResultEntryList.get(i);
                    if (putRecordsResultEntry.getErrorCode() != null) {
                        putRecordRequestEntry.setPartitionKey(String.format("partitionKey-%d",
                                (random.nextInt(maxRecordSize) + 1)));
                        failedRecordsList.add(putRecordRequestEntry);
                        failedRecordsListFromKinesis.add(putRecordsResultEntry);
                    }
                }
                logFailedRecords(failedRecordsListFromKinesis);
                putRecordsRequestEntryList = failedRecordsList;
                putRecordsRequest.setRecords(putRecordsRequestEntryList);
                putRecordsResult = amazonKinesisClient.putRecords(putRecordsRequest);
            }

        } finally {
            amazonKinesisClient.shutdown();
            putRecordsRequestEntryList.clear();
        }

        return true;

    }

    private List<PutRecordsRequestEntry> generateRecordsRequestEntryList(String message) {
        List<PutRecordsRequestEntry> result = newArrayList();
        PutRecordsRequestEntry entry = new PutRecordsRequestEntry();
        entry.setData(ByteBuffer.wrap(message.getBytes()));
        entry.setPartitionKey("kuku");
        result.add(entry);
        return result;
    }

    private void logFailedRecords(List<PutRecordsResultEntry> result) {
        StringBuilder failureReasons = new StringBuilder();

        for (PutRecordsResultEntry resultEntry : result) {
            failureReasons.append("{error: ").append(resultEntry.getErrorMessage()).append("}\n");
        }

        log.error("The following records were rejected by kinesis and will be retried\n\n" + failureReasons.toString());
    }

}

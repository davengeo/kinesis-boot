package demo;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;


public class KinesisFactory implements IRecordProcessorFactory {

        @Override
        public IRecordProcessor createProcessor() {
            return new KinesisConsumer();
        }
}

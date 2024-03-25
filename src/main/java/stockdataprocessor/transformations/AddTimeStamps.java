package stockdataprocessor.transformations;

import org.apache.beam.sdk.transforms.DoFn;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AddTimeStamps extends DoFn<String, Float> {
    private static Logger logger = LoggerFactory.getLogger(AddTimeStamps.class.getName());

    @ProcessElement
    public void processElement(@Element String element, OutputReceiver<Float> out) {
        String[] comps = element.split(",");
        String dateStr = comps[0];
        String highPrice = comps[2];
        Instant logTimeStamp = null;
        Float highP = null;
        // Extract the timestamp from log entry we're currently processing.
        try {
            logTimeStamp = Instant.parse(dateStr, DateTimeFormat.forPattern("yyyy-MM-dd"));
            highP = Float.parseFloat(highPrice);
            // Use OutputReceiver.outputWithTimestamp (rather than
            // OutputReceiver.output) to emit the entry with timestamp attached.
            out.outputWithTimestamp(highP, logTimeStamp);
        } catch (Exception e){
            logger.info("Error {}", e);
        }


    }
}

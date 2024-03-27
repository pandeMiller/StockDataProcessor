package stockdataprocessor;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Instant;

import java.util.HashMap;
import java.util.Map;

public class FakePostGresIOConnector extends PostGresIOConnector {
    private static Map<Instant,Float> fakeTable = new HashMap<>();

    public FakePostGresIOConnector(Main.CustomPipelineOptions customPipelineOptions) {
        super(customPipelineOptions);
    }

    /** put the records in PCollection in the fake table **/
    @Override
    public PCollection<KV<Instant, Float>> persistRecords(PCollection<KV<Instant, Float>> input) {
        input.apply(ParDo.of(new TestDataProcessor()));
        return input;
    }

    /** get the persisted data in the fake table  **/
    public Map<Instant,Float> getPersistedData(){
        return fakeTable;
    }

    /** clean up the data in the fake table **/
    public void cleanUp() {
        fakeTable.clear();
    }

    public static class TestDataProcessor extends DoFn<KV<Instant, Float>,KV<Instant, Float>> {

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            fakeTable.put(c.element().getKey(),c.element().getValue());
            c.output(c.element());
        }
    }
}

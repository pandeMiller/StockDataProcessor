package stockdataprocessor;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
//import
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import stockdataprocessor.transformations.CsvDataTo10DayMovingAvg;

import java.util.Map;


public class CsvDataTo10DayMovingAvgTest {
//    private Pipeline testPipeline;
    private Main.CustomPipelineOptions customPipelineOptions;
//    private FakePostGresIOConnector fakePostGresIOConnector;
    @Before
    public void setUp(){
        customPipelineOptions = PipelineOptionsFactory.fromArgs("--pathToCsvFile=../../src/test/TestAAPL.csv","--ticker=AAPL").as(Main.CustomPipelineOptions.class);
    }

    @Test
     public void testCsvDataTo10DayMovingAvg() {
        Pipeline testPipeline = TestPipeline.create(customPipelineOptions);
        FakePostGresIOConnector fakePostGresIOConnector = new FakePostGresIOConnector(customPipelineOptions);
        CsvDataTo10DayMovingAvg csvDataTo10DayMovingAvg = new CsvDataTo10DayMovingAvg(testPipeline,fakePostGresIOConnector);
        PCollection<KV<Instant,Float>> actualPersistedData = csvDataTo10DayMovingAvg.getData();
        Map<Instant,Float> expectedPersistedData = fakePostGresIOConnector.getPersistedData();
        PAssert.thatMap(actualPersistedData).isEqualTo(expectedPersistedData);
        testPipeline.run().waitUntilFinish();
     }
}

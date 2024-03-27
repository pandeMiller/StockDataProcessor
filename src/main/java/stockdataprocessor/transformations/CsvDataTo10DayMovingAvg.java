package stockdataprocessor.transformations;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stockdataprocessor.Main.CustomPipelineOptions;
import stockdataprocessor.PostGresIOConnector;

import java.util.List;

public class CsvDataTo10DayMovingAvg {
    private static Logger logger = LoggerFactory.getLogger(CsvDataTo10DayMovingAvg.class.getName());
    private static int windowSize = 10;
    private static int newWindowStartFreq = 1;

    private PostGresIOConnector postGresIOConnector;

    private Pipeline pipeline;
    public CsvDataTo10DayMovingAvg(Pipeline pipeline, PostGresIOConnector postGresIOConnector){
        this.pipeline = pipeline;
        this.postGresIOConnector = postGresIOConnector;
    }

    public PCollection<KV<Instant,Float>> getData(){
        CustomPipelineOptions customPipelineOptions = (CustomPipelineOptions)pipeline.getOptions();
        PCollection<Float> floatPCollection=  pipeline.apply(TextIO.read().from(customPipelineOptions.getPathToCsvFile()))
                .apply(ParDo.of(new AddTimeStamps())).apply(Window.into(SlidingWindows.of(Duration.standardDays(windowSize)).every(Duration.standardDays(newWindowStartFreq))));
        PCollection<KV<Instant,Float>> input = floatPCollection.apply(ParDo.of(new GroupByWindow())).apply(GroupByKey.create()).apply(ParDo.of(new MeanPerWindow())).apply(ParDo.of(new LogOutput<>("PCollection numbers after Mean transform: ")));
        return postGresIOConnector.persistRecords(input);
    }

    public static class LogOutput<T> extends DoFn<T, T> {
        private final String prefix;

        public LogOutput(String prefix) {
            this.prefix = prefix;
        }

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            logger.info(prefix + c.element());
            c.output(c.element());
        }
    }

    public static class GroupByWindow extends DoFn<Float, KV<IntervalWindow,Float>> {
        @ProcessElement
        public void processElement(ProcessContext c, BoundedWindow window){
            IntervalWindow intervalWindow = (IntervalWindow) window;
            c.output(KV.of(intervalWindow, c.element()));
        }
    }

    public static class MeanPerWindow extends DoFn<KV<IntervalWindow,Iterable<Float>>, KV<Instant,Float>> {
        @ProcessElement
        public void processElement(ProcessContext c){
            IntervalWindow window = c.element().getKey();
            List<Float> data = (List) c.element().getValue();
            float mean = (float) data.stream().mapToDouble(d -> d).average().orElse(0.0);
            c.output(KV.of(window.maxTimestamp(),mean));
        }
    }

}



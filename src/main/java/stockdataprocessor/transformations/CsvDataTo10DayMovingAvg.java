package stockdataprocessor.transformations;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
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

import javax.sql.DataSource;
import java.sql.Date;
import java.util.List;

public class CsvDataTo10DayMovingAvg {
    private static Logger logger = LoggerFactory.getLogger(CsvDataTo10DayMovingAvg.class.getName());
    private static int windowSize = 10;
    private static int newWindowStartFreq = 1;

    private Pipeline pipeline;
    public CsvDataTo10DayMovingAvg(Pipeline pipeline){
        this.pipeline = pipeline;
    }

    public void getData(){
        CustomPipelineOptions customPipelineOptions = (CustomPipelineOptions)pipeline.getOptions();
        String ticker = customPipelineOptions.getTicker();
        PCollection<Float> floatPCollection=  pipeline.apply(TextIO.read().from(customPipelineOptions.getPathToCsvFile()))
                .apply(ParDo.of(new AddTimeStamps())).apply(Window.into(SlidingWindows.of(Duration.standardDays(windowSize)).every(Duration.standardDays(newWindowStartFreq))));
        floatPCollection.apply(ParDo.of(new GroupByWindow())).apply(GroupByKey.create()).apply(ParDo.of(new MeanPerWindow())).apply(ParDo.of(new LogOutput<>("PCollection numbers after Mean transform: ")))
                .apply(
                JdbcIO.< KV<Instant,Float>>write().withDataSourceProviderFn(new PostgresDataSourceFn())

        .withStatement("insert into ten_day_moving_avg_high(date,avg_high,ticker_symbol) values(?, ?, ?)").withPreparedStatementSetter((JdbcIO.PreparedStatementSetter<KV<Instant, Float>>) (element, query) -> {
            query.setDate(1, new Date(element.getKey().getMillis()));
            query.setFloat(2,element.getValue());
            query.setString(3,ticker);
        }));
    }

    private static class PostgresDataSourceFn implements SerializableFunction<Void, DataSource> {
        private static transient DataSource dataSource;

        @Override
        public synchronized DataSource apply(Void input) {
            if (dataSource == null) {
                dataSource = JdbcIO.DataSourceConfiguration.create("org.postgresql.Driver", "jdbc:postgresql://localhost:5432/postgres?schema=PUBLIC").withUsername("postgres").withPassword("password").buildDatasource();
            }
            return dataSource;
        }
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



package stockdataprocessor;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import stockdataprocessor.transformations.CsvDataTo10DayMovingAvg;

public class Main {
    public interface CustomPipelineOptions extends PipelineOptions{

        @Description("Path to the csv file with the stock data")
        String getPathToCsvFile();
        void setPathToCsvFile(String value);

        @Description("Ticker symbol")
        String getTicker();
        void setTicker(String value);

    }
    public static void main(String[] args) {
        PipelineOptions options =PipelineOptionsFactory.fromArgs(args).withValidation().as(CustomPipelineOptions.class);
        Pipeline pipeline = Pipeline.create(options);
        CsvDataTo10DayMovingAvg csvDataTo10DayMovingAvg = new CsvDataTo10DayMovingAvg(pipeline);
        csvDataTo10DayMovingAvg.getData();
        pipeline.run().waitUntilFinish();
    }
}

package stockdataprocessor;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import javax.sql.DataSource;
import java.sql.Date;
import org.joda.time.Instant;

public class PostGresIOConnector {
    private Main.CustomPipelineOptions customPipelineOptions;
    public PostGresIOConnector(Main.CustomPipelineOptions customPipelineOptions){
        this.customPipelineOptions = customPipelineOptions;
    }

    public PCollection<KV<Instant,Float>> persistRecords(PCollection<KV<Instant,Float>> input){
        String ticker = customPipelineOptions.getTicker();
        input.apply(JdbcIO.< KV<Instant,Float>>write().withDataSourceProviderFn(new PostgresDataSourceFn())

                .withStatement("insert into ten_day_moving_avg_high(date,avg_high,ticker_symbol) values(?, ?, ?)").withPreparedStatementSetter((JdbcIO.PreparedStatementSetter<KV<org.joda.time.Instant, Float>>) (element, query) -> {
                    query.setDate(1, new Date(element.getKey().getMillis()));
                    query.setFloat(2,element.getValue());
                    query.setString(3,ticker);
                }));
        return input;
    }

    private static class PostgresDataSourceFn implements SerializableFunction<Void, DataSource> {
        private static transient DataSource dataSource;

        @Override
        public synchronized DataSource apply(Void input) {
            if (dataSource == null) {
                dataSource = JdbcIO.DataSourceConfiguration.create("org.postgresql.Driver", "jdbc:postgresql://localhost:5432/postgres").withUsername("postgres").withPassword("password").buildDatasource();
            }
            return dataSource;
        }
    }
}

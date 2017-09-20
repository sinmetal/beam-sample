package org.sinmetal.beam.examples;

import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.Value;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

/**
 * Created by sinmetal on 2017/09/01.
 */
public class StorageToDatastore {

    public interface CSVToDatastoreOptions extends GcpOptions {

        @Description("Input File Path. Example gs://hoge/hoge.csv")
        @Default.String("ga://hoge/data.csv")
        String getInputFile();
        void setInputFile(String value);
    }

    static class CSVToEntityFn extends DoFn<String, Entity> {

        @ProcessElement
        public void processElement(ProcessContext c) {
            String[] columns = c.element().split(",");

            Key.Builder keyBuilder = Key.newBuilder();
            Key.PathElement pathElement = keyBuilder.addPathBuilder().setKind("Item").setId(Long.parseLong(columns[0])).build();
            Key key = keyBuilder.setPath(0, pathElement).build();

            Entity.Builder entityBuilder = Entity.newBuilder();
            entityBuilder.setKey(key);
            entityBuilder.putProperties("Name", Value.newBuilder().setStringValue(columns[1]).build());
            entityBuilder.putProperties("CategoryId", Value.newBuilder().setIntegerValue(Integer.parseInt(columns[2])).build());
            entityBuilder.putProperties("Price", Value.newBuilder().setIntegerValue(Integer.parseInt(columns[3])).build());
            c.output(entityBuilder.build());
        }
    }

    public static class CSVToDatastore extends PTransform<PCollection<String>, PCollection<Entity>> {
        @Override
        public PCollection<Entity> expand(PCollection<String> lines) {
            return lines.apply(ParDo.of(new CSVToEntityFn()));
        }
    }

    public static void main(String[] args) {
        CSVToDatastoreOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
                .as(CSVToDatastoreOptions.class);
        Pipeline p = Pipeline.create(options);

        p.apply(TextIO.read().from(options.getInputFile())).apply(new CSVToDatastore())
                .apply(DatastoreIO.v1().write().withProjectId(options.getProject()));

        p.run();
    }
}

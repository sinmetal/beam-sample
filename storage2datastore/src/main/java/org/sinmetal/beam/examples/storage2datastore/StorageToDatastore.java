package org.sinmetal.beam.examples.storage2datastore;

import com.google.datastore.v1.Entity;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.*;

import java.util.Map;

/**
 * Created by sinmetal on 2017/09/01.
 */
public class StorageToDatastore {

    public interface CSVToDatastoreOptions extends GcpOptions {

        @Description("Input File Path. Example gs://hoge/hoge.csv")
        @Default.String("ga://hoge/data.csv")
        String getInputFile();
        void setInputFile(String value);

        @Description("Input Category Master File Path. Example gs://hoge/category.csv")
        @Default.String("ga://hoge/category.csv")
        String getCategoryMasterInputFile();
        void setCategoryMasterInputFile(String value);

        @Description("Output Invalid Record File Path. Example gs://hoge/invalid.txt")
        @Default.String("ga://hoge/category.csv")
        String getInvalidOutputFile();
        void setInvalidOutputFile(String value);
    }

    public static class JoinCategoryMaster extends PTransform<PCollection<Entity>, PCollection<Entity>> {

        private final PCollectionView<Map<Integer, String>> categoryMasterView;

        public JoinCategoryMaster(PCollectionView<Map<Integer, String>> categoryMasterView) {
            this.categoryMasterView = categoryMasterView;
        }

        @Override
        public PCollection<Entity> expand(PCollection<Entity> lines) {
            return lines.apply(ParDo.of(new JoinCategoryMasterFn(this.categoryMasterView)).withSideInputs(this.categoryMasterView));
        }
    }

    public static class CSVToDatastore extends PTransform<PCollection<String>, PCollection<Entity>> {
        @Override
        public PCollection<Entity> expand(PCollection<String> lines) {
            return lines.apply(ParDo.of(new CSVToEntityFn()));
        }
    }

    public static class CSVToCategoryMasterMap extends PTransform<PCollection<String>, PCollection<KV<Integer, String>>> {
        @Override
        public PCollection<KV<Integer, String>> expand(PCollection<String> lines) {
            return lines.apply(ParDo.of(new CSVToCategoryKVFn()));
        }
    }

    public static void main(String[] args) {
        final TupleTag<String> validRecordTag = new TupleTag<String>() {private static final long serialVersionUID = 1L;};

        final TupleTag<String> invalidRecordTag = new TupleTag<String>() {private static final long serialVersionUID = 1L;};

        CSVToDatastoreOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
                .as(CSVToDatastoreOptions.class);
        Pipeline p = Pipeline.create(options);

        PCollection<String> category = p.apply("Read Category Master", TextIO.read().from(options.getCategoryMasterInputFile()));
        PCollectionView<Map<Integer, String>> categoryMapView = category.apply("CSV To Category Master Map", new CSVToCategoryMasterMap()).apply(View.asMap());

        PCollectionTuple validOrInvalidRecords = p.apply("Read Item Master", TextIO.read().from(options.getInputFile()))
                .apply("Validation", new ValidationTransform(validRecordTag, invalidRecordTag));
        validOrInvalidRecords.get(validRecordTag)
                .apply("CSV Transfer To Datastore", new CSVToDatastore())
                .apply("Join Category Master", new JoinCategoryMaster(categoryMapView))
                .apply(DatastoreIO.v1().write().withProjectId(options.getProject()));
        validOrInvalidRecords.get(invalidRecordTag)
                .apply("Output Invalid Record", TextIO.write().to(options.getInvalidOutputFile()));

        p.run();
    }
}

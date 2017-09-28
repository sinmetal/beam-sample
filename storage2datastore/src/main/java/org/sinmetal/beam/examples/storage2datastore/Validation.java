package org.sinmetal.beam.examples.storage2datastore;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

public class Validation extends PTransform<PCollection<String>, PCollectionTuple> {

    final TupleTag<String> validRecordTag;

    final TupleTag<String> invalidRecordTag;

    public Validation(TupleTag<String> validRecordTag, TupleTag<String> invalidRecordTag) {
        this.validRecordTag = validRecordTag;
        this.invalidRecordTag = invalidRecordTag;
    }

    @Override
    public PCollectionTuple expand(PCollection<String> lines) {
        // TODO こいつをクラスに分離してUnitTest書くぞ！
        return lines.apply(ParDo.of(new DoFn<String, String>(){
            @ProcessElement
            public void processElement(ProcessContext c) {
                String[] columns = c.element().split(",");
                if (columns.length != 4) {
                    c.output(validRecordTag, c.element());
                } else {
                    c.output(invalidRecordTag, c.element());
                }
            }
        }).withOutputTags(validRecordTag, TupleTagList.of(invalidRecordTag)));
    }
}

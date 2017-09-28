package org.sinmetal.beam.examples.storage2datastore;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

public class ValidationTransform extends PTransform<PCollection<String>, PCollectionTuple> {

    final TupleTag<String> validRecordTag;

    final TupleTag<String> invalidRecordTag;

    public ValidationTransform(TupleTag<String> validRecordTag, TupleTag<String> invalidRecordTag) {
        this.validRecordTag = validRecordTag;
        this.invalidRecordTag = invalidRecordTag;
    }

    @Override
    public PCollectionTuple expand(PCollection<String> lines) {
        return lines.apply(ParDo.of(new ValidationFn(validRecordTag, invalidRecordTag)).withOutputTags(validRecordTag, TupleTagList.of(invalidRecordTag)));
    }
}

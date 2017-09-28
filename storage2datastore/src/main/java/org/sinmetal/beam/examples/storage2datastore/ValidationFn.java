package org.sinmetal.beam.examples.storage2datastore;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;

public class ValidationFn extends DoFn<String, String> {

    final TupleTag<String> validRecordTag;

    final TupleTag<String> invalidRecordTag;

    public ValidationFn(TupleTag<String> validRecordTag, TupleTag<String> invalidRecordTag) {
        this.validRecordTag = validRecordTag;
        this.invalidRecordTag = invalidRecordTag;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        String[] columns = c.element().split(",");
        if (columns.length == 4) {
            c.output(validRecordTag, c.element());
        } else {
            c.output(invalidRecordTag, c.element());
        }
    }
}

package org.sinmetal.beam.examples.storage2datastore;

import org.apache.beam.sdk.transforms.DoFnTester;
import org.apache.beam.sdk.values.TupleTag;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Created by sinmetal on 2017/09/28.
 */
public class ValidationFnTest {

    @Test
    public void testValidationFn() throws Exception {
        final TupleTag<String> validRecordTag = new TupleTag<>();

        final TupleTag<String> invalidRecordTag = new TupleTag<>();

        DoFnTester<String, String> extractValidationFn =
                DoFnTester.of(new ValidationFn(validRecordTag, invalidRecordTag));

        final String validRecord = "1,GCPUG標準Tシャツ,1,1500";
        extractValidationFn.processBundle(validRecord);

        final String invalidRecord = "invalid!!!!";
        extractValidationFn.processBundle(invalidRecord);

        Assert.assertThat(extractValidationFn.getMutableOutput(validRecordTag).size(), CoreMatchers.is(1));
        Assert.assertThat(extractValidationFn.getMutableOutput(validRecordTag).get(0).getValue(), CoreMatchers.is(validRecord));

        Assert.assertThat(extractValidationFn.getMutableOutput(invalidRecordTag).size(), CoreMatchers.is(1));
        Assert.assertThat(extractValidationFn.getMutableOutput(invalidRecordTag).get(0).getValue(), CoreMatchers.is(invalidRecord));
    }
}

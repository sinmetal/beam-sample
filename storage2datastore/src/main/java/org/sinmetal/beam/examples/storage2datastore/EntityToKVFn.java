package org.sinmetal.beam.examples.storage2datastore;

import com.google.datastore.v1.Entity;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

/**
 * Created by sinmetal on 2017/09/29.
 */
public class EntityToKVFn extends DoFn<Entity, KV<Long,Entity>> {

    @ProcessElement
    public void processElement(ProcessContext c) {
        c.output(KV.of(c.element().getPropertiesMap().get("CategoryId").getIntegerValue(), c.element()));
    }
}

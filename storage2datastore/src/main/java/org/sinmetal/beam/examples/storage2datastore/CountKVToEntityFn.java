package org.sinmetal.beam.examples.storage2datastore;

import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.Value;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

/**
 * Created by sinmetal on 2017/10/01.
 */
public class CountKVToEntityFn extends DoFn<KV<Long,Long>, Entity> {
    @ProcessElement
    public void processElement(ProcessContext c) {

        Key.Builder keyBuilder = Key.newBuilder();
        Key.PathElement pathElement = keyBuilder.addPathBuilder().setKind("CountItemCategory").setId(c.element().getKey()).build();
        Key key = keyBuilder.setPath(0, pathElement).build();

        Entity.Builder entityBuilder = Entity.newBuilder();
        entityBuilder.setKey(key);
        entityBuilder.putProperties("Count", Value.newBuilder().setIntegerValue(c.element().getValue()).build());
        c.output(entityBuilder.build());
    }
}

package org.sinmetal.beam.examples.storage2datastore;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.language.v1.Token;
import com.google.datastore.v1.ArrayValue;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.Value;
import com.google.gson.Gson;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import org.apache.avro.data.Json;
import org.apache.beam.sdk.transforms.DoFn;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Created by sinmetal on 2017/09/21.
 */
public class NLTokenToEntity extends DoFn<List<Token>, String> {

    public static class NLPojo {
        public String Text;
        public int beginOffset;
        public String lemma;
        public String tag;
        public String aspect;
        public String caseValue;
        public String form;
        public String gender;
        public String mood;
        public String number;
        public String person;
        public String proper;
        public String reciprocity;
        public String tense;
        public String voice;
        public int headTokenIndex;
        public String label;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        List<NLPojo> pojos = new ArrayList<>();

        for (Token token : c.element()) {
            NLPojo pojo = new NLPojo();

            pojo.Text = token.getText().getContent();
            pojo.beginOffset = token.getText().getBeginOffset();
            pojo.lemma = token.getLemma();
            pojo.tag = token.getPartOfSpeech().getTag().toString();
            pojo.aspect = token.getPartOfSpeech().getAspect().toString();
            pojo.caseValue =token.getPartOfSpeech().getCase().toString();
            pojo.form = token.getPartOfSpeech().getForm().toString();
            pojo.gender = token.getPartOfSpeech().getGender().toString();
            pojo.mood = token.getPartOfSpeech().getMood().toString();
            pojo.number = token.getPartOfSpeech().getNumber().toString();
            pojo.person = token.getPartOfSpeech().getPerson().toString();
            pojo.proper = token.getPartOfSpeech().getProper().toString();
            pojo.reciprocity = token.getPartOfSpeech().getReciprocity().toString();
            pojo.tense = token.getPartOfSpeech().getTense().toString();
            pojo.voice = token.getPartOfSpeech().getVoice().toString();
            pojo.headTokenIndex = token.getDependencyEdge().getHeadTokenIndex();
            pojo.label = token.getDependencyEdge().getLabel().toString();

            pojos.add(pojo);
        }

        Gson gson = new Gson();
        String json = gson.toJson(pojos);

        c.output(json);
    }
}

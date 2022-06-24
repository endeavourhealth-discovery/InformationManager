package org.endeavourhealth.informationmanager.utils.sets;

import org.endeavourhealth.imapi.model.tripletree.TTArray;
import org.endeavourhealth.imapi.model.tripletree.TTDocument;
import org.endeavourhealth.imapi.model.tripletree.TTEntity;
import org.endeavourhealth.imapi.model.tripletree.TTValue;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;


@RunWith(JUnitPlatform.class)
class SetsTypeFixerTest {

    @Test
    void testType() {
        List<String> sets = new ArrayList<>();
        sets.add("http://endhealth.info/im#CSET_Covid0");
        TTManager manager = new TTManager();
        TTDocument document = manager.createDocument();
        document
                .addEntity(new TTEntity()
                        .setIri("http://endhealth.info/im#CSET_Covid0")
                        .setName("Covid related value sets"))
                .addEntity(new TTEntity()
                        .setIri("http://endhealth.info/im#CSET_HospValueSets")
                        .setName("Concept sets - Hospital encounters"));

        TTDocument testDocument = manager.createDocument();
        testDocument
                .addEntity(new TTEntity()
                        .setIri("http://endhealth.info/im#CSET_Covid0")
                        .setName("Covid related value sets")
                        .setType(new TTArray().add(IM.FOLDER)))
                .addEntity(new TTEntity()
                        .setIri("http://endhealth.info/im#CSET_HospValueSets")
                        .setName("Concept sets - Hospital encounters"));

        TTValue expected = testDocument.getEntities().get(0).getType().getElements().get(0);

        TTValue actual =  new SetsTypeFixer().fixSetsTypes(sets,document).getEntities().get(0).getType().getElements().get(0);

        System.out.println(actual.asIriRef().getIri());
        assertEquals(expected, actual);
        
    }





}
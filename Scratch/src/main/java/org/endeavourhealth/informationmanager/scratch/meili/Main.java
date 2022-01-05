package org.endeavourhealth.informationmanager.scratch.meili;

import com.fasterxml.jackson.core.JsonProcessingException;

public class Main {
    public static void main(String[] argv) throws JsonProcessingException, InterruptedException {
        new OpenSearchSender().execute();
    }
}

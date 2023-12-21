package org.endeavourhealth.plugins;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.List;

public class VocabDef {
    public static class Entry {
        public String name;
        public JsonNode value;
    }

    public String name;
    public List<Entry> entries;
}

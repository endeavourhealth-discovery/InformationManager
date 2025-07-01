package org.endeavourhealth.qofextractor;

import java.util.ArrayList;
import java.util.List;

public class QOFDocument {
    private String name;
    private List<Selection> selections = new ArrayList<>();
    private List<Register> registers = new ArrayList<>();
    private List<ExtractionField> extractionFields = new ArrayList<>();

    public String getName() {
        return name;
    }

    public QOFDocument setName(String name) {
        this.name = name;
        return this;
    }

    public List<Selection> getSelections() {
        return selections;
    }

    public QOFDocument setSelections(List<Selection> selections) {
        this.selections = selections;
        return this;
    }

    public List<Register> getRegisters() {
        return registers;
    }

    public QOFDocument setRegisters(List<Register> registers) {
        this.registers = registers;
        return this;
    }

    public List<ExtractionField> getExtractionFields() {
        return extractionFields;
    }

    public QOFDocument setExtractionFields(List<ExtractionField> extractionFields) {
        this.extractionFields = extractionFields;
        return this;
    }
}

package org.endeavourhealth.informationmanager.scratch.opensearch;

import org.endeavourhealth.imapi.model.tripletree.TTIriRef;

import java.util.ArrayList;
import java.util.List;

public class OpenSearchDocument {
    Integer id;
    String iri;
    String name;
    String code;
    TTIriRef scheme;
    List<TTIriRef> entityType = new ArrayList<>();
    TTIriRef status;

    public Integer getId() {
        return id;
    }

    public OpenSearchDocument setId(Integer id) {
        this.id = id;
        return this;
    }

    public String getIri() {
        return iri;
    }

    public OpenSearchDocument setIri(String iri) {
        this.iri = iri;
        return this;
    }

    public String getName() {
        return name;
    }

    public OpenSearchDocument setName(String name) {
        this.name = name;
        return this;
    }

    public String getCode() {
        return code;
    }

    public OpenSearchDocument setCode(String code) {
        this.code = code;
        return this;
    }

    public TTIriRef getScheme() {
        return scheme;
    }

    public OpenSearchDocument setScheme(TTIriRef scheme) {
        this.scheme = scheme;
        return this;
    }

    public List<TTIriRef> getEntityType() {
        return entityType;
    }

    public OpenSearchDocument setEntityType(List<TTIriRef> entityType) {
        this.entityType = entityType;
        return this;
    }

    public TTIriRef getStatus() {
        return status;
    }

    public OpenSearchDocument setStatus(TTIriRef status) {
        this.status = status;
        return this;
    }

    public OpenSearchDocument addType(TTIriRef type) {
        this.entityType.add(type);
        return this;
    }
}

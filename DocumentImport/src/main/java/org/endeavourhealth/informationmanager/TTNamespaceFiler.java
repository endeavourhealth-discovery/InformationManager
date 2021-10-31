package org.endeavourhealth.informationmanager;

import org.endeavourhealth.imapi.model.tripletree.TTPrefix;

import java.util.List;
import java.util.Map;

public interface TTNamespaceFiler {
    void fileNamespaces(List<TTPrefix> namespaces, Map<String, String> prefixMap) throws TTFilerException;
}

package org.endeavourhealth.informationmanager.transforms.models;

import org.endeavourhealth.imapi.vocabulary.GRAPH;
import org.endeavourhealth.imapi.vocabulary.QR;
import org.endeavourhealth.imapi.vocabulary.SNOMED;

import java.util.*;

public class TTImportConfig {
  private String folder = ".";
  private String importType = null;
  private boolean secure = false;
  private boolean skiptct = false;
  private boolean skipsearch = false;
  private boolean skipdelete = false;
  private boolean skiplucene = false;
  private boolean skipBulk = false;
  private String resourceFolder;
  private List<String> graph = new ArrayList<>();
  private String singleEntity;




  public String getSingleEntity() {
    return singleEntity;
  }

  public TTImportConfig setSingleEntity(String singleEntity) {
    this.singleEntity = singleEntity;
    return this;
  }

  public List<String> getGraph() {
    return graph;
  }

  public TTImportConfig setGraph(List<String> graph) {
    this.graph = graph;
    return this;
  }
    public TTImportConfig addGraph (String includeGraph){
      if (this.graph == null) {
        this.graph = new ArrayList<>();
      }
      this.graph.add(includeGraph);
      return this;
    }





  public String getResourceFolder() {
    return resourceFolder;
  }

  public TTImportConfig setResourceFolder(String resourceFolder) {
    this.resourceFolder = resourceFolder;
    return this;
  }

  public boolean isSkipBulk() {
    return skipBulk;
  }

  public TTImportConfig setSkipBulk(boolean skipBulk) {
    this.skipBulk = skipBulk;
    return this;
  }

  public boolean isSkiplucene() {
    return skiplucene;
  }

  public TTImportConfig setSkiplucene(boolean skiplucene) {
    this.skiplucene = skiplucene;
    return this;
  }

  public String getFolder() {
    return folder;
  }

  public TTImportConfig setFolder(String folder) {
    this.folder = folder;
    return this;
  }

  public String getImportType() {
    return importType;
  }

  public TTImportConfig setImportType(String importType) {
    this.importType = importType;
    return this;
  }

  public boolean isSecure() {
    return secure;
  }

  public TTImportConfig setSecure(boolean secure) {
    this.secure = secure;
    return this;
  }

  public boolean isSkiptct() {
    return skiptct;
  }

  public TTImportConfig setSkiptct(boolean skiptct) {
    this.skiptct = skiptct;
    return this;
  }

  public boolean isSkipsearch() {
    return skipsearch;
  }

  public TTImportConfig setSkipsearch(boolean skipsearch) {
    this.skipsearch = skipsearch;
    return this;
  }

  public boolean isSkipdelete() {
    return skipdelete;
  }

  public TTImportConfig setSkipdelete(boolean skipdelete) {
    this.skipdelete = skipdelete;
    return this;
  }
}

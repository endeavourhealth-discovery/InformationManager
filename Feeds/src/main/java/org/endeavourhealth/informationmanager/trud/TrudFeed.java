package org.endeavourhealth.informationmanager.trud;

import java.util.function.Consumer;

public class TrudFeed {
  private String name;
  private String id;
  private String remoteVersion;
  private String download;
  private boolean updated = false;

  private Consumer<TrudFeed> postProcess;

  public TrudFeed(String name, String id) {
    this(name, id, null);
  }

  public TrudFeed(String name, String id, Consumer<TrudFeed> postProcess) {
    this.name = name;
    this.id = id;
    this.postProcess = postProcess;
  }

  public String getName() {
    return name;
  }

  public TrudFeed setName(String name) {
    this.name = name;
    return this;
  }

  public String getId() {
    return id;
  }

  public TrudFeed setId(String id) {
    this.id = id;
    return this;
  }

  public String getRemoteVersion() {
    return remoteVersion;
  }

  public TrudFeed setRemoteVersion(String remoteVersion) {
    this.remoteVersion = remoteVersion;
    return this;
  }

  public String getDownload() {
    return download;
  }

  public TrudFeed setDownload(String download) {
    this.download = download;
    return this;
  }

  public boolean getUpdated() {
    return updated;
  }

  public TrudFeed setUpdated(boolean updated) {
    this.updated = updated;
    return this;
  }

  public void runPostProcess() {
    if (this.postProcess != null)
      this.postProcess.accept(this);
  }

}

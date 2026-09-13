package org.endeavourhealth.informationmanager.transforms.models;

import org.endeavourhealth.imapi.model.imq.Query;

import java.util.List;

public class ColumnGroups {
	private String type;
	private List<Query> columnGroups;
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public List<Query> getColumnGroups() {
		return columnGroups;
	}
	public void setColumnGroups(List<Query> columnGroups) {
		this.columnGroups = columnGroups;
	}
}

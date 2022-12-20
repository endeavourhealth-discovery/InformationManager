package org.endeavourhealth.informationmanager.transforms.sources;

public class EmisCode {
	String codeId;
	String term;
	String code;
	String conceptId;
	String descid;
	String parentId;
	String snomedDescripton;

	public String getSnomedDescripton() {
		return snomedDescripton;
	}

	public EmisCode setSnomedDescripton(String snomedDescripton) {
		this.snomedDescripton = snomedDescripton;
		return this;
	}

	public String getCodeId() {
		return codeId;
	}

	public EmisCode setCodeId(String codeId) {
		this.codeId = codeId;
		return this;
	}

	public String getTerm() {
		return term;
	}

	public EmisCode setTerm(String term) {
		this.term = term;
		return this;
	}

	public String getCode() {
		return code;
	}

	public EmisCode setCode(String code) {
		this.code = code;
		return this;
	}

	public String getConceptId() {
		return conceptId;
	}

	public EmisCode setConceptId(String conceptId) {
		this.conceptId = conceptId;
		return this;
	}

	public String getDescid() {
		return descid;
	}

	public EmisCode setDescid(String descid) {
		this.descid = descid;
		return this;
	}

	public String getParentId() {
		return parentId;
	}

	public EmisCode setParentId(String parentId) {
		this.parentId = parentId;
		return this;
	}
}

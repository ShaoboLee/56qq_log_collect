package com.wlqq.bigdata.monitor.statistic;

import java.io.Serializable;

import backtype.storm.tuple.Tuple;

public class Result implements Serializable {
	private static final long serialVersionUID = 1L;

	public enum STATUS {
		OK, 
		FAILED_RAWDATA_FORMAT_ERROR,
		FAILED_BY_ES_RESULT,
		FAILED_BY_ES_EXECUTE_EXCEPTION, 
		FAILED_RECOVERABLE,
		FAILED_REJECT_ERROR,
		FAILED_MAPPING_ERROR
	}

	private STATUS status = null;
	private Exception ex = null;
	private String rawJSONDocument = null;


	private String faidedReason = null;
	private Tuple tuple = null;

	public Result(STATUS status) {
		this(status, null, null, null);
	}

	public Result(STATUS status, String rawDocument) {
		this(status, null, rawDocument, null);
	}

	public Result(STATUS status, Exception e, String rawDocument) {
		this(status, e, null, rawDocument);
	}

	public Result(STATUS status, String faildeReason, String rawDocument) {
		this(status, null, faildeReason, rawDocument);
	}

	public Result(STATUS status, Exception e, String faidedReason, String rawDocument) {
		this.status = status;
		this.ex = e;
		this.faidedReason = faidedReason;
		this.rawJSONDocument = rawDocument;
	}

	public STATUS getStatus() {
		return this.status;
	}

	public Result asNoTupleResult() {
		return new Result(this.status, this.ex, this.faidedReason, this.rawJSONDocument);
	}

	public Exception getException() {
		return this.ex;
	}

	public String getFailedReason() {
		return this.faidedReason;
	}
	public void setRawJSONDocument(String rawJSONDocument) {
		this.rawJSONDocument = rawJSONDocument;
	}
	public String getRawJSONDocument() {
		return rawJSONDocument;
	}

	public Tuple getTuple() {
		return this.tuple;
	}

	public void setTuple(Tuple tuple) {
		this.tuple = tuple;
	}

	public String toString() {
		return "Status:" + status + ",Failed reason:" + faidedReason + ",Raw document:[[[" + rawJSONDocument
				+ "]]],Exception:" + ex;
	}
}

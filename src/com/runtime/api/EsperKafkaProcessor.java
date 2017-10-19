package com.runtime.api;

import java.util.ArrayList;
import java.util.List;

public class EsperKafkaProcessor {
	
	private String eventTypes;
	private String epls;
	private String outType;
	private int parallelism;
	
	public EsperKafkaProcessor() {
		this.eventTypes = "";
		this.epls = "";
		this.outType = "";
		this.parallelism = 0;
	}
	
	public int getParallelism() {
		return parallelism;
	}

	public void setParallelism(int parallelism) {
		this.parallelism = parallelism;
	}

	public String getEventTypes() {
		return eventTypes;
	}

	public String getEpls() {
		return epls;
	}

	public String getOutType() {
		return outType;
	}

	public void setEventType(String eventTypes) {
		this.eventTypes = eventTypes;
	}
	
	public void setEpl(String epls) {
		this.epls = epls;
	}
	
	public void setOutType(String outType) {
		this.outType = outType;
	}

}

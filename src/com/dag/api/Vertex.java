package com.dag.api;

import java.util.ArrayList;
import java.util.List;

import com.runtime.api.EsperKafkaProcessor;

public class Vertex {
	
	private int id;
	private String vertexName;
	
	private List<Edge> inputEdges;
	private List<Edge> outputEdges;
	
	private EsperKafkaProcessor processor;
	
	private Vertex(int id) {
		this.id = id;
		this.vertexName = "";
		this.inputEdges = new ArrayList<Edge>();
		this.outputEdges = new ArrayList<Edge>();
	}
	
	public static Vertex create(int id) {
		return new Vertex(id);
	}
	
	public void addInputEdge(Edge edge) {
		inputEdges.add(edge);
	}
	
	public void addOutputEdge(Edge edge) {
		outputEdges.add(edge);
	}
	
	public void setVertexName(String name) {
		this.vertexName = name;
	}
	
	public void setProcessor(EsperKafkaProcessor processor) {
		this.processor = processor;
	}
	
	public int getId() {
		return id;
	}

	public String getVertexName() {
		return vertexName;
	}

	public List<Edge> getInputEdges() {
		return inputEdges;
	}

	public List<Edge> getOutputEdges() {
		return outputEdges;
	}

	public EsperKafkaProcessor getProcessor() {
		return processor;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + id;
		return result;
	}
	
	@Override
	public boolean equals(Object obj) {
		if(this==obj)return true;
		if(obj==null)return false;
		if(this.getClass()!=obj.getClass())return false;
		
		Vertex other = (Vertex) obj;
		if(id!=other.getId())
			return false;
		
		return true;
	}

}

package com.dag.api;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

public class DAG {
	
	private String name;
	
	private Map<Integer, Vertex> vertices;
	private Set<Edge> edges;
	
	private DAG(String name) {
		this.name = name;
		vertices = new HashMap<Integer, Vertex>();
		edges = new HashSet<Edge>();
	}
	
	public static DAG create(String name) {
		return new DAG(name);
	}
	
	public void addVertex(Vertex vertex) {
		if(vertices.containsKey(vertex.getVertexName()))
			throw new IllegalStateException(
			        "Vertex " + vertex.getVertexName() + " already defined!");
		vertices.put(vertex.getId(), vertex);
	}
	
	public void addEdge(Edge edge) {
		/*if(!vertices.containsKey(edge.getInputVertex().getVertexName()))
			throw new IllegalArgumentException(
			        "Input vertex " + edge.getInputVertex() + " doesn't exist!");
		
		if(!vertices.containsKey(edge.getOutputVertex().getVertexName()))
			throw new IllegalArgumentException(
			        "Output vertex " + edge.getOutputVertex() + " doesn't exist!");*/
		
		if(edges.contains(edge))
			throw new IllegalArgumentException(
			        "Edge " + edge + " already defined!");
		
		edge.getInputVertex().addOutputEdge(edge);
		edge.getOutputVertex().addInputEdge(edge);
		
		edges.add(edge);
		
	}

	public String getName() {
		return name;
	}
	
	public Vertex getVertex(int id) {
		return vertices.get(id);
	}
	
	public Set<Vertex> getVertices() {
		Set<Vertex> vs = new HashSet<Vertex>();
		for(int id : vertices.keySet()){
			vs.add(vertices.get(id));
		}
		return vs;
	}
	
	public Queue<Vertex> getVerticesQueue() {
		Queue<Vertex> vertexQueue = new LinkedList<Vertex>();
		
		for(int id : vertices.keySet()){
			vertexQueue.offer(vertices.get(id));
		}
		
		return vertexQueue;
	}
	
	public boolean containsVertex(int id) {
		if(vertices.containsKey(id))
			return true;
		return false;
	}

}

package com.yarn.esper.app;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.log4j.LogManager;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.dag.api.DAG;
import com.dag.api.Edge;
import com.dag.api.Vertex;
import com.kafka.client.KafkaConsumerClient;
import com.kafka.client.KafkaProducerClient;
import com.runtime.api.EsperKafkaProcessor;
import com.yarn.conf.EsperConfiguration;
import com.yarn.conf.MonitorConfiguration;

public class EsperApplicationMaster {
	
	private static final Log LOG = LogFactory.getLog(EsperApplicationMaster.class);
	
	// Configuration
	private Configuration conf;
	
	// Handle to communicate with the Resource Manager
	private AMRMClientAsync<ContainerRequest> amRMClient;
	
	// Handle to communicate with the Node Manager
	private NMClientAsync nmClient;
	
	// Listen to process the response from the Resource Manager
	private ContainerAllocator containerAllocator;
	
	// Listen to process the response from the Node Manager
	private ContainerListener containerListener;
	
	// Application Attempt Id ( combination of attemptId and fail count )
	private ApplicationAttemptId appAttemptId;
	
	// For status update for clients - yet to be implemented
	// Hostname of the container
	private String appMasterHostName;
	// Port on which the app master listens for status updates from clients
	private int appMasterRpcPort;
	// Tracking url to which app master publishes info for clients to monitor
	private String appMasterTrackingUrl;
	
	// App Master configuration
	// Memory to request for the container
	private int containerMemory;
	// VirtualCores to request for the container
	private int containerVCores;
	// Priority of the request
	private int requestPriority;
	
	private String esperEngineJarPath;
	private String esperEngineMainClass;
	
	private String monitorJarPath;
	private String monitorMainClass;
	
	private String kafkaServer;
	
	// Count of failed containers
	private AtomicInteger failedContainers;
	// No. of containers to run shell command on
	private int totalContainers;
	// Allocated container count so that we know how many containers has the RM
	// allocated to us
	private AtomicInteger allocatedContainers;
	// Count of containers already requested from the RM
	// Needed as once requested, we should not request for containers again.
	// Only request for more if the original requirement changes.
	private AtomicInteger requestedContainers;
	// Counter for completed containers ( complete denotes successful or failed )
	private AtomicInteger completedContainers;
	
	// Command line options
	private Options opts;
	
	// Launch threads
	private Map<String, Thread> launchThreads;
	
	//threads is ready to run
	private Map<Integer, Boolean> isCompleted;
	
	//Launched containers
	private Map<ContainerId, Container> launchedContainers;
	
	//containers of every vertex
	private Map<String, Integer> containerVertex;
	
	private String vertexJson;
	private DAG dag;
	private Queue<Vertex> vertexQueue;
	
	private Set<String> nodes;
	private Set<String> unMonitorNodes;
	
	private KafkaProducerClient<String, String> producer;
	private KafkaConsumerClient<String, String> consumer;
	
	private boolean done;
	
	private boolean retry = true;//used to test retry function
	
	public EsperApplicationMaster() {
		conf = new YarnConfiguration();
		conf.setStrings(YarnConfiguration.RM_HOSTNAME, "10.109.253.145");
		
		appMasterHostName = "";
		appMasterRpcPort = -1;
		appMasterTrackingUrl = "";
		
		containerMemory = 16;
		containerVCores = 1;
		requestPriority = 0;
		
		esperEngineJarPath = "";
		esperEngineMainClass = "";
		
		monitorJarPath = "";
		monitorMainClass = "";
		
		Properties prop = new Properties();
		InputStream input = EsperApplicationMaster.class.
				getClassLoader().getResourceAsStream("appMaster.properties");
		
		try {
			prop.load(input);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			LOG.error("read cep properties error", e);
		}
		
		String host = prop.getProperty("kafka.host");
		String port = prop.getProperty("kafka.port");
		kafkaServer = host + ":" + port;
		
		failedContainers = new AtomicInteger();
		totalContainers = 0;
		allocatedContainers = new AtomicInteger();
		requestedContainers = new AtomicInteger();
		completedContainers = new AtomicInteger();
		
		launchThreads = new HashMap<String, Thread>();
		
		isCompleted = new HashMap<Integer, Boolean>();
		
		launchedContainers = new HashMap<ContainerId, Container>();
		
		containerVertex = new HashMap<String, Integer>();
		
		vertexJson = "";
		
		vertexQueue = new LinkedList<Vertex>();
		
		nodes = new HashSet<String>();
		unMonitorNodes = new HashSet<String>();
		
		producer = new KafkaProducerClient<String, String>(kafkaServer);
		producer.addTopic(prop.getProperty("container.id.topic"));
		
		String groupId = prop.getProperty("group.id");
		consumer = new KafkaConsumerClient<String, String>(kafkaServer, groupId);
		consumer.addTopic(prop.getProperty("container.warning.topic"));
		
		opts = new Options();
	}
	
	public boolean init(String[] args) throws ParseException {
		
		opts.addOption("container_memory", true, "Amount of memory in MB to be requested");
		opts.addOption("container_vcores", true, "Amount of virtual cores to be requested");
		opts.addOption("request_priority", true, "Application Priority. Default 0");
		
		opts.addOption("esper_jar_path", true, "The esper jar path");
		opts.addOption("esper_main_class", true, "The esper main class");
		
		opts.addOption("monitor_jar_path", true, "the monitor jar path");
		opts.addOption("monitor_main_class", true, "the monitor main class");

		opts.addOption("vertex_json", true, "The json of the vertex");
		
		CommandLine cliParser = new GnuParser().parse(opts, args);
		
		Map<String, String> sysEnv = System.getenv();
		if (!sysEnv.containsKey(Environment.CONTAINER_ID.name())){
			throw new RuntimeException(ApplicationConstants.APP_SUBMIT_TIME_ENV 
					+ " not set in the environment");
		}else{
			ContainerId containerId = ConverterUtils.toContainerId
					(sysEnv.get(Environment.CONTAINER_ID.name()));
			appAttemptId = containerId.getApplicationAttemptId();
		}
		
		if(!sysEnv.containsKey(ApplicationConstants.APP_SUBMIT_TIME_ENV)){
			throw new RuntimeException(ApplicationConstants.APP_SUBMIT_TIME_ENV 
					+ " not set in the environment");
		}
		
		if(!sysEnv.containsKey(Environment.NM_HOST.name())){
			throw new RuntimeException(Environment.NM_HOST.name() 
					+ " not set in the environment");
		}
		
		if(!sysEnv.containsKey(Environment.NM_HTTP_PORT.name())){
			throw new RuntimeException(Environment.NM_HTTP_PORT.name() 
					+ " not set in the environment");
		}
		
		if(!sysEnv.containsKey(Environment.NM_PORT.name())){
			throw new RuntimeException(Environment.NM_PORT.name() 
					+ " not set in the environment");
		}
		
		LOG.info("Application master for app" + ", appId="
		        + appAttemptId.getApplicationId().getId() + ", clustertimestamp="
		        + appAttemptId.getApplicationId().getClusterTimestamp()
		        + ", attemptId=" + appAttemptId.getAttemptId());
		
		appMasterHostName = NetUtils.getHostname();
		appMasterRpcPort = 0;
		appMasterTrackingUrl = "";
		
		esperEngineJarPath = cliParser.getOptionValue
				("esper_jar_path", "/usr/esper/apps/esper-kafka-engine.jar");
		esperEngineMainClass = cliParser.getOptionValue
				("esper_main_class", "com.esper.kafka.adapter.EsperKafkaAdapter");
		
		monitorJarPath = cliParser.getOptionValue
				("monitor_jar_path", "/usr/hadoop-yarn/monitor/container-resource-monitor.jar");
		monitorMainClass = cliParser.getOptionValue
				("monitor_main_class", "com.container.notification.ContainerNotification");
		
		vertexJson = cliParser.getOptionValue("vertex_json", "{}")
				.replaceAll("%", "\"").replaceAll("$", "\'");
		LOG.info("get vertex json " + vertexJson);
		
		containerMemory = Integer.parseInt(cliParser.getOptionValue("container_memory", "16"));
		containerVCores = Integer.parseInt(cliParser.getOptionValue("container_vcores", "1"));
		requestPriority = Integer.parseInt(cliParser.getOptionValue("request_priority", "0"));
		
		dag = convertToDag(vertexJson);
		
		LOG.info("the total num of containers is " + totalContainers);
		
		containerAllocator = new ContainerAllocator();
		amRMClient = AMRMClientAsync.createAMRMClientAsync(100, containerAllocator);
		
		containerListener = new ContainerListener();
		nmClient = NMClientAsync.createNMClientAsync(containerListener);
		
		return true;
	}
	
	public void runContainers() throws YarnException, IOException {
		
		LOG.info("Starting ApplicationMaster");
		
		amRMClient.init(conf);
		amRMClient.start();
		
		nmClient.init(conf);
		nmClient.start();
		
		// Register self with ResourceManager
	    // This will start heartbeating to the RM
		RegisterApplicationMasterResponse response = amRMClient.registerApplicationMaster
				(appMasterHostName, appMasterRpcPort, appMasterTrackingUrl);
		
		int maxMemory = response.getMaximumResourceCapability().getMemory();
		LOG.info("Max mem capabililty of resources in this cluster " + maxMemory);
		
		int maxVCores = response.getMaximumResourceCapability().getVirtualCores();
		LOG.info("Max vcores capabililty of resources in this cluster " + maxVCores);
		
		if(containerMemory>maxMemory){
			LOG.info("Container memory specified above max threshold of cluster."
			          + " Using max value." + ", specified=" + containerMemory + ", max="
			          + maxMemory);
			containerMemory = maxMemory;
		}
		
		if(containerVCores>maxVCores){
			LOG.info("Container virtual cores specified above max threshold of cluster."
			          + " Using max value." + ", specified=" + containerVCores + ", max="
			          + maxVCores);
			containerVCores = maxVCores;
		}
		
		// Setup ask for containers from RM
	    // Send request for containers to RM
	    // Until we get our fully allocated quota, we keep on polling RM for
	    // containers
	    // Keep looping until all the containers are launched and app
	    // executed on them ( regardless of success/failure).
		for(int i=0;i<totalContainers;i++){
			ContainerRequest containerReuqest = setupContainerAskForRM(null, null, requestPriority, true);
		    amRMClient.addContainerRequest(containerReuqest);
		}
		
		requestedContainers.set(totalContainers);
		
		listenContainerWarning();
		
	}
	
	//Setup the request that will be sent to the RM for the container ask.	
	private ContainerRequest setupContainerAskForRM(String[] nodes, String[] racks, int priority, boolean relaxLocality) {
		
		// set the priority for the request
		Priority pri = Priority.newInstance(priority);
		
		// Set up resource type requirements
		Resource capability = Resource.newInstance(containerMemory, containerVCores);
		ContainerRequest request = new ContainerRequest
				(capability, nodes, racks, pri, relaxLocality);

		LOG.info("Requested container ask: " + request.toString());
		return request;
		
	}
	
	private DAG convertToDag(String json) throws JSONException {
		
		DAG dag = DAG.create("dag");
		
		JSONObject jsonObject = new JSONObject(json);
		JSONArray jsonArray = jsonObject.getJSONArray("vertex");
		
		for(int i=0;i<jsonArray.length();i++){
			JSONObject vertexJson = jsonArray.getJSONObject(i);
			int id = vertexJson.getInt("id");
			
			String name = vertexJson.getString("name");
			Vertex vertex = null;
			if(dag.containsVertex(id)){
				vertex = dag.getVertex(id);
			}else{
				vertex = Vertex.create(id);
				dag.addVertex(vertex);
			}	
			vertex.setVertexName(name);
			JSONArray children = vertexJson.getJSONArray("children");
			for(int j=0;j<children.length();j++){
				int cid = children.getInt(j);
				Vertex child = null;
				if(dag.containsVertex(cid)){
					child = dag.getVertex(cid);
				}else{
					child = Vertex.create(cid);
					dag.addVertex(child);
				}
				Edge edge = Edge.create(vertex, child);
				dag.addEdge(edge);
			}
			
			EsperKafkaProcessor processor = new EsperKafkaProcessor();
			JSONArray eventTypes = vertexJson.getJSONArray("event_types");
			processor.setEventType(eventTypes.toString());
			
			JSONArray epls = vertexJson.getJSONArray("epl");
			processor.setEpl(epls.toString());
			
			processor.setOutType(vertexJson.getString("out_type"));
			processor.setParallelism(1);
			
			vertex.setProcessor(processor);
			
			vertexQueue.offer(vertex);
			isCompleted.put(id, false);
			totalContainers++;
			
		}
		
		return dag;
		
	}
	
	private void listenContainerWarning() {
		
		consumer.subscibe();
		while(!done){
			ConsumerRecords<String, String> records = consumer.consume();
			for(ConsumerRecord<String, String> record : records){
				String containerWarningMsg = record.value();
				LOG.info("receive container warning msg:" + containerWarningMsg);
				JSONObject containerWarningJson = new JSONObject(containerWarningMsg);
				String containerId = containerWarningJson.getString("container_id");
				int vertexId = containerVertex.get(containerId);
				vertexQueue.offer(dag.getVertex(vertexId));
				
				ContainerRequest containerReuqest = setupContainerAskForRM(null, null, requestPriority, true);
			    amRMClient.addContainerRequest(containerReuqest);
			    totalContainers++;
			    requestedContainers.incrementAndGet();
			}
		}
		
		producer.produce(null, "{\"finish\":\"finish\"}");
		
	}
	
	public boolean finish() throws YarnException, IOException {
		
		// wait for completion.
		while(!done && (completedContainers.get()!=totalContainers)){
			try {
				Thread.sleep(200);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		// Join all launched threads
	    // needed for when we time out
	    // and we need to release containers
		for(String containerId : launchThreads.keySet()){
			try {
				launchThreads.get(containerId).join();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				LOG.info("Exception thrown in thread join: " + e.getMessage());
				e.printStackTrace();
			}
		}
		
		// When the application completes, it should stop all running containers
		LOG.info("Application completed. Stopping running containers");
		nmClient.stop();
		
		// When the application completes, it should send a finish application
	    // signal to the RM
		LOG.info("Application completed. Signalling finish to RM");
		
		FinalApplicationStatus appStatus = FinalApplicationStatus.SUCCEEDED;
		String appMessage = "";
		boolean success = true;
		
		if(failedContainers.get()!=0){
			appStatus = FinalApplicationStatus.FAILED;
			appMessage = "total: " + totalContainers
					+ ", failed: " + failedContainers.get();
			LOG.info(appMessage);
			success = false;
		}
		
		try {
			amRMClient.unregisterApplicationMaster(appStatus, appMessage, "");
		} catch (YarnException ex) {
			// TODO: handle exception
			LOG.error("Failed to unregister application", ex);
		} catch (IOException e) {
			LOG.error("Failed to unregister application", e);
		}
		
		amRMClient.stop();
		
		return success;
	}
	
	private class ContainerAllocator implements AMRMClientAsync.CallbackHandler {

		@Override
		public float getProgress() {
			// TODO Auto-generated method stub
			// set progress to deliver to RM on next heartbeat
			return (float) completedContainers.get()/totalContainers;
		}

		@Override
		public void onContainersAllocated(List<Container> containers) {
			// TODO Auto-generated method stub
			LOG.info("Got response from RM for container ask, allocatedCnt="
			          + containers.size());
			allocatedContainers.addAndGet(containers.size());
			for(Container container : containers){
				String nodeHost = container.getNodeId().getHost();
				LOG.info("Launching app on a new container."
			            + ", containerId=" + container.getId()
			            + ", containerNode=" + nodeHost
			            + ":" + container.getNodeId().getPort()
			            + ", containerNodeURI=" + container.getNodeHttpAddress()
			            + ", containerResourceMemory"
			            + container.getResource().getMemory()
			            + ", containerResourceVirtualCores"
			            + container.getResource().getVirtualCores());
				
				launchedContainers.put(container.getId(), container);
				
				if(!nodes.contains(nodeHost)){
					nodes.add(nodeHost);
					unMonitorNodes.add(nodeHost);
					LOG.info("add cluster node:" + nodeHost);
					
					ContainerRequest containerReuqest = setupContainerAskForRM(new String[]{nodeHost}, new String[]{"/default-rack"}, 1, false);
				    amRMClient.addContainerRequest(containerReuqest);
				    LOG.info("ask for a container to monitor the containers on node " + nodeHost);
				}
				
				int vertexId = 0;
				if(!vertexQueue.isEmpty()){
					vertexId = vertexQueue.poll().getId();
					LOG.info("add vertex of id " + vertexId + " to container " + container.getId());
					Thread launchThread = new Thread(new LaunchCEPContainerRunnable(container, vertexId));
					
					// launch and start the container on a separate thread to keep
			        // the main thread unblocked
			        // as all containers may not be allocated at one go.
					launchThreads.put(container.getId().toString(), launchThread);
					containerVertex.put(container.getId().toString(), vertexId);
					
					JSONObject containerIdJson = new JSONObject();
					containerIdJson.put("node_host", nodeHost);
					containerIdJson.put("container_id", container.getId().toString());
					producer.produce(null, containerIdJson.toString());
					
					launchThread.start();	
				}else{
					if(unMonitorNodes.size()!=0){
						LOG.info("start monitor of node " + nodeHost + " with container " + container.getId());
						Thread monitorThread = new Thread
								(new LaunchMonitorContainerRunnable
										(container, containerMemory, nodeHost));
						
						unMonitorNodes.remove(nodeHost);
						
						monitorThread.start();
					}
				}
				
			}
		}

		@Override
		public void onContainersCompleted(List<ContainerStatus> containerStatuses) {
			// TODO Auto-generated method stub
			LOG.info("Got response from RM for container ask, completedCnt="
			          + containerStatuses.size());
			
			int numToRequest = 0;
			for(ContainerStatus containerStatus : containerStatuses){
				LOG.info(appAttemptId + " got container status for containerID="
			            + containerStatus.getContainerId() + ", state="
			            + containerStatus.getState() + ", exitStatus="
			            + containerStatus.getExitStatus() + ", diagnostics="
			            + containerStatus.getDiagnostics());
				
				int exitStatus = containerStatus.getExitStatus();
				String containerId = containerStatus.getContainerId().toString();
				
				if(exitStatus!=ContainerExitStatus.SUCCESS){
					if(exitStatus==ContainerExitStatus.ABORTED){
						// container was killed by framework, possibly preempted
			            // we should re-try as the container was lost for some reason
						if(containerVertex.containsKey(containerId)){
							int vertexId = containerVertex.get(containerId);
							vertexQueue.offer(dag.getVertex(vertexId));
							containerVertex.remove(containerId);
							launchThreads.remove(containerId);
							LOG.info("The vertex id of the failed container is " + vertexId);
							numToRequest++;
						}	
						
					}else{
						// shell script failed
			            // counts as completed
						if(containerVertex.containsKey(containerId)){
							int vertexId = containerVertex.get(containerId);
							LOG.info("The vertex id of the failed container is " + vertexId);
							//setCompleted(vertexId);
							completedContainers.incrementAndGet();
							failedContainers.incrementAndGet();
						}		
					}
				}else{
					if(containerVertex.containsKey(containerId)){
						completedContainers.incrementAndGet();
						int vertexId = containerVertex.get(containerId);
						//setCompleted(vertexId);
						LOG.info("The vertex id of the completed container is " + vertexId);
					}		
					LOG.info("Container completed successfully." + ", containerId="
				              + containerId);	
				}
			}
			
			if(numToRequest!=0)LOG.info("ask for " + numToRequest + " containers for unsuccess containers");
			
			for(int i=0;i<numToRequest;i++){
				ContainerRequest containerAsk = setupContainerAskForRM(null, null, requestPriority, true);
		        amRMClient.addContainerRequest(containerAsk);
			}
			
			requestedContainers.addAndGet(numToRequest);
			totalContainers = totalContainers + numToRequest;
			
			if(completedContainers.get()==totalContainers)done = true;
			
		}

		@Override
		public void onError(Throwable arg0) {
			// TODO Auto-generated method stub
			LOG.info("something error on AMRMClientAsync.CallbackHandler ", arg0);
			done = true;
			amRMClient.stop();
		}

		@Override
		public void onNodesUpdated(List<NodeReport> arg0) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void onShutdownRequest() {
			// TODO Auto-generated method stub
			done = true;
		}
		
	}
	
	private class ContainerListener implements NMClientAsync.CallbackHandler {

		@Override
		public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> arg1) {
			// TODO Auto-generated method stub
			
			LOG.info("Succeeded to start Container " + containerId);
			if(launchedContainers.containsKey(containerId))
				nmClient.getContainerStatusAsync(containerId, 
						launchedContainers.get(containerId).getNodeId());
			
		}

		@Override
		public void onContainerStatusReceived(ContainerId containerId, ContainerStatus containerStatus) {
			// TODO Auto-generated method stub
			
			LOG.info("Container Status: id=" + containerId + ", status=" +
		            containerStatus);
			/*if(retry){
				nmClient.stopContainerAsync(containerId, 
						launchedContainers.get(containerId).getNodeId());
				retry = false;
			}*/
			
		}

		@Override
		public void onContainerStopped(ContainerId containerId) {
			// TODO Auto-generated method stub
			
			LOG.info("Succeeded to stop Container " + containerId);
			
		}

		@Override
		public void onGetContainerStatusError(ContainerId containerId, Throwable arg1) {
			// TODO Auto-generated method stub
			
			LOG.error("Failed to query the status of Container " + containerId, arg1);
			
		}

		@Override
		public void onStartContainerError(ContainerId containerId, Throwable arg1) {
			// TODO Auto-generated method stub
			
			LOG.error("Failed to start Container " + containerId, arg1);
			if(containerVertex.containsKey(containerId)){
				completedContainers.incrementAndGet();
				failedContainers.incrementAndGet();
			}			
			
		}

		@Override
		public void onStopContainerError(ContainerId containerId, Throwable arg1) {
			// TODO Auto-generated method stub
			
			LOG.error("Failed to stop Container " + containerId, arg1);
			
		}
		
	}
	
	private synchronized void setCompleted(int id) {
		LOG.info("set vertex " + id + " to completed");
		isCompleted.replace(id, true);
		notifyAll();
	}
	
	private synchronized void waitForCompleted(int id) {
		boolean wait = true;
		while(true){
			Vertex v = dag.getVertex(id);
			wait = false;
			for(Edge edge : v.getInputEdges()){
				if(!isCompleted.get(edge.getInputVertex().getId()))
					wait = true;
			}
			if(!wait)break;
			try {
				wait();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	private class LaunchCEPContainerRunnable implements Runnable {
		
		// Allocated container
		private Container container;
		private int vertexId;
		
		public LaunchCEPContainerRunnable(Container container, int vertexId) {
			this.container = container;
			this.vertexId = vertexId;
		}

		@Override
		public void run() {
			// TODO Auto-generated method stub
			
			/*for(Edge edge : dag.getVertex(vertexId).getInputEdges()){
				if(launchThreads.containsKey(edge.getInputVertex().getId())){
					try {
						launchThreads.get(edge.getInputVertex().getId()).join();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}*/
			
			//waitForCompleted(vertexId);
			
			LOG.info("Setting up container launch container for containerid="
			          + container.getId());
			
			// Set the local resources
			/*Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
			try {
				FileSystem fs = FileSystem.get(conf);
				Path esperJarPath = new Path(esperEngineJarPath);
				FileStatus jarStatus = fs.getFileStatus(esperJarPath);
				LocalResource esperJar = Records.newRecord(LocalResource.class);
				esperJar.setResource(ConverterUtils.getYarnUrlFromPath(esperJarPath));
				esperJar.setSize(jarStatus.getLen());
				esperJar.setTimestamp(jarStatus.getModificationTime());
				esperJar.setType(LocalResourceType.FILE);
				esperJar.setVisibility(LocalResourceVisibility.PUBLIC);
				localResources.put("esperJar", esperJar);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				LOG.error("Read esper jar file error", e);
				completedContainers.incrementAndGet();
				failedContainers.incrementAndGet();
			}*/

			// Set the environment
			Map<String, String> esperEnv = new HashMap<String, String>();
			StringBuilder esperClasspath = new StringBuilder(Environment.CLASSPATH.$());
			esperClasspath.append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./*");
			for(String c : EsperConfiguration.DEFAULT_ESPER_APPLICATION_CLASSPARH){
				esperClasspath.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
				esperClasspath.append(c.trim());
			}
			esperClasspath.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
			esperClasspath.append(esperEngineJarPath);
			esperEnv.put("CLASSPATH", esperClasspath.toString());
			
			LOG.info("Complete setting up esper env " + esperClasspath.toString());
			
			
			Vertex v = dag.getVertex(vertexId);
			String eventType = "\'" + v.getProcessor().getEventTypes()
					.replace("\"", "%")+ "\'";
			String epl = "\'" + v.getProcessor().getEpls().toString()
					.replace("\"", "%").replace("\'", "$")+ "\'";
			String outType = v.getProcessor().getOutType();
			String vertexName = v.getVertexName();
			
			StringBuilder c = new StringBuilder();
			c.append("\'[");
			
			for(Edge edge : v.getOutputEdges()){
				c.append("\"").append(edge.getOutputVertex().getVertexName())
				.append("\"").append(",");
			}
			if(v.getOutputEdges().size()==0)c.append("\"vertexend\"");
			c.append("]\'");
			
			String children = c.toString().replace("\"", "%");
			
			StringBuilder p = new StringBuilder();
			p.append("\'[");
			
			for(Edge edge : v.getInputEdges()){
				p.append("\"").append(edge.getInputVertex().getVertexName())
				.append("\"").append(",");
			}
			if(v.getInputEdges().size()==0)p.append("\"vertexstart\"");
			p.append("]\'");
			
			String parents = p.toString().replace("\"", "%");
			
			// Set the necessary command to execute on the allocated container
			List<String> esperCommands = new ArrayList<String>();
			esperCommands.add("$JAVA_HOME/bin/java");
			esperCommands.add("-Xmx" + containerMemory + "M");
			esperCommands.add(esperEngineMainClass);
			// Set params for Application Master
			esperCommands.add("--event_type " + eventType);
			esperCommands.add("--epl " + epl);
			esperCommands.add("--out_type " + outType);
			esperCommands.add("--children " + children);
			esperCommands.add("--parents " + parents);
			esperCommands.add("--vertex_name " + vertexName);
			//esperCommands.add("mkdir /usr/test");
			
			LOG.info("Completed setting up esper engine command " + esperCommands.toString());
			
			// Set up ContainerLaunchContext, setting local resource, environment, command
			ContainerLaunchContext esperContainer = ContainerLaunchContext.newInstance
					(null, esperEnv, esperCommands, null, null, null);
						
			
			nmClient.startContainerAsync(container, esperContainer);
		}
		
	}
	
	private class LaunchMonitorContainerRunnable implements Runnable {
		
		private Container container;
		private int allocatedMem;
		private String nodeHost;
		
		public LaunchMonitorContainerRunnable(Container container, 
				int allocatedMem, String nodeHost) {
			this.container = container;
			this.allocatedMem = allocatedMem;
			this.nodeHost = nodeHost;
		}

		@Override
		public void run() {
			// TODO Auto-generated method stub
			
			LOG.info("Setting up container launch container for containerid="
			          + container.getId());
			
			Map<String, String> monitorEnv = new HashMap<String, String>();
			StringBuilder monitorClasspath = new StringBuilder(Environment.CLASSPATH.$());
			monitorClasspath.append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./*");
			monitorClasspath.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
			monitorClasspath.append(MonitorConfiguration.DEFAULT_MONITOR_APPLICATION_CLASSPATH);
			monitorClasspath.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
			monitorClasspath.append(monitorJarPath);
			monitorEnv.put("CLASSPATH", monitorClasspath.toString());
			
			LOG.info("complete setting monitor env:" + monitorClasspath.toString());
			
			List<String> monitorCommands = new ArrayList<String>();
			monitorCommands.add("$JAVA_HOME/bin/java");
			monitorCommands.add("-Xmx" + containerMemory + "M");
			monitorCommands.add(monitorMainClass);
			
			monitorCommands.add("--app_id " + appAttemptId.getApplicationId().toString());
			monitorCommands.add("--allocated_mem " + allocatedMem);
			monitorCommands.add("--node_host " + nodeHost);
			
			LOG.info("complete setting monitor commands:" + monitorCommands.toString());
			
			ContainerLaunchContext monitorContainer = ContainerLaunchContext.newInstance
					(null, monitorEnv, monitorCommands, null, null, null);
			
			nmClient.startContainerAsync(container, monitorContainer);
		}
		
	}
	
	public static void main(String[] args) {
		
		boolean result = false;
		
		try {
			EsperApplicationMaster appMaster = new EsperApplicationMaster();
			LOG.info("Initializing ApplicationMaster");
			boolean doRun = appMaster.init(args);
			if(!doRun){
				System.exit(0);
			}
			appMaster.runContainers();
			result = appMaster.finish();
		} catch (Throwable t) {
			// TODO: handle exception
			LOG.fatal("Error running ApplicationMaster", t);
			LogManager.shutdown();
		    ExitUtil.terminate(1, t);
		}
		
		if(result){
			LOG.info("Application Master completed successfully. exiting");
			System.exit(0);
		}
		
		LOG.info("Application Master failed. exiting");
		System.exit(2);
		
	}

}

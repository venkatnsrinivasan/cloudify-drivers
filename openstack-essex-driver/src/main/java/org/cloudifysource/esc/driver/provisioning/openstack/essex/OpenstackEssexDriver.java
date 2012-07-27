package org.cloudifysource.esc.driver.provisioning.openstack.essex;


import java.io.IOException;
import java.io.StringReader;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.ws.rs.core.MediaType;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathException;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.commons.lang.StringUtils;
import org.cloudifysource.dsl.cloud.Cloud;
import org.cloudifysource.dsl.cloud.CloudTemplate;
import org.cloudifysource.esc.driver.provisioning.CloudDriverSupport;
import org.cloudifysource.esc.driver.provisioning.CloudProvisioningException;
import org.cloudifysource.esc.driver.provisioning.MachineDetails;
import org.cloudifysource.esc.driver.provisioning.ProvisioningDriver;
import org.cloudifysource.esc.driver.provisioning.openstack.FloatingIP;
import org.cloudifysource.esc.driver.provisioning.openstack.Node;
import org.cloudifysource.esc.driver.provisioning.openstack.OpenstackException;
import org.codehaus.jackson.map.ObjectMapper;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.UniformInterfaceException;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.ClientResponse.Status;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.client.filter.LoggingFilter;
import com.sun.jersey.core.util.Base64;

/**
 * Copied the source for the openstack driver and made changes to use
 * urls provided in the REST responses and make it work with the Essex release.
 * This allocates floating ips manually and doesnt check for auto assign .This is because
 * the essex api has a bug where the floating ip assigned takes a long time to respond.
 * Also this driver uses a 'Rest url' built into the template to check if the machine is
 * UP because openstack's status of ACTIVE can come before the ssh service is active
 * which can cause cloudify to fail.
 * @author Venkat
 *
 */
public class OpenstackEssexDriver extends CloudDriverSupport implements ProvisioningDriver {

	private static final String MACHINE_STATUS_ACTIVE = "ACTIVE";
	private static final int HTTP_NOT_FOUND = 404;
	private static final int INTERNAL_SERVER_ERROR = 500;
	private static final int SERVER_POLLING_INTERVAL_MILLIS = 10 * 1000; // 10 seconds
	private static final int DEFAULT_SHUTDOWN_TIMEOUT_MILLIS = 5 * 60 * 1000; // 5 minutes
	private static final int DEFAULT_TIMEOUT_AFTER_CLOUD_INTERNAL_ERROR = 30 * 1000; // 30 seconds
	private static final String OPENSTACK_OPENSTACK_IDENTITY_ENDPOINT = "openstack.identity.endpoint";
	private static final String OPENSTACK_WIRE_LOG = "openstack.wireLog";
	private static final String OPENSTACK_KEY_PAIR = "openstack.keyPair";
	private static final String OPENSTACK_SECURITYGROUP = "openstack.securityGroup";
	private static final String OPENSTACK_OPENSTACK_ENDPOINT = "openstack.endpoint";
	private static final String OPENSTACK_TENANT = "openstack.tenant";
	private static final String OPENSTACK_USERNAME = "openstack.username";
	private static final String OPENSTACK_PASSWORD = "openstack.password";

	private static final String STARTING_THROTTLING = "The cloud reported an Internal Server Error (status 500)."
			+ " Requests for new machines will be suspended for "
			+ DEFAULT_TIMEOUT_AFTER_CLOUD_INTERNAL_ERROR / 1000 + " seconds";
	private static final String RUNNING_THROTTLING = "Requests for new machines are currently suspended";
	
	private final XPath xpath = XPathFactory.newInstance().newXPath();

	private final Client client;

	private long throttlingTimeout = -1;
	private String serverNamePrefix;
	private String tenant;
	private String username;
	private String password;
	private String endpoint;
	private WebResource service;
	
	private String identityEndpoint;
	private final DocumentBuilderFactory dbf;
	private final Object xmlFactoryMutex = new Object();


	/************
	 * Constructor.
	 * 
	 * @throws ParserConfigurationException
	 */
	public OpenstackEssexDriver() {
		dbf = DocumentBuilderFactory.newInstance();
		dbf.setNamespaceAware(false);

		final ClientConfig config = new DefaultClientConfig();
		this.client = Client.create(config);

	}

	private DocumentBuilder createDocumentBuilder() {
		synchronized (xmlFactoryMutex) {
			// Document builder is not guaranteed to be thread sage
			try {
				// Document builders are not thread safe
				return dbf.newDocumentBuilder();
			} catch (final ParserConfigurationException e) {
				throw new IllegalStateException("Failed to set up XML Parser", e);
			}
		}

	}

	public void close() {
	}


	public String getCloudName() {
		return "openstack-essex";
	}

	@Override
	public void setConfig(final Cloud cloud, final String templateName, final boolean management) {
		super.setConfig(cloud, templateName, management);

		if (this.management) {
			this.serverNamePrefix = this.cloud.getProvider().getManagementGroup();
		} else {
			this.serverNamePrefix = this.cloud.getProvider().getMachineNamePrefix();
		}

		this.tenant = (String) this.cloud.getCustom().get(OPENSTACK_TENANT);
		if (tenant == null) {
			throw new IllegalArgumentException("Custom field '" + OPENSTACK_TENANT + "' must be set");
		}

		this.username = (String)this.cloud.getCustom().get(OPENSTACK_USERNAME);
		
		
		if (username == null) {
			throw new IllegalArgumentException("Custom field '" + OPENSTACK_USERNAME + "' must be set");
		}
		
		this.password = (String)this.cloud.getCustom().get(OPENSTACK_PASSWORD);
		
		if (password == null) {
			throw new IllegalArgumentException("Custom field '" + OPENSTACK_PASSWORD + "' must be set");
		}

		this.endpoint = (String) this.cloud.getCustom().get(OPENSTACK_OPENSTACK_ENDPOINT);
		if (this.endpoint == null) {
			throw new IllegalArgumentException("Custom field '" + OPENSTACK_OPENSTACK_ENDPOINT + "' must be set");
		}
		this.service = client.resource(this.endpoint);

		this.identityEndpoint = (String) this.cloud.getCustom().get(OPENSTACK_OPENSTACK_IDENTITY_ENDPOINT);
		if (this.identityEndpoint == null) {
			throw new IllegalArgumentException("Custom field '" + OPENSTACK_OPENSTACK_IDENTITY_ENDPOINT
					+ "' must be set");
		}

		final String wireLog = (String) this.cloud.getCustom().get(OPENSTACK_WIRE_LOG);
		if (wireLog != null) {
			if (Boolean.parseBoolean(wireLog)) {
				this.client.addFilter(new LoggingFilter(logger));
			}
		}

	}


	public MachineDetails startMachine(final long duration, final TimeUnit unit)
			throws TimeoutException, CloudProvisioningException {
		
		if (isThrottling()) {
			throw new CloudProvisioningException(RUNNING_THROTTLING);
		}

		final long endTime = System.currentTimeMillis() + unit.toMillis(duration);

		AuthInfo authInfo;
		try {
			authInfo = createAuthenticationToken();
		} catch (OpenstackException ose) {
			throw new CloudProvisioningException(ose);	
		}
		MachineDetails md;
		try {
			md = newServer(authInfo, endTime, this.template);
		} catch (final Exception e) {
			if (e instanceof UniformInterfaceException 
					&& ((UniformInterfaceException) e).getResponse().getStatus() == INTERNAL_SERVER_ERROR) {
				throttlingTimeout = calcEndTimeInMillis(DEFAULT_TIMEOUT_AFTER_CLOUD_INTERNAL_ERROR, 
						TimeUnit.MILLISECONDS);
				throw new CloudProvisioningException(STARTING_THROTTLING, e);
			} else {
				throw new CloudProvisioningException(e);	
			}
		}
		return md;
	}

	private long calcEndTimeInMillis(final long duration, final TimeUnit unit) {
		return System.currentTimeMillis() + unit.toMillis(duration);
	}


	public MachineDetails[] startManagementMachines(final long duration, final TimeUnit unit)
			throws TimeoutException, CloudProvisioningException {
		AuthInfo authInfo;
		try {
			authInfo = createAuthenticationToken();
		} catch (OpenstackException e) {
			throw new CloudProvisioningException(e);	
		}
		final long endTime = calcEndTimeInMillis(duration, unit);

		final int numOfManagementMachines = cloud.getProvider().getNumberOfManagementMachines();

		// thread pool - one per machine
		final ExecutorService executor =
				Executors.newFixedThreadPool(cloud.getProvider().getNumberOfManagementMachines());

		try {
			return doStartManagement(endTime, authInfo, numOfManagementMachines, executor);
		} finally {
			executor.shutdown();
		}
	}

	private MachineDetails[] doStartManagement(final long endTime, final AuthInfo authInfo,
			final int numOfManagementMachines, final ExecutorService executor)
			throws CloudProvisioningException {

		// launch machine on a thread
		final List<Future<MachineDetails>> list = new ArrayList<Future<MachineDetails>>(numOfManagementMachines);
		for (int i = 0; i < numOfManagementMachines; ++i) {
			final Future<MachineDetails> task = executor.submit(new Callable<MachineDetails>() {

			
				public MachineDetails call()
						throws Exception {

					final MachineDetails md = newServer(authInfo, endTime, template);
					return md;

				}

			});
			list.add(task);

		}

		// get the machines
		Exception firstException = null;
		final List<MachineDetails> machines = new ArrayList<MachineDetails>(numOfManagementMachines);
		for (final Future<MachineDetails> future : list) {
			try {
				machines.add(future.get());
			} catch (final Exception e) {
				if (firstException == null) {
					firstException = e;
				}
			}
		}

		if (firstException == null) {
			return machines.toArray(new MachineDetails[machines.size()]);
		} else {
			// in case of an exception, clear the machines
			logger.warning("Provisioning of management machines failed, the following node will be shut down: "
					+ machines);
			for (final MachineDetails machineDetails : machines) {
				try {
					this.terminateServer(new ServerInfo(machineDetails.getMachineId(),authInfo.getComputeServiceEndpointURL()+"/servers/"+machineDetails.getMachineId(),machineDetails.getPublicAddress()), authInfo, endTime);
				} catch (final Exception e) {
					logger.log(Level.SEVERE,
							"While shutting down machine after provisioning of management machines failed, "
									+ "shutdown of node: " + machineDetails.getMachineId()
									+ " failed. This machine may be leaking. Error was: " + e.getMessage(), e);
				}
			}

			throw new CloudProvisioningException(
					"Failed to launch management machines: " + firstException.getMessage(), firstException);
		}
	}


	public boolean stopMachine(final String ip, final long duration, final TimeUnit unit)
			throws InterruptedException, TimeoutException, CloudProvisioningException {
		final long endTime = calcEndTimeInMillis(duration, unit);

		if (isStopRequestRecent(ip)) {
			return false;
		}

		
		try {
			final AuthInfo authInfo = createAuthenticationToken();
			terminateServerByIp(ip, authInfo, endTime);
			return true;
		} catch (final Exception e) {
			throw new CloudProvisioningException(e);
		}
	}

	
	public void stopManagementMachines()
			throws TimeoutException, CloudProvisioningException {
		 AuthInfo authInfo =null;
		final long endTime = calcEndTimeInMillis(DEFAULT_SHUTDOWN_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
		List<Node> nodes;
		try {
			authInfo = createAuthenticationToken();
			nodes = listServers(authInfo);
		} catch (final OpenstackException e) {
			throw new CloudProvisioningException(e);
		}

		final List<ServerInfo> serverInfoList = new LinkedList<ServerInfo>();
		for (final Node node : nodes) {
			logger.info("Machine name "+node.getName());
			logger.info("Machine id "+node.getId());
			if (node.getName().startsWith(this.serverNamePrefix)) {
				try {
					serverInfoList.add(new ServerInfo(node.getId(),((EssexNode)node).getServerURL(),node.getPublicIp()));

				} catch (final Exception e) {
					throw new CloudProvisioningException(e);
				}
			}
		}

		try {
			terminateServers(serverInfoList, authInfo, endTime);
		} catch (final TimeoutException e) {
			throw e;
		} catch (final Exception e) {
			throw new CloudProvisioningException("Failed to shut down managememnt machines", e);
		}
	}

	private Node getNode(final ServerInfo serverInfo, final AuthInfo authInfo)
			throws OpenstackException {
		logger.info("Server URL" +serverInfo.getUrl());
		service = client.resource(serverInfo.getUrl());
		final String response =
				service.header("X-Auth-Token", authInfo.getToken())
						.accept(MediaType.APPLICATION_XML).get(String.class);
		final EssexNode node = new EssexNode();
		try {
			final DocumentBuilder documentBuilder = createDocumentBuilder();
			final Document xmlDoc = documentBuilder.parse(new InputSource(new StringReader(response)));

			node.setId(xpath.evaluate("/server/@id", xmlDoc));
			node.setStatus(xpath.evaluate("/server/@status", xmlDoc));
			node.setName(xpath.evaluate("/server/@name", xmlDoc));

			// We expect to get 2 IP addresses, public and private. Currently we get them both in an xml
			// under a private node attribute. this is expected to change.
			final NodeList addresses =
					(NodeList) xpath.evaluate("/server/addresses/network/ip/@addr", xmlDoc, XPathConstants.NODESET);
			if (node.getStatus().equalsIgnoreCase(MACHINE_STATUS_ACTIVE)) {
				node.setPrivateIp(addresses.item(0).getTextContent());
				if (addresses.getLength() != 2) {
					logger.warning("Public IP not yet set");
					
				}else{
					node.setPublicIp(addresses.item(1).getTextContent());
				}

				
				
			}
			node.setServerURL(serverInfo.getUrl());
			logger.info("Node Info "+node);
		} catch (final XPathExpressionException e) {
			throw new OpenstackException("Failed to parse XML Response from server. Response was: " + response
					+ ", Error was: " + e.getMessage(), e);
		} catch (final SAXException e) {
			throw new OpenstackException("Failed to parse XML Response from server. Response was: " + response
					+ ", Error was: " + e.getMessage(), e);
		} catch (final IOException e) {
			throw new OpenstackException("Failed to send request to server. Response was: " + response
					+ ", Error was: " + e.getMessage(), e);
		}

		return node;

	}

	List<Node> listServers(final AuthInfo authInfo)
			throws OpenstackException {
		final List<ServerInfo> serverInfoList = listServerIds( authInfo);
		final List<Node> nodes = new ArrayList<Node>(serverInfoList.size());

		for (final ServerInfo serverInfo: serverInfoList) {
			nodes.add(getNode(serverInfo,  authInfo));
		}
		
		return nodes;
	}

	// public void listFlavors(final String token) throws Exception {
	// final WebResource service = client.resource(this.endpoint);
	//
	// String response = null;
	//
	// response = service.path(this.pathPrefix + "flavors").header("X-Auth-Token", token)
	// .accept(MediaType.APPLICATION_XML).get(String.class);
	//
	// System.out.println(response);
	//
	// }

	private List<ServerInfo> listServerIds(final AuthInfo authInfo)
			throws OpenstackException {

		String response = null;
		try {
			service = client.resource(authInfo.getComputeServiceEndpointURL());
			response =
					service.path("/servers").header("X-Auth-Token", authInfo.getToken())
							.accept(MediaType.APPLICATION_XML).get(String.class);
			final DocumentBuilder documentBuilder = createDocumentBuilder();
			final Document xmlDoc = documentBuilder.parse(new InputSource(new StringReader(response)));

			
		   
		
			
			final NodeList idNodes = (NodeList) xpath.evaluate("/servers/server/@id", xmlDoc, XPathConstants.NODESET);
			final NodeList urlNodes = (NodeList)xpath.evaluate("/servers/server/link[@rel='self']/@href",xmlDoc,XPathConstants.NODESET);
			final int howmany = idNodes.getLength();
			final List<ServerInfo> serverInfoList = new ArrayList<ServerInfo>(howmany);
			for (int i = 0; i < howmany; i++) {
				serverInfoList.add(new ServerInfo(idNodes.item(i).getTextContent(),urlNodes.item(i).getTextContent(),null));

			}
			return serverInfoList;

		} catch (final UniformInterfaceException e) {
			final String responseEntity = e.getResponse().getEntity(String.class).toString();
			throw new OpenstackException(e + " Response entity: " + responseEntity, e);

		} catch (final SAXException e) {
			throw new OpenstackException("Failed to parse XML Response from server. Response was: " + response
					+ ", Error was: " + e.getMessage(), e);
		} catch (final XPathException e) {
			throw new OpenstackException("Failed to parse XML Response from server. Response was: " + response
					+ ", Error was: " + e.getMessage(), e);
		} catch (final IOException e) {
			throw new OpenstackException("Failed to send request to server. Response was: " + response
					+ ", Error was: " + e.getMessage(), e);

		}
	}

	private void terminateServerByIp(final String serverIp, final AuthInfo authInfo, final long endTime)
			throws Exception {
		final Node node = getNodeByIp(serverIp, authInfo);
		if (node == null) {
			throw new IllegalArgumentException("Could not find a server with IP: " + serverIp);
		}
		terminateServer(new ServerInfo(node.getId(),((EssexNode)node).getServerURL(),node.getPublicIp()), authInfo, endTime);
	}

	private Node getNodeByIp(final String serverIp,  final AuthInfo authInfo)
			throws OpenstackException {
		final List<Node> nodes = listServers(authInfo);
		for (final Node node : nodes) {
			if (node.getPrivateIp() != null && node.getPrivateIp().equalsIgnoreCase(serverIp)
					|| node.getPublicIp() != null && node.getPublicIp().equalsIgnoreCase(serverIp)) {
				return node;
			}
		}

		return null;
	}

	private void terminateServer(final ServerInfo serverInfo, final AuthInfo authInfo, final long endTime)
			throws Exception {
		terminateServers(Arrays.asList(serverInfo), authInfo, endTime);
	}

	private void terminateServers(final List<ServerInfo> serverInfoList, final AuthInfo authInfo, final long endTime)
			throws Exception {

		// detach public ip and delete the servers
		for (final ServerInfo serverInfo : serverInfoList) {
			try {
				if(!StringUtils.isEmpty(serverInfo.getIp())){
					detachFloatingIP(serverInfo, serverInfo.getIp(), authInfo);
					deleteFloatingIP(serverInfo.getIp(), authInfo);
				}
				service = client.resource(serverInfo.getUrl());
				service.header("X-Auth-Token", authInfo.getToken())
						.accept(MediaType.APPLICATION_XML).delete();
			} catch (final UniformInterfaceException e) {
				final String responseEntity = e.getResponse().getEntity(String.class).toString();
				throw new IllegalArgumentException(e + " Response entity: " + responseEntity);
			}

		}

		int successCounter = 0;

		// wait for all servers to die
		for (final ServerInfo serverInfo : serverInfoList) {
			while (System.currentTimeMillis() < endTime) {
				try {
					this.getNode(serverInfo, authInfo);

				} catch (final UniformInterfaceException e) {
					if (e.getResponse().getStatus() == HTTP_NOT_FOUND) {
						++successCounter;
						break;
					}
					throw e;
				}
				Thread.sleep(SERVER_POLLING_INTERVAL_MILLIS);
			}

		}

		if (successCounter == serverInfoList.size()) {
			return;
		}

		throw new TimeoutException("Nodes " + serverInfoList + " did not shut down in the required time");

	}

	/**
	 * Creates server. Block until complete. Returns id
	 * 
	 * @param name the server name
	 * @param timeout the timeout in seconds
	 * @param serverTemplate the cloud template to use for this server
	 * @return the server id
	 */
	private MachineDetails newServer(final AuthInfo authInfo, final long endTime, final CloudTemplate serverTemplate)
			throws Exception {
		
		final ServerInfo serverInfo = createServer(authInfo, serverTemplate);

		try {
			final MachineDetails md = new MachineDetails();
			// wait until complete
			waitForServerToReachStatus(md, endTime, serverInfo, authInfo, MACHINE_STATUS_ACTIVE);

			// if here, we have a node with a private and public ip.
			

			
			md.setMachineId(serverInfo.getId());
			md.setAgentRunning(false);
			md.setCloudifyInstalled(false);
			md.setInstallationDirectory(serverTemplate.getRemoteDirectory());
			md.setRemoteUsername(serverTemplate.getUsername());
			md.setRemotePassword(serverTemplate.getPassword());

			return md;
		} catch (final Exception e) {
			logger.log(Level.WARNING, "server: " + serverInfo.getId() + " failed to start up correctly. "
					+ "Shutting it down. Error was: " + e.getMessage(), e);
			try {
				terminateServer(serverInfo, authInfo, endTime);
			} catch (final Exception e2) {
				logger.log(Level.WARNING,
						"Error while shutting down failed machine: " + serverInfo.getId() + ". Error was: " + e.getMessage()
								+ ".It may be leaking.", e);
			}
			throw e;
		}

	}

	private ServerInfo createServer(final AuthInfo authInfo, final CloudTemplate serverTemplate)
			throws OpenstackException {
		String serverName = null;
		String serviceNamePrefix =(String) serverTemplate.getCustom().get("machineNamePrefix");
		 if(!StringUtils.isEmpty(serviceNamePrefix)){
			 serverName = serviceNamePrefix + System.currentTimeMillis();
		 }else{
			 serverName = this.serverNamePrefix + System.currentTimeMillis();
		 }
		
		
		final String securityGroup = getCustomTemplateValue(serverTemplate, OPENSTACK_SECURITYGROUP, null, false);
		final String keyPairName = getCustomTemplateValue(serverTemplate, OPENSTACK_KEY_PAIR, null, false);
		String userdata = new String(Base64.encode(serverName.getBytes()));
		// Start the machine!
		final String json =
				"{\"server\":{ \"name\":\"" + serverName + "\",\"imageRef\":\"" + serverTemplate.getImageId() +"\",\"user_data\":\"" + userdata
						+ "\",\"flavorRef\":\"" + serverTemplate.getHardwareId() + "\",\"key_name\":\"" + keyPairName
						+ "\",\"security_groups\":[{\"name\":\"" + securityGroup + "\"}]}}";

		String serverBootResponse = null;
		try {
			service = client.resource(authInfo.getComputeServiceEndpointURL());
			serverBootResponse =
					service.path("/servers").header("Content-Type", "application/json")
							.header("X-Auth-Token", authInfo.getToken()).accept(MediaType.APPLICATION_XML).post(String.class, json);
		} catch (final UniformInterfaceException e) {
			final String responseEntity = e.getResponse().getEntity(String.class).toString();
			throw new OpenstackException(e + " Response entity: " + responseEntity, e);
		}

		try {
			// if we are here, the machine started!
			final DocumentBuilder documentBuilder = createDocumentBuilder();
			Document doc = documentBuilder.parse(new InputSource(new StringReader(serverBootResponse)));
			
			String serverDetailURL =xpath.evaluate("//server/link[@rel='self']/@href", doc);
			service = client.resource(serverDetailURL);
			
			String serverDetail = service.header("X-Auth-Token", authInfo.getToken())
					.accept(MediaType.APPLICATION_XML).get(String.class);
			
			doc = documentBuilder.parse(new InputSource(new StringReader(serverDetail)));
			final String status = xpath.evaluate("/server/@status", doc);
			if (!status.startsWith("BUILD")) {
				throw new IllegalStateException("Expected server status of BUILD(*), got: " + status);
			}

			final String serverId = xpath.evaluate("/server/@id", doc);
		    final String serverInfoURL = xpath.evaluate("/server/link[@rel='self']/@href", doc);
			
			
			return new ServerInfo(serverId,serverInfoURL,null);
		} catch (final XPathExpressionException e) {
			throw new OpenstackException("Failed to parse XML Response from server. Response was: "
					+ serverBootResponse + ", Error was: " + e.getMessage(), e);
		} catch (final SAXException e) {
			throw new OpenstackException("Failed to parse XML Response from server. Response was: "
					+ serverBootResponse + ", Error was: " + e.getMessage(), e);
		} catch (final IOException e) {
			throw new OpenstackException("Failed to send request to server. Response was: " + serverBootResponse
					+ ", Error was: " + e.getMessage(), e);
		}
	}

	private String getCustomTemplateValue(final CloudTemplate serverTemplate, final String key,
			final String defaultValue, final boolean allowNull) {
		final String value = (String) serverTemplate.getOptions().get(key);
		if (value == null) {
			if (allowNull) {
				return defaultValue;
			} else {
				throw new IllegalArgumentException("Template option '" + key + "' must be set");
			}
		} else {
			return value;
		}

	}

	private void waitForServerToReachStatus(final MachineDetails md, final long endTime, final ServerInfo serverInfo,
			final AuthInfo authInfo, final String status)
			throws OpenstackException, TimeoutException, InterruptedException {

		final String respone = null;
		while (true) {

			final Node node = this.getNode(serverInfo, authInfo);

			final String currentStatus = node.getStatus().toLowerCase();

			if (currentStatus.equalsIgnoreCase(status)) {

				md.setPrivateAddress(node.getPrivateIp());
				String floatingIp = allocateFloatingIP(authInfo);
				addFloatingIP(serverInfo, floatingIp, authInfo);			
				waitForServerToRespond(floatingIp);
				md.setPublicAddress(floatingIp);
				break;
			} else {
				if (currentStatus.contains("error")) {
					throw new OpenstackException("Server provisioning failed. Node ID: " + node.getId() + ", status: "
							+ node.getStatus());
				}

			}

			if (System.currentTimeMillis() > endTime) {
				throw new TimeoutException("timeout creating server. last status:" + respone);
			}

			Thread.sleep(SERVER_POLLING_INTERVAL_MILLIS);

		}

	}

	/**
	 * The server takes a while to come in which case control
	 * must return back to cloudify apis only when its ready to accept
	 * requests
	 * @param floatingIp
	 * @throws InterruptedException
	 */
	private void waitForServerToRespond(String floatingIp) throws InterruptedException {
		boolean isActive=false;
		while(!isActive){
			try {
				service = client.resource("http://"+floatingIp+":7777/");
				ClientResponse response = service.get(ClientResponse.class);
				 
				 if(response.getClientResponseStatus().equals(Status.OK)){
					 logger.info(floatingIp + " returned " + Status.OK + " so its ready.");
					 isActive =true;
					 break;
				 }
				}
				catch(ClientHandlerException ce) {
					if(ce.getCause() instanceof ConnectException){
						 logger.info("Unable to connect to "+floatingIp+"  ...waiting");
					}
				}
				Thread.sleep(SERVER_POLLING_INTERVAL_MILLIS);
			}
		
	}

	@SuppressWarnings("rawtypes")
	List<FloatingIP> listFloatingIPs(final AuthInfo authInfo)
			throws SAXException, IOException {
		
		service = client.resource(authInfo.getComputeServiceEndpointURL());
		final String response =
				service.path("/os-floating-ips").header("X-Auth-Token", authInfo.getToken())
						.accept(MediaType.APPLICATION_JSON).get(String.class);

		final ObjectMapper mapper = new ObjectMapper();
		final Map map = mapper.readValue(new StringReader(response), Map.class);
		@SuppressWarnings("unchecked")
		final List<Map> list = (List<Map>) map.get("floating_ips");
		final List<FloatingIP> floatingIps = new ArrayList<FloatingIP>(map.size());

		for (final Map floatingIpMap : list) {
			final FloatingIP ip = new FloatingIP();

			final Object instanceId = floatingIpMap.get("instance_id");

			ip.setInstanceId(instanceId == null ? null : instanceId.toString());
			ip.setIp((String) floatingIpMap.get("ip"));
			ip.setFixedIp((String) floatingIpMap.get("fixed_ip"));
			ip.setId(floatingIpMap.get("id").toString());
			floatingIps.add(ip);
		}
		return floatingIps;

	}

	private FloatingIP getFloatingIpByIp(final String ip, final AuthInfo authInfo)
			throws SAXException, IOException {
		final List<FloatingIP> allips = listFloatingIPs(authInfo);
		for (final FloatingIP floatingIP : allips) {
			if (ip.equals(floatingIP.getIp())) {
				return floatingIP;
			}
		}

		return null;
	}

	/*********************
	 * Deletes a floating IP.
	 * 
	 * @param ip .
	 * @param token .
	 * @throws SAXException .
	 * @throws IOException .
	 */
	public void deleteFloatingIP(final String ip, final AuthInfo authInfo)
			throws SAXException, IOException {

		service = client.resource(authInfo.getComputeServiceEndpointURL());
		final FloatingIP floatingIp = getFloatingIpByIp(ip, authInfo);
		if (floatingIp == null) {
			logger.warning("Could not find floating IP " + ip + " in list. IP was not deleted.");
		} else {
			service.path("/os-floating-ips/" + floatingIp.getId()).header("X-Auth-Token", authInfo.getToken())
					.accept(MediaType.APPLICATION_JSON).delete();

		}

	}

	/**************
	 * Allocates a floating IP.
	 * 
	 * @param token .
	 * @return .
	 */
	
	public String allocateFloatingIP(final AuthInfo authInfo) {

		try {
			service = client.resource(authInfo.getComputeServiceEndpointURL());
			final String resp =
					service.path("/os-floating-ips").header("Content-type", "application/json")
							.header("X-Auth-Token", authInfo.getToken()).accept(MediaType.APPLICATION_JSON).post(String.class, "");

			final Matcher m = Pattern.compile("\"ip\": \"([^\"]*)\"").matcher(resp);
			if (m.find()) {
				return m.group(1);
			} else {
				throw new IllegalStateException("Failed to allocate floating IP - IP not found in response");
			}
		} catch (final UniformInterfaceException e) {
			logRestError(e);
			throw new IllegalStateException("Failed to allocate floating IP", e);
		}

	}

	private void logRestError(final UniformInterfaceException e) {
		logger.severe("REST Error: " + e.getMessage());
		logger.severe("REST Status: " + e.getResponse().getStatus());
		logger.severe("REST Message: " + e.getResponse().getEntity(String.class));
	}

	/**
	 * Attaches a previously allocated floating ip to a server.
	 * 
	 * @param serverInfo .
	 * @param ip public ip to be assigned .
	 * @param authInfo .
	 * @throws Exception .
	 */
	
	public void addFloatingIP(final ServerInfo serverInfo, final String ip, final AuthInfo authInfo)
			 {

		service = client.resource(serverInfo.getUrl());
		service.path("/action")
				.header("Content-type", "application/json")
				.header("X-Auth-Token", authInfo.getToken())
				.accept(MediaType.APPLICATION_JSON)
				.post(String.class,
						String.format("{\"addFloatingIp\":{\"server\":\"%s\",\"address\":\"%s\"}}", serverInfo.getId(), ip));

	}

	/**********
	 * Detaches a floating IP from a server.
	 * 
	 * @param serverId .
	 * @param ip .
	 * @param token .
	 */
	
	public void detachFloatingIP(final ServerInfo serverInfo, final String ip, final AuthInfo authInfo) {

		service = client.resource(serverInfo.getUrl());
		service.path("/action")
				.header("Content-type", "application/json")
				.header("X-Auth-Token", authInfo.getToken())
				.accept(MediaType.APPLICATION_JSON)
				.post(String.class,
						String.format("{\"removeFloatingIp\":{\"server\": \"%s\", \"address\": \"%s\"}}",
								serverInfo.getId(), ip));

	}

	/**********
	 * Creates an openstack keystone authentication token.
	 * 
	 * @return the authentication token.
	 */

	public AuthInfo createAuthenticationToken() throws OpenstackException{

		final String json =
				"{\"auth\":{\"passwordCredentials\":{\"username\":\"" + this.username
				+ "\",\"password\":\"" + this.password + "\"},\"tenantName\":\""
				+ this.tenant + "\"}}";

		final WebResource service = client.resource(this.identityEndpoint);

		final String response =
				service.path("/v2.0/tokens").header("Content-Type", "application/json")
						.accept(MediaType.APPLICATION_XML).post(String.class, json);
		String authToken =null;
		String computeServiceURL =null;
		try{
		DocumentBuilder db = createDocumentBuilder();
		final Document xmlDoc = db.parse(new InputSource(new StringReader(response)));
		 authToken = xpath.evaluate("//access/token/@id", xmlDoc);
		 computeServiceURL = xpath.evaluate("//access/serviceCatalog/service[@name='Compute Service']/endpoint/@publicURL",xmlDoc);

		} catch (final SAXException e) {
			throw new OpenstackException("Failed to parse XML Response from server. Response was: " + response
					+ ", Error was: " + e.getMessage(), e);
		} catch (final XPathException e) {
			throw new OpenstackException("Failed to parse XML Response from server. Response was: " + response
					+ ", Error was: " + e.getMessage(), e);
		} catch (final IOException e) {
			throw new OpenstackException("Failed to send request to server. Response was: " + response
					+ ", Error was: " + e.getMessage(), e);
		}
		return new AuthInfo(authToken, computeServiceURL);
		
	}
	
	/**
	 * Checks if throttling is now activated, to avoid overloading the cloud.
	 * @return True if throttling is activate, false otherwise
	 */
	public boolean isThrottling() {
		boolean throttling = false;
		if (throttlingTimeout > 0 && throttlingTimeout - System.currentTimeMillis() > 0) {
			throttling = true;
		}
		
		return throttling;
	}
}
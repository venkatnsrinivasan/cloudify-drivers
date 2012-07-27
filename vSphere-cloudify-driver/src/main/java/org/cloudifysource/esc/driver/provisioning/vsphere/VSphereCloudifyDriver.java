package org.cloudifysource.esc.driver.provisioning.vsphere;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;

import org.apache.commons.lang.StringUtils;
import org.cloudifysource.dsl.cloud.Cloud;
import org.cloudifysource.esc.driver.provisioning.CloudDriverSupport;
import org.cloudifysource.esc.driver.provisioning.CloudProvisioningException;
import org.cloudifysource.esc.driver.provisioning.MachineDetails;
import org.cloudifysource.esc.driver.provisioning.ProvisioningDriver;
import org.cloudifysource.esc.driver.provisioning.context.ProvisioningDriverClassContext;
import org.cloudifysource.esc.driver.provisioning.vsphere.api.VSphereCommunicatorService;
import org.cloudifysource.esc.driver.provisioning.vsphere.api.impl.VSphereCommunicatorServiceImpl;

/**
 * Vsphere Driver using the vijava driver http://vijava.sourceforge.net/
 * Driver makes a some assumptions like - a)Both private and public addresses
 * are same for now.b)All Ips are assigned by DHCP.
 * @author Venkat
 *
 */
public class VSphereCloudifyDriver extends CloudDriverSupport implements ProvisioningDriver
		 {

	private static final String VSPHERE_URL = "vsphere.URL";
	private static final String VSPHERE_USER_NAME = "vsphere.username";
	private static final String VSPHERE_PASSWORD = "vsphere.password";

	private String serverNamePrefix;
	private String vsphereURL;
	private String vsphereUserName;
	private String vspherePassword;
	private VSphereCommunicatorService vsphereCommunicatorService ;

	public void setProvisioningDriverClassContext(
			ProvisioningDriverClassContext provisioningDriverClassContext) {
	
		
	}

	public void close() {
		vsphereCommunicatorService.close();		
	}

	
	@Override
	public void setConfig(Cloud cloud, String templateName, boolean management) {
		super.setConfig(cloud, templateName, management);
		
		if (this.management) {
			this.serverNamePrefix = this.cloud.getProvider().getManagementGroup();
		} else {
			this.serverNamePrefix = this.cloud.getProvider().getMachineNamePrefix();
		}
		
		this.vsphereURL = (String)this.cloud.getCustom().get(VSPHERE_URL);
		this.vsphereUserName = (String)this.cloud.getCustom().get(VSPHERE_USER_NAME);
		this.vspherePassword = (String)this.cloud.getCustom().get(VSPHERE_PASSWORD);
		if (vsphereURL == null) {
			throw new IllegalArgumentException("Custom field '" + VSPHERE_URL + "' must be set");
		}
		
		if (vsphereUserName == null) {
			throw new IllegalArgumentException("Custom field '" + VSPHERE_USER_NAME + "' must be set");
		}
		
		if (vspherePassword == null) {
			throw new IllegalArgumentException("Custom field '" + VSPHERE_PASSWORD + "' must be set");
		}
		
		try {
			vsphereCommunicatorService = new VSphereCommunicatorServiceImpl(vsphereURL, vsphereUserName, vspherePassword);
		} catch (CloudProvisioningException e) {
			throw new RuntimeException(e);
		}		
	}

	
	public MachineDetails startMachine(long duration, TimeUnit timeout)
			throws TimeoutException, CloudProvisioningException {
		MachineDetails md;
		String machineName = serverNamePrefix;
		 String serviceNamePrefix =(String) this.template.getCustom().get("machineNamePrefix");
		 if(!StringUtils.isEmpty(serviceNamePrefix)){
			 machineName=serviceNamePrefix ;
		 }
		try {
			md = vsphereCommunicatorService.createServer(duration,timeout,this.serverNamePrefix,machineName+System.currentTimeMillis(),this.cloud,this.template);
		} catch (final Exception e) {
			throw new CloudProvisioningException(e);
		}
		return md;
	}

	public MachineDetails[] startManagementMachines(long duration, TimeUnit timeout)
			throws TimeoutException, CloudProvisioningException {
		final int numOfManagementMachines = cloud.getProvider().getNumberOfManagementMachines();

	
		final ExecutorService executor =
				Executors.newFixedThreadPool(cloud.getProvider().getNumberOfManagementMachines());

		try {
			return doStartManagement(duration,timeout,numOfManagementMachines, executor);
		} finally {
			executor.shutdown();
		}
	}

	private MachineDetails[] doStartManagement(final long duration, final TimeUnit timeout,int numOfManagementMachines,
			ExecutorService executor) throws CloudProvisioningException {
		// launch machine on a thread
		final List<Future<MachineDetails>> list = new ArrayList<Future<MachineDetails>>(numOfManagementMachines);
		for (int i = 0; i < numOfManagementMachines; i++) {
			final int index = i+1;
			final Future<MachineDetails> task = executor.submit(new Callable<MachineDetails>() {

				//@Override
				public MachineDetails call()
						throws Exception {

					final MachineDetails md = vsphereCommunicatorService.createServer(duration,timeout,serverNamePrefix,serverNamePrefix+index,cloud,template);
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
					vsphereCommunicatorService.terminateServer(duration, timeout,machineDetails);
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

	public boolean stopMachine(String machineIp, long duration, TimeUnit timeout)
			throws InterruptedException, TimeoutException,
			CloudProvisioningException {
		return vsphereCommunicatorService.terminateServerByIp(duration,timeout,machineIp);
	}

	public void stopManagementMachines() throws TimeoutException,
			CloudProvisioningException {
		



		try {
			vsphereCommunicatorService.terminateServers(serverNamePrefix);
		
		} catch (final Exception e) {
			throw new CloudProvisioningException("Failed to shut down managememnt machines", e);
		}		
	}

	public String getCloudName() {
		return this.cloud.getName();
	}
	
	
	

}

	
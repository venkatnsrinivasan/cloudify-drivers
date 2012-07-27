package org.cloudifysource.esc.driver.provisioning.vsphere.api;

import java.util.concurrent.TimeUnit;

import org.cloudifysource.dsl.cloud.Cloud;
import org.cloudifysource.dsl.cloud.CloudTemplate;
import org.cloudifysource.esc.driver.provisioning.CloudProvisioningException;
import org.cloudifysource.esc.driver.provisioning.MachineDetails;

public interface VSphereCommunicatorService {

	MachineDetails createServer(long duration, TimeUnit timeout, String serverNamePrefix,String vmName, Cloud cloud,CloudTemplate serverTemplate) throws CloudProvisioningException;

	void terminateServer(long duration, TimeUnit timeout, MachineDetails machineDetails) throws CloudProvisioningException;

	boolean terminateServerByIp(long duration, TimeUnit timeout,String machineIp) throws CloudProvisioningException;

	void terminateServers(String serverNamePrefix) throws CloudProvisioningException;

	void close();

}

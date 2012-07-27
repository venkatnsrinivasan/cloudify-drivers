package org.cloudifysource.esc.driver.provisioning.vsphere.api.impl;


import java.net.URL;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.cloudifysource.dsl.cloud.Cloud;
import org.cloudifysource.dsl.cloud.CloudTemplate;
import org.cloudifysource.esc.driver.provisioning.CloudProvisioningException;
import org.cloudifysource.esc.driver.provisioning.MachineDetails;
import org.cloudifysource.esc.driver.provisioning.vsphere.api.VSphereCommunicatorService;

import com.vmware.vim25.CustomizationAdapterMapping;
import com.vmware.vim25.CustomizationDhcpIpGenerator;
import com.vmware.vim25.CustomizationGlobalIPSettings;
import com.vmware.vim25.CustomizationIPSettings;
import com.vmware.vim25.CustomizationLinuxPrep;
import com.vmware.vim25.CustomizationPrefixName;
import com.vmware.vim25.CustomizationSpec;
import com.vmware.vim25.ManagedEntityStatus;
import com.vmware.vim25.ManagedObjectReference;
import com.vmware.vim25.TaskInfoState;
import com.vmware.vim25.VirtualMachineCloneSpec;
import com.vmware.vim25.VirtualMachineConfigSpec;
import com.vmware.vim25.VirtualMachineRelocateSpec;
import com.vmware.vim25.mo.ComputeResource;
import com.vmware.vim25.mo.Folder;
import com.vmware.vim25.mo.HostSystem;
import com.vmware.vim25.mo.InventoryNavigator;
import com.vmware.vim25.mo.ManagedEntity;
import com.vmware.vim25.mo.ServiceInstance;
import com.vmware.vim25.mo.Task;
import com.vmware.vim25.mo.VirtualMachine;
import com.vmware.vim25.mo.util.MorUtil;

public class VSphereCommunicatorServiceImpl implements
		VSphereCommunicatorService {

	private static final String RESOURCE_POOL = "resourcePool";
	private static final String VIRTUAL_MACHINE = "VirtualMachine";
	private static final String GUEST_DOMAIN = "vsphere.guest.domain";
	private static final String GUEST_TIME_ZONE = "vsphere.guest.time.zone";
	private static final String RUNNING = "running";
	private static final long GUEST_POLL_TIME = 3000;
	private String url;
	private String username;
	private String password;
	private ServiceInstance serviceInstance;
	private Folder rootFolder;
	private  static final Logger logger = Logger.getLogger(VSphereCommunicatorServiceImpl.class.getName());
	private static final long WAIT_UNTIL_FINISH = -1;

	public VSphereCommunicatorServiceImpl(String url,String username,String password) throws CloudProvisioningException{
		this.url=url;
		this.username=username;
		this.password=password;
		try {
			serviceInstance=new ServiceInstance(new URL(url),username,password,true);
			rootFolder =serviceInstance.getRootFolder();
		} catch (Exception e) {
			throw new CloudProvisioningException("Cannot initialize vsphere service instance", e);
		}
	}
	public MachineDetails createServer(long duration, TimeUnit timeout,String serverNamePrefix,String vmName,Cloud cloud,CloudTemplate serverTemplate) throws CloudProvisioningException {
		
		if(logger.isLoggable(Level.INFO)){
			logger.info("Creating new server from template .." +ToStringBuilder.reflectionToString(serverTemplate) +" and " +" serverNamePrefix " + serverNamePrefix + " and "+ vmName);
		}
		String vmTemplateName = serverTemplate.getImageId();
		VirtualMachine virtualMachineTemplate = findVirtualMachine(vmTemplateName);
		
		VirtualMachine newVirtualMachine = cloneVMTemplate(virtualMachineTemplate,serverNamePrefix,vmName,cloud,serverTemplate);
				
		
		return createMachineDetails( duration,  timeout,newVirtualMachine,cloud,serverTemplate);
	}
	
	private MachineDetails createMachineDetails(long duration, TimeUnit unit, VirtualMachine newVirtualMachine,Cloud cloud,CloudTemplate serverTemplate) throws CloudProvisioningException {
		final MachineDetails md = new MachineDetails();
		md.setAgentRunning(false);
		md.setCloudifyInstalled(false);
		md.setInstallationDirectory(null);
		boolean nodeUp;
		try {
			nodeUp = waitForVirtualMachineToBeUp(duration,unit,newVirtualMachine);
		} catch (InterruptedException e) {
			throw new CloudProvisioningException("Virtual Machine launch wait timedout",e);
		}
		if(!nodeUp){
			throw new CloudProvisioningException("Virtual Machine launch timedout");
 
		}
		md.setMachineId(newVirtualMachine.getConfig().name);
		md.setPublicAddress(newVirtualMachine.getGuest().getIpAddress());
		md.setPrivateAddress(newVirtualMachine.getGuest().getIpAddress());



		md.setRemoteUsername(serverTemplate.getUsername());
		md.setRemotePassword(serverTemplate.getPassword());
		if(logger.isLoggable(Level.INFO)){
			logger.info("Machine Details of new VM .." +ToStringBuilder.reflectionToString(md));
		}
		
		return md;
	}
	private boolean waitForVirtualMachineToBeUp(long duration, TimeUnit unit,VirtualMachine newVirtualMachine) throws InterruptedException {
		final long end = System.currentTimeMillis() + unit.toMillis(duration);
		while(System.currentTimeMillis() < end){
			
			String guestState = newVirtualMachine.getGuest().getGuestState();
			if(guestState.equals(RUNNING) && newVirtualMachine.getGuestHeartbeatStatus() == ManagedEntityStatus.green){
				if(logger.isLoggable(Level.INFO)){
					logger.info("GuestInfo.." +ToStringBuilder.reflectionToString(newVirtualMachine.getGuest()));
				}
				return true;
				
			}
			Thread.sleep(GUEST_POLL_TIME);
		}
		return false;
	}
	/**
     * TODO: Make method protected and move to an abstract class so this can 
	 * be extended by use case needs of how any enduser would want to customize their
	 * VM.
	 * @param virtualMachineTemplate
	 * @param serverNamePrefix
	 * @param vmName
	 * @param cloud
	 * @param serverTemplate
	 * @return
	 * @throws CloudProvisioningException
	 */
	private VirtualMachine cloneVMTemplate(
			VirtualMachine virtualMachineTemplate, String serverNamePrefix,
			String vmName,Cloud cloud,CloudTemplate serverTemplate) throws CloudProvisioningException {
		VirtualMachineConfigSpec virtualMachineConfigSpec =createVMConfigSpec(serverTemplate);
		CustomizationSpec customizationSpec = createCustomizationSpec(serverNamePrefix,vmName,cloud,serverTemplate);
		VirtualMachineRelocateSpec virtualMachineRelocateSpec = createVMRelocateSpec(virtualMachineTemplate);
		
		VirtualMachineCloneSpec virtualMachineCloneSpec = new VirtualMachineCloneSpec();
		
			virtualMachineCloneSpec.setTemplate(false);
		    virtualMachineCloneSpec.setPowerOn(true);
		    virtualMachineCloneSpec.setCustomization(customizationSpec);
		    virtualMachineCloneSpec.setConfig(virtualMachineConfigSpec);
		    virtualMachineCloneSpec.setLocation(virtualMachineRelocateSpec); 
		    try {
		    Task task =virtualMachineTemplate.cloneVM_Task((Folder)virtualMachineTemplate.getParent(),vmName,virtualMachineCloneSpec);
		    task.waitForTask();
		    if(task.getTaskInfo().state == TaskInfoState.success){
		    	ManagedObjectReference newVmMOR = (ManagedObjectReference) task.getTaskInfo().getResult();
		    	VirtualMachine newVM = (VirtualMachine) MorUtil.createExactManagedEntity(serviceInstance.getServerConnection(), newVmMOR);
			    return newVM;

		    }else{
		    	throw new CloudProvisioningException("Unable to  create VM "+ task.getTaskInfo().state);	
		    }
		    }catch(Exception e){
				throw new CloudProvisioningException("Cannot create new VM", e);	
		    }
	}
	
	
	private VirtualMachineConfigSpec createVMConfigSpec(CloudTemplate serverTemplate){
		VirtualMachineConfigSpec virtualMachineConfigSpec = new VirtualMachineConfigSpec();
		virtualMachineConfigSpec.setMemoryMB((long) serverTemplate.getMachineMemoryMB());
		return virtualMachineConfigSpec;
	}

	/**
	 * TODO: Make method protected and move to an abstract class so this can 
	 * be extended by use case needs of how any enduser would want to customize their
	 * VM.
	 * @param serverNamePrefix
	 * @param vmName
	 * @param cloud
	 * @param serverTemplate
	 * @return
	 */
	private CustomizationSpec createCustomizationSpec(
			String serverNamePrefix, String vmName, Cloud cloud,CloudTemplate serverTemplate) {
		 CustomizationLinuxPrep customizationLinuxPrep = new CustomizationLinuxPrep();
		 CustomizationPrefixName hostnamePrefix = new CustomizationPrefixName();
		 String serviceNamePrefix =(String) serverTemplate.getCustom().get("machineNamePrefix");
		 if(!StringUtils.isEmpty(serviceNamePrefix)){
			 hostnamePrefix.setBase(serviceNamePrefix);
		 }else{
		     hostnamePrefix.setBase(serverNamePrefix);
		 }
	     customizationLinuxPrep.setHostName(hostnamePrefix);
	     customizationLinuxPrep.setDomain((String) cloud.getCustom().get(GUEST_DOMAIN));

			  	CustomizationAdapterMapping[] customizationAdapterMapping = new CustomizationAdapterMapping[1];
			  	customizationLinuxPrep.setHwClockUTC(true);
			  	customizationLinuxPrep.setTimeZone((String) cloud.getCustom().get(GUEST_TIME_ZONE)) ;
			  	CustomizationIPSettings customizationIPSettings = new CustomizationIPSettings();
			  	customizationIPSettings.setIp(new CustomizationDhcpIpGenerator());
			  	
			  	for(int i=0;i < customizationAdapterMapping.length;i++){
			  	  customizationAdapterMapping[i] = new CustomizationAdapterMapping();
			  	  customizationAdapterMapping[i].setAdapter(customizationIPSettings);
			  	}
			  	
			  	 CustomizationSpec customizationSpec = new CustomizationSpec();
			  	customizationSpec.setIdentity(customizationLinuxPrep);
			  	customizationSpec.setGlobalIPSettings(new CustomizationGlobalIPSettings());
			  	customizationSpec.setNicSettingMap(customizationAdapterMapping);
			  	
			  	return customizationSpec;
			  	
	}
	private VirtualMachineRelocateSpec createVMRelocateSpec(
			VirtualMachine virtualMachineTemplate) {
		 ManagedObjectReference hostMOR = virtualMachineTemplate.getRuntime().getHost();
         HostSystem host = (HostSystem) MorUtil.createExactManagedEntity(virtualMachineTemplate.getServerConnection(), hostMOR);
         ManagedObjectReference rourcePoolMOR = (ManagedObjectReference)((ComputeResource)host.getParent()).getPropertyByPath(RESOURCE_POOL);
         VirtualMachineRelocateSpec vmrs = new VirtualMachineRelocateSpec();
         vmrs.setPool(rourcePoolMOR);
         return vmrs;
	}
	public void terminateServer(long duration, TimeUnit unit,MachineDetails machineDetails) throws CloudProvisioningException {

		if(logger.isLoggable(Level.INFO)){
			logger.info("Destroying VM .." +ToStringBuilder.reflectionToString(machineDetails) );
		}
		VirtualMachine virtualMachine = findVirtualMachine(machineDetails.getMachineId());
		
		destroyVM(WAIT_UNTIL_FINISH, unit, virtualMachine);
	}
	

	public boolean terminateServerByIp(long duration,TimeUnit unit ,String machineIp) throws CloudProvisioningException {
		try{
			logger.log(Level.INFO,"Looking to terminate server "+machineIp);
		ManagedEntity[] mentities =  new InventoryNavigator(rootFolder).searchManagedEntities(VIRTUAL_MACHINE);
		if(mentities !=null){
			VirtualMachine[] vms = Arrays.asList(mentities).toArray(new VirtualMachine[mentities.length]);
			logger.log(Level.FINE,"vms length "+vms.length);
			for(VirtualMachine eachVM:vms){
				
				logger.log(Level.FINE,"Guest Ip "+eachVM.getGuest().getIpAddress());
				if(StringUtils.equals(eachVM.getGuest().getIpAddress(),machineIp)){
					logger.log(Level.INFO,"Terminating Server "+machineIp);
					destroyVM(WAIT_UNTIL_FINISH, unit, eachVM);
					return true;
				}
			}
		}
		}catch(Exception e){
			logger.log(Level.SEVERE,"Cannot Terminate Server",e);
			throw new CloudProvisioningException("Cannot terminate server", e)	;
		}
		return false;
	}

	public void terminateServers(String serverNamePrefix) throws CloudProvisioningException {
		try{
			ManagedEntity[] mentities = new InventoryNavigator(rootFolder).searchManagedEntities(VIRTUAL_MACHINE);
			List<VirtualMachine> vmsToBeDestroyed= new ArrayList<VirtualMachine>();
			if(mentities !=null){
				VirtualMachine[] vms = Arrays.asList(mentities).toArray(new VirtualMachine[mentities.length]);

				for(VirtualMachine eachVM:vms){
					if(eachVM.getName().startsWith(serverNamePrefix)){
						vmsToBeDestroyed.add(eachVM);
						
					}
				}
				for(VirtualMachine eachVM:vmsToBeDestroyed){
					destroyVM(WAIT_UNTIL_FINISH, null, eachVM);
				}
			}
			}catch(Exception e){
				
				logger.log(Level.SEVERE,"Cannot Terminate Server",e);
				throw new CloudProvisioningException("Cannot terminate server", e)	;
			}
		
	}
	public void close() {
	    serviceInstance.getServerConnection().logout();		
	}
	
	private void destroyVM(long duration, TimeUnit unit,
			VirtualMachine virtualMachine) throws CloudProvisioningException {
		try {
		Task task = virtualMachine.powerOffVM_Task();
		if(duration > 0){
		task.wait(unit.toMillis(duration));
		}else{
			task.waitForTask();
		}
		if(task.getTaskInfo().state != TaskInfoState.success){
			throw new CloudProvisioningException("Cannot terminate server..Time expired when powering off")	;
		}
		task = virtualMachine.destroy_Task();
		if(duration > 0){
			task.wait(unit.toMillis(duration));
			}else{
				task.waitForTask();
			}
		if(task.getTaskInfo().state != TaskInfoState.success){
			throw new CloudProvisioningException("Cannot terminate server..Time expired when destroying vm")	;
		}
		}catch(Exception e){
			
			logger.log(Level.SEVERE,"Cannot Terminate Server",e);
			throw new CloudProvisioningException("Cannot terminate server", e)	;
		}
	}
	private VirtualMachine findVirtualMachine(
			String vmName) throws CloudProvisioningException {
		
		ManagedEntity managedEntity;
		try {
			managedEntity = new InventoryNavigator(rootFolder).searchManagedEntity(VIRTUAL_MACHINE, vmName);
		} catch (Exception e) {
			throw new CloudProvisioningException(e);
		}
		if(managedEntity == null){
			throw new CloudProvisioningException("Cannot find provided image " + vmName);
		}
		VirtualMachine virtualMachineTemplate = (VirtualMachine)managedEntity;
		return virtualMachineTemplate;
	}
}


cloud {
	// Mandatory. The name of the cloud, as it will appear in the Cloudify UI.
	name = "Openstack-Essex"
	configuration {
		// Mandatory - openstack Diablo cloud driver.
		className "org.cloudifysource.esc.driver.provisioning.openstack.essex.OpenstackEssexDriver"
		// Optional. The template name for the management machines. Defaults to the first template in the templates section below.
		managementMachineTemplate "MEDIUM_LINUX"
		// Optional. Indicates whether internal cluster communications should use the machine private IP. Defaults to true.
		connectToPrivateIp true
	}

	provider {
		// optional 
		provider "openstack-essex"
		localDirectory "tools/cli/plugins/esc/openstack-essex/upload"
		remoteDirectory "/root/gs-files"
		cloudifyUrl "http://171.68.121.203/gigaspaces-cloudify-2.1.0-rc-b1196.zip"
		cloudifyOverridesUrl "http://171.68.121.203/gigaspaces-overrides.zip"
		machineNamePrefix "app-Agent-"
		
		dedicatedManagementMachines true
		managementOnlyFiles ([])
		
		managementGroup "app-Management-"
		numberOfManagementMachines 1
		zones (["agent"])
		reservedMemoryCapacityPerMachineInMB 1024
		
	}
	user {
		user "root"
	//	apiKey "novaadmin"
	//	keyFile "ENTER_KEY_FILE"
	}
	templates ([
				MEDIUM_LINUX : template{
					imageId "de22bfba-269a-408b-bd53-588d89d04019"
					machineMemoryMB 2048
					hardwareId "6"
					username  "root"
					password  "server"
					//locationId "us-east-1"
					options ([
						"openstack.securityGroup" : "default",
						"openstack.keyPair" : "smxkey",
						// indicates if a floating IP should be assigned to this machine. Defaults to true.
						"openstack.allocate-floating-ip" : "true"
					])
					
				},
				NGINX_TEMPLATE : template{
					imageId "de22bfba-269a-408b-bd53-588d89d04019"
					machineMemoryMB 2048
					hardwareId "7"
					username  "root"
					password  "server"
					//locationId "us-east-1"
					options ([
						"openstack.securityGroup" : "default",
						"openstack.keyPair" : "smxkey",
						// indicates if a floating IP should be assigned to this machine. Defaults to true.
						"openstack.allocate-floating-ip" : "true"
					])
					custom ([ 
						"machineNamePrefix" : "app-nginx-"
						])

					
				},
				POSTGRESQL_TEMPLATE : template{
					imageId "de22bfba-269a-408b-bd53-588d89d04019"
					machineMemoryMB 2048
					hardwareId "7"
					username  "root"
					password  "server"
					//locationId "us-east-1"
					options ([
						"openstack.securityGroup" : "default",
						"openstack.keyPair" : "smxkey",
						// indicates if a floating IP should be assigned to this machine. Defaults to true.
						"openstack.allocate-floating-ip" : "true"
					])
					custom ([ 
						"machineNamePrefix" : "app-postgres-"
						])

					
				},
				TOMCAT_TEMPLATE : template{
					imageId "de22bfba-269a-408b-bd53-588d89d04019"
					machineMemoryMB 2048
					hardwareId "7"
					username  "root"
					password  "server"
					//locationId "us-east-1"
					options ([
						"openstack.securityGroup" : "default",
						"openstack.keyPair" : "smxkey",
						// indicates if a floating IP should be assigned to this machine. Defaults to true.
						"openstack.allocate-floating-ip" : "true"
					])
					custom ([ 
						"machineNamePrefix" : "app-tomcat-"
						])

					
				}
			])
			
	custom ([
		"openstack.endpoint": "http://10.194.53.153:5000",
		"openstack.identity.endpoint": "http://10.194.53.153:5000",
		"openstack.tenant" : "admin",
		"openstack.username" : "admin",
		"openstack.password" : "admin",
		"openstack.wireLog": "false"

	])
}


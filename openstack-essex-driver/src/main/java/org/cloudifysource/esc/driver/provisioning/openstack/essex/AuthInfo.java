package org.cloudifysource.esc.driver.provisioning.openstack.essex;

public class AuthInfo {

	private String token;
	private String computeServiceEndpointURL;
	public AuthInfo(String token, String computeServiceEndpointURL) {
		super();
		this.token = token;
		this.computeServiceEndpointURL = computeServiceEndpointURL;
	}
	public String getToken() {
		return token;
	}
	public String getComputeServiceEndpointURL() {
		return computeServiceEndpointURL;
	}
	
	
	
	
}

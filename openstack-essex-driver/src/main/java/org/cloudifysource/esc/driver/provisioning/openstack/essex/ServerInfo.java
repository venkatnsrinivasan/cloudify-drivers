package org.cloudifysource.esc.driver.provisioning.openstack.essex;

public class ServerInfo {

	private String id;
	private String url;
	private String ip;
	public ServerInfo(String id, String url,String ip) {
		super();
		this.id = id;
		this.url = url;
		this.ip = ip;
	}
	public String getIp() {
		return ip;
	}
	public String getId() {
		return id;
	}
	public String getUrl() {
		return url;
	}
	
	
}

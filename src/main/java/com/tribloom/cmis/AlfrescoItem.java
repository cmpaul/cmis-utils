package com.tribloom.cmis;

import java.util.HashMap;
import java.util.Map;

import org.apache.chemistry.opencmis.client.api.Folder;
import org.apache.chemistry.opencmis.commons.PropertyIds;

public class AlfrescoItem {

	// Destination should be a NodeRef, but can be GUID or path
	private Folder destinationObject;
	private String destination;
	private Map<String, String> properties = new HashMap<String, String>();
	private Map<String, String> associations = new HashMap<String, String>();
	private Object content;
	private String site;
	private String mimetype;

	public String getDestination() {
		return destination.trim();
	}
	public String getType() {
		return properties.get(PropertyIds.OBJECT_TYPE_ID);
	}
	public void setType(String type) {
		properties.put(PropertyIds.OBJECT_TYPE_ID, type.trim());
	}
	public void setDestination(String destination) {
		// TODO: Make this check to enforce this as a NodeRef
		this.destination = destination.trim();
	}
	public Map<String, String> getProperties() {
		return properties;
	}
	public void setProperties(Map<String, String> props) {
		for (String key : props.keySet()) {
			this.properties.put(key, props.get(key));
		}
	}
	public void addProperty(String key, String value) {
		if (key != null && value != null) {
			this.properties.put(key.trim(), value.trim());
		}
	}
	public Object getContent() {
		return content;
	}
	public void setContent(Object content) {
		this.content = content;
	}
	public Folder getDestinationObject() {
		return destinationObject;
	}
	public void setDestinationObject(Folder destinationObject) {
		this.destinationObject = destinationObject;
	}
	public void setName(String name) {
		properties.put(PropertyIds.NAME, name.trim());
	}	
	public String getName() {
		return properties.get(PropertyIds.NAME).trim();
	}
	public String getGUID() {
		if (properties != null && properties.containsKey("w:guid")) {
			return properties.get("w:guid");
		}
		return null;
	}
	public String toString() {
		return properties.toString() + " -> " + destination;
	}
	public void addAssociation(String associationName, String nodeRef) {
		associations.put(associationName, nodeRef);
	}
	public Map<String, String> getAssociations() {
		return associations;
	}
	public void setAssociations(Map<String, String> assocs) {
		this.associations = assocs;
	}
	public void setSiteName(String siteName) {
		this.site = siteName;
	}
	public String getSiteName() {
		return this.site;
	}
	/**
	 * An item is only valid if it has a name and a destination nodeRef 
	 * (or destination CmisObject or site name).
	 * @return boolean 
	 */
	public boolean isValid() {
		return (this.getName() != null && 
				(this.destination != null || (this.destinationObject != null || this.site != null)));
	}
	public String getMimetype() {
		return mimetype;
	}
	public void setMimetype(String type) {
		mimetype = type;
	}
}

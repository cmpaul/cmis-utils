package com.tribloom.cmis;

import java.util.Properties;

import org.apache.chemistry.opencmis.client.api.CmisObject;
import org.apache.chemistry.opencmis.client.api.Folder;
import org.apache.chemistry.opencmis.client.api.ItemIterable;
import org.apache.chemistry.opencmis.client.api.Session;

import com.tribloom.cmis.CmisUtils;

import junit.framework.TestCase;

public class TestCmisUtils extends TestCase {

	public void testCreateSession() {
		Properties properties = new Properties();
		properties.setProperty("host", "repo.opencmis.org");
		properties.setProperty("port", "");
		properties.setProperty("cmisPath", "/inmemory/atom/");
		
		Session session = CmisUtils.createSession(properties);
		assertNotNull(session);
		
        // Get everything in the root folder and print the names of the objects
        Folder root = session.getRootFolder();
        ItemIterable<CmisObject> children = root.getChildren();
        System.out.println("Found the following objects in the root folder:-");
        for (CmisObject o : children) {
            System.out.println(o.getName());
        }
	}
	
	public void testGetSiteFolder() {
		Properties properties = new Properties();
		properties.setProperty("host", "localhost");
		properties.setProperty("port", "8080");
		properties.setProperty("cmisPath", "/alfresco/cmisatom");
		
		Session session = CmisUtils.createSession(properties);
		assertNotNull(session);
		assertNotNull(CmisUtils.getSiteFolder("swsdp"));
	}
	
	public void testGetDocumentLibrary() {
		Properties properties = new Properties();
		properties.setProperty("host", "localhost");
		properties.setProperty("port", "8080");
		properties.setProperty("cmisPath", "/alfresco/cmisatom");
		
		Session session = CmisUtils.createSession(properties);
		assertNotNull(session);
		assertNotNull(CmisUtils.getDocumentLibrary("swsdp"));
	}
}

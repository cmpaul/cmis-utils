package com.tribloom.cmis;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.chemistry.opencmis.client.api.CmisObject;
import org.apache.chemistry.opencmis.client.api.Document;
import org.apache.chemistry.opencmis.client.api.Folder;
import org.apache.chemistry.opencmis.client.api.ItemIterable;
import org.apache.chemistry.opencmis.client.api.ObjectId;
import org.apache.chemistry.opencmis.client.api.ObjectType;
import org.apache.chemistry.opencmis.client.api.Property;
import org.apache.chemistry.opencmis.client.api.QueryResult;
import org.apache.chemistry.opencmis.client.api.Repository;
import org.apache.chemistry.opencmis.client.api.Session;
import org.apache.chemistry.opencmis.client.api.SessionFactory;
import org.apache.chemistry.opencmis.client.runtime.SessionFactoryImpl;
import org.apache.chemistry.opencmis.commons.PropertyIds;
import org.apache.chemistry.opencmis.commons.SessionParameter;
import org.apache.chemistry.opencmis.commons.data.ContentStream;
import org.apache.chemistry.opencmis.commons.definitions.PropertyDefinition;
import org.apache.chemistry.opencmis.commons.enums.BindingType;
import org.apache.chemistry.opencmis.commons.enums.VersioningState;
import org.apache.chemistry.opencmis.commons.exceptions.CmisContentAlreadyExistsException;
import org.apache.chemistry.opencmis.commons.exceptions.CmisRuntimeException;
import org.apache.chemistry.opencmis.commons.exceptions.CmisStreamNotSupportedException;
import org.apache.chemistry.opencmis.commons.impl.dataobjects.ContentStreamImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CmisUtils {
	
	// SL4J Logger
	private static final Logger logger = LoggerFactory.getLogger(CmisUtils.class);
	
	// Property names for CMIS connection
	private static final String PROP_HOSTNAME = "cmis.server.hostname";
	private static final String PROP_PORT = "cmis.server.port";
	private static final String PROP_USER = "cmis.user";
	private static final String PROP_PASSWORD = "cmis.password";
	private static final String PROP_SERVICE_PATH = "cmis.service.path";
	private static final String PROP_OVERWRITE = "cmis.utils.overwrite";

	// Default properties for CMIS connection
	private static String HOSTNAME = "localhost";
	private static String PORT = "8080";
	private static String USER = "admin";
	private static String PASSWORD = "admin";
	private static String CMIS_PATH = "/alfresco/cmisatom";
	private static boolean OVERWRITE = false;
	
	private static Session session;
	private static Map<String, Folder> siteFolderCache = new HashMap<String, Folder>();
	private static Map<String, Folder> docLibCache = new HashMap<String, Folder>();
	private static Map<String, CmisObject> cmisObjectCache = new HashMap<String, CmisObject>();
	
	/**
	 * Set up properties and connect to Alfresco.
	 * 
	 * Properties should consist of the following:
	 * - HOSTNAME
	 */
	public static Session createSession(Properties properties) {
		logger.info("Connecting to Alfresco...");
		
		if (properties.containsKey(PROP_HOSTNAME)) {
			HOSTNAME = properties.getProperty(PROP_HOSTNAME);	
		}
		if (properties.containsKey(PROP_PORT)) {
			PORT = properties.getProperty(PROP_PORT);	
		}
		if (properties.containsKey(PROP_USER)) {
			USER = properties.getProperty(PROP_USER);	
		}
		if (properties.containsKey(PROP_PASSWORD)) {
			PASSWORD = properties.getProperty(PROP_PASSWORD);	
		}
		if (properties.containsKey(PROP_SERVICE_PATH)) {
			CMIS_PATH = properties.getProperty(PROP_SERVICE_PATH);
		}
		if (properties.containsKey(PROP_OVERWRITE)) {
			OVERWRITE = ("true".equalsIgnoreCase(properties.getProperty(PROP_OVERWRITE)));
		}
		
		// Create a SessionFactory and set up the SessionParameter map
		SessionFactory sessionFactory = SessionFactoryImpl.newInstance();
		Map<String, String> parameter = new HashMap<String, String>();

		// Set up the Alfresco connection parameters
		String URL = "http://" + HOSTNAME + (!PORT.equals("") ? ":" : "") + PORT + CMIS_PATH;
		parameter.put(SessionParameter.ATOMPUB_URL, URL);
		parameter.put(SessionParameter.BINDING_TYPE, BindingType.ATOMPUB.value());
		parameter.put(SessionParameter.USER, USER);
		parameter.put(SessionParameter.PASSWORD, PASSWORD);
		parameter.put(SessionParameter.REPOSITORY_ID, "Main Repository");
		
		// Add the Alfresco OpenCMIS Extension, allowing us to work with aspects and data lists
		// For more information, see:
		//  https://code.google.com/a/apache-extras.org/p/alfresco-opencmis-extension/
		parameter.put(SessionParameter.OBJECT_FACTORY_CLASS,
				"org.alfresco.cmis.client.impl.AlfrescoObjectFactoryImpl");

		// There should only be one main repository, so grab the first one
		List<Repository> repositories = sessionFactory.getRepositories(parameter);
		session = repositories.get(0).createSession();
		logger.info("Connected!");
		return session;
	}
	
	public static Session getCurrentSession() {
		return session;
	}
	
	public static Folder getSiteFolder(String siteName) {
		if (getCurrentSession() == null) {
			logger.error("No session created");
			return null;
		}
		if (siteFolderCache.containsKey(siteName)) {
			return siteFolderCache.get(siteName);
		}

		String type = "cmis:folder";
		ObjectType typeObj = session.getTypeDefinition(type);
		PropertyDefinition<?> objIdPropDef = typeObj.getPropertyDefinitions().get(PropertyIds.OBJECT_ID);
		String objId = objIdPropDef.getQueryName();

		String query = "SELECT " + objId + " FROM " + typeObj.getQueryName() 
				+ " WHERE CONTAINS('=PATH:\"/app:company_home/st:sites/cm:" + siteName + "\"')";
		logger.debug("Query : " + query);
		
		ItemIterable<QueryResult> results = session.query(query, false);
		Folder folder = null;
		for (QueryResult result : results) {
			String objectId = result.getPropertyValueByQueryName(objId);
			folder = (Folder) session.getObject(session.createObjectId(objectId));
			debug(folder);
			// Assume there's only one, so if we found one, break
			break;
		}	
		
		// Cache this folder for future retrieval
		if (folder != null) {
			siteFolderCache.put(siteName, folder);	
		}
		
		return folder;
	}
	
	public static Folder getDocumentLibrary(String siteName) {
		if (getCurrentSession() == null) {
			logger.error("No session created");
			return null;
		}
		if (docLibCache.containsKey(siteName)) {
			return docLibCache.get(siteName); 
		}
		
		Folder site = getSiteFolder(siteName);
		
		String type = "cmis:folder";
		ObjectType typeObj = session.getTypeDefinition(type);
		PropertyDefinition<?> objIdPropDef = typeObj.getPropertyDefinitions().get(PropertyIds.OBJECT_ID);
		String objId = objIdPropDef.getQueryName();
		
		String query = "SELECT " + objId + " FROM " + typeObj.getQueryName() 
				+ " WHERE cmis:name = 'documentLibrary' AND IN_FOLDER('" + site.getId() + "')";
		logger.debug("Query : " + query);
		
		ItemIterable<QueryResult> results = session.query(query, false);
		Folder folder = null;
		for (QueryResult result : results) {
			String objectId = result.getPropertyValueByQueryName(objId);
			folder = (Folder) session.getObject(session.createObjectId(objectId));
			debug(folder);
			// Assume there's only one, so if we found one, break
			break;
		}
		
		// Cache this folder for future retrieval
		if (folder != null) {
			docLibCache.put(siteName, folder);
		}
		
		return folder;
	}
	
	/**
	 * Returns the data list container (folder) for the given site.
	 * 
	 * @param siteName String
	 * @param createIfNotFound boolean
	 * @return Folder
	 */
	public static Folder getDataListContainer(String siteName, boolean createIfNotFound) {
		if (getCurrentSession() == null) {
			logger.error("No session created");
			return null;
		}
		if (siteName == null) {
			logger.error("No site name provided");
			return null;
		}
		String dlType = "F:dl:dataList";
		ObjectType dlObjType = session.getTypeDefinition(dlType);
		PropertyDefinition<?> objIdPropDef = dlObjType.getPropertyDefinitions().get(PropertyIds.OBJECT_ID);
		String objId = objIdPropDef.getQueryName();
		
		String query = "SELECT " + objId + " FROM cmis:folder" 
				+ " WHERE CONTAINS('PATH:\"/app:company_home/st:sites/cm:" + siteName + "/cm:dataLists//.\"')";
		logger.debug("Query : " + query);
		
		ItemIterable<QueryResult> results = session.query(query, false);
		Folder containerFolder = null;
		for (QueryResult result : results) {
			String objectId = result.getPropertyValueByQueryName(objId);
			containerFolder = (Folder) session.getObject(session.createObjectId(objectId));
			debug(containerFolder);
			break;
		}
		
		if (containerFolder == null && createIfNotFound) {
			// If there is no data list container, create one
			logger.debug("Creating Data Lists folder");
			Folder siteFolder = getSiteFolder(siteName);
			if (siteFolder != null) {
				Map<String, Object> properties = new HashMap<String, Object>();
				properties.put(PropertyIds.NAME, "dataLists");
				properties.put(PropertyIds.OBJECT_TYPE_ID, "cmis:folder,P:st:siteContainer,P:cm:ownable,P:cm:titled,P:cm:tagscope");
				properties.put("st:componentId", "dataLists");
				properties.put("cm:owner", USER);
				properties.put("cm:description", "Data Lists");
				ObjectId newContainerId = session.createFolder(properties, siteFolder);
				containerFolder = (Folder) session.getObject(newContainerId);
			} else {
				// No site!
				logger.error("No site found: " + siteName);
			}
		}
		
		return containerFolder;
	}
	
	/**
	 * Returns all data list folders (dl:dataList) for the given site that contain
	 * the provided dl:dataListItemType. 
	 * 
	 * If the third parameter is true, it will attempt to create the data list 
	 * if it doesn't exist.
	 *  
	 * Note: The "Data Lists" site component may still need to be added to the site
	 *       from the Share "Customize Site" page to make it visible).
	 * 
	 * @param siteName String
	 * @param dataListItemType String
	 * @param createIfNotFound boolean
	 * @return List<Folder> data list folders
	 */
	public static List<Folder> getDataLists(String siteName, String dataListItemType, boolean createIfNotFound) {
		if (getCurrentSession() == null) {
			logger.error("No session created");
			return null;
		}
		if (siteName == null) {
			logger.error("No site name provided");
			return null;
		}
		if (dataListItemType == null) {
			logger.error("No data list item type provided");
			return null;
		}
		// Validate the provided dataListItemType
		// Note: CMIS notation requires the "D:" namespace, as it's technically 
		//       a "document" from the CMIS standpoint.
		String dlItemTypeStr = dataListItemType;
		if (!dataListItemType.startsWith("D:")) {
			dlItemTypeStr = "D:" + dataListItemType;
		}
		ObjectType dlItemTypeObj = session.getTypeDefinition(dlItemTypeStr);
		if (dlItemTypeObj == null) {
			logger.error("No data list item type defined for: " + dataListItemType);
			return null;
		}
		
		List<Folder> dataLists = new ArrayList<Folder>();
		
		String dlType = "F:dl:dataList";
		ObjectType dlTypeObj = session.getTypeDefinition(dlType);
		PropertyDefinition<?> objIdPropDef = dlTypeObj.getPropertyDefinitions().get(PropertyIds.OBJECT_ID);
		String objId = objIdPropDef.getQueryName();

		String query = "SELECT " + objId 
				+ " FROM " + dlTypeObj.getQueryName() 
				+ " WHERE dl:dataListItemType = '" + dataListItemType + "'" 
				+ " AND CONTAINS('PATH:\"/app:company_home/st:sites/cm:" + siteName + "/cm:dataLists//.\"')";
		logger.debug("Query : " + query);
		
		ItemIterable<QueryResult> results = session.query(query, false);
		for (QueryResult result : results) {
			String objectId = result.getPropertyValueByQueryName(objId);
			Folder dataListFolder = (Folder) session.getObject(session.createObjectId(objectId));
			debug(dataListFolder);
			dataLists.add(dataListFolder);
		}
		 
		if (dataLists.isEmpty() && createIfNotFound) {
			logger.debug("Creating new data list");
			// Find the data list container
			Folder containerFolder = getDataListContainer(siteName, createIfNotFound);
			
			// Create the Learning Objectives data list in the data list container
			Map<String, Object> properties = new HashMap<String, Object>();
			properties.put(PropertyIds.OBJECT_TYPE_ID, dlTypeObj.getId() + ",P:cm:titled");
			properties.put(PropertyIds.NAME, dlItemTypeObj.getDisplayName());
			properties.put("dl:dataListItemType", dataListItemType);
			properties.put("cm:description", "Imported data list: " + dlItemTypeObj.getDisplayName());
			ObjectId newDataListId = session.createFolder(properties, containerFolder);
			dataLists.add((Folder) session.getObject(newDataListId));
		}
		return dataLists;
	}
	
	/**
	 * Determine where the item should be created. This is done by 
	 * querying the item for it's destination NodeRef. If this is blank,
	 * and the item has a Site name set, then it will be created in that
	 * site's document library.
	 * 
	 * @param item ImportItem
	 */
	private static void configureDestination(AlfrescoItem item) {
		String dest = item.getDestination();
		if (dest == null || "".equals(dest)) {
			// If there's no destination, place the item in the Document Library
			String siteName = item.getSiteName();
			if (siteName != null) {
				Folder docLib = getDocumentLibrary(siteName);
				item.setDestination(docLib.getId());
				item.setDestinationObject(docLib);	
			}
		} else if (dest.contains("workspace://")) { // TODO: Update this when AlfrescoItem validates this 
			CmisObject destObj = cmisObjectCache.get(dest);
			if (destObj != null) {
				if (destObj instanceof Folder) {
					item.setDestinationObject((Folder) destObj);	
				} else {
					logger.error("Item destination is not a folder: " + dest);
					return;
				}
			} else {
				// TODO: Search for / create destination?
			}
		} else {
			logger.warn("Unable to process item destination: " + dest);
			logger.warn("  " + item.toString());
		}
	}
	
	public static CmisObject getChildByName(Folder folder, String name) {
		if (getCurrentSession() == null) {
			logger.error("No session created");
			return null;
		}
		if (folder == null) {
			logger.error("No folder provided");
			return null;
		}
		if (name == null) {
			logger.error("No name provided");
			return null;
		}
		for (CmisObject child : folder.getChildren()) {
			if (name.equals(child.getName())) {
				return child;
			}
		}
		return null;
	}
	
	/**
	 * Creates the item in Alfresco via CMIS and returns the ID (NodeRef) 
	 * of the new item.
	 * 
	 * @return String nodeRef
	 */
	public static String importItem(AlfrescoItem item) {
		if (!item.isValid()) {
			logger.error("Item is not valid");
			return null;
		}
		
		configureDestination(item);
		
		// At this point, we should have a CmisObject destination for the item
		if (item.getDestinationObject() == null) {
			logger.error("Unable to locate destination for item: " + item.getName());
			return null;
		}
		
		// The item type may be a comma-separated list of values.
		// For example, "cmis:folder,P:cm:titled" would represent a folder with the "cm:titled" aspect.
		// Grab the first type, which represents the primary type.
		String itemType = item.getType();
		if (itemType.indexOf(',') > -1) {
			itemType = itemType.substring(0, itemType.indexOf(','));
		}
		
		// Validate type name
		ObjectType itemTypeObj = session.getTypeDefinition(itemType);
		if (itemTypeObj == null) {
			logger.error("Invalid type: " + itemTypeObj);
			return null;
		}
	
		// Search for duplicates
		String name = item.getName();
		CmisObject cmisItem = getChildByName(item.getDestinationObject(), name);
		if (cmisItem != null) {
			if (OVERWRITE) {
				logger.info("Updating node: " + name);
				// TODO: Diff properties to see what needs to be updated
				cmisItem.updateProperties(item.getProperties());
				if (cmisItem instanceof Document) {
					Object contentObj = item.getContent();
					if (contentObj instanceof String) {
						// TODO: Diff content to see if this actually needs to be updated?
						byte[] content = ((String) contentObj).getBytes();
						InputStream stream = new ByteArrayInputStream(content);
						String type = "text/plain";
						if (name.endsWith(".xml")) {
							type = "text/xml";
						} else if (name.endsWith(".json")) {
							type = "application/json";
						}
						ContentStream contentStream = 
								new ContentStreamImpl(name, BigInteger.valueOf(content.length), type, stream);
						logger.debug("Updating content on " + name 
								+ " (" + cmisItem.getId() + ") with content (" + type + ")");
						logger.debug((String) contentObj);
						try {
							((Document) cmisItem).setContentStream(contentStream, true);
						} catch (CmisStreamNotSupportedException ex) {
							logger.warn("Unable to update node content...", ex);
						}
					}
				}	
			} else {
				logger.info("Existing item not updated: " + name + "...");
			}
		} else {
			Object contentObj = item.getContent();
			if (contentObj instanceof String) {
				if (item.getMimetype() == null) {
					logger.error("No mimetype provided");
					return null;
				}
				logger.info("Creating document: " + name);
				byte[] content = ((String) contentObj).getBytes();
				InputStream stream = new ByteArrayInputStream(content);
				ContentStream contentStream = 
						new ContentStreamImpl(name, BigInteger.valueOf(content.length), item.getMimetype(), stream);
				Folder parentFolder = item.getDestinationObject();
				cmisItem = parentFolder.createDocument(item.getProperties(), contentStream, VersioningState.MAJOR);
			} else {
				logger.info("Creating item: " + name);
				try {
					ObjectId newItemId = session.createItem(item.getProperties(), item.getDestinationObject());
					cmisItem = session.getObject(newItemId);
				} catch (CmisContentAlreadyExistsException ex) {
					// This should not get hit, since we're checking for duplicates above,
					// but I've seen instances where an item is not found from a search, 
					// which may be due to the indexes not updating in time?
					logger.warn("Content already exists");
					return null;
				}
			}
		}
		
		// Create associations, if they exist.
		// These are stored as a map of association type names to a comma-separated list 
		// of target objects (nodeRefs)
		Map<String, String> assocs = item.getAssociations();
		for (String assocName : assocs.keySet()) {
			logger.debug("Creating associations...");
			String targets = assocs.get(assocName);
			String[] targetArray = targets.split(",");
			for (String target : targetArray) {
				CmisObject targetObj = cmisObjectCache.get(target);
				if (targetObj != null) {
					Map<String, String> properties = new HashMap<String, String>();
					properties.put(PropertyIds.SOURCE_ID, cmisItem.getId());
					properties.put(PropertyIds.TARGET_ID, targetObj.getId());
					properties.put(PropertyIds.OBJECT_TYPE_ID, "R:" + assocName);
					try {
						session.createRelationship(properties);	
					} catch (CmisRuntimeException ex) {
						logger.warn("Relationship already exists");
					}
				} else {
					logger.warn("No CmisObject found for ID: " + target);
					
				}
			}
		}
		
		// Cache this item in case it's needed for an association or as a destination
		if (cmisItem != null) {
			cmisObjectCache.put(cmisItem.getId(), cmisItem);	
			return cmisItem.getId();
		}

		return null;
	}
	
	/**
	 * Helper method to display debug output for a CmisObject.
	 * @param obj CmisObject
	 */
	private static void debug(CmisObject obj) {
		if (logger.isDebugEnabled()) {
			logger.debug("CmisObject: " + obj.getName() + " (" + obj.getId() + ")");
			logger.debug("  Type: " + obj.getType().getDisplayName());
			logger.debug("  Properties:");
			for (Property<?> prop : obj.getProperties()) {
				logger.debug("    " + prop.getDisplayName() + " (" + prop.getId() + ") = " + prop.getValueAsString());
			}	
		}
	}
}

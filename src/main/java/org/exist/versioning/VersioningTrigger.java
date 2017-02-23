/**
 * Versioning Module for eXist-db XQuery
 * Copyright (C) 2008 eXist-db <exit-open@lists.sourceforge.net>
 * http://exist-db.org
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 1, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
 */
package org.exist.versioning;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.Map.Entry;

import javax.xml.transform.OutputKeys;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.exist.EXistException;
import org.exist.collections.Collection;
import org.exist.collections.IndexInfo;
import org.exist.collections.triggers.FilteringTrigger;
import org.exist.collections.triggers.TriggerException;
import org.exist.dom.persistent.BinaryDocument;
import org.exist.dom.persistent.DocumentImpl;
import org.exist.dom.QName;
import org.exist.security.Account;
import org.exist.security.PermissionDeniedException;
import org.exist.security.Subject;
import org.exist.storage.BrokerPool;
import org.exist.storage.DBBroker;
import org.exist.storage.lock.Lock;
import org.exist.storage.txn.Txn;
import org.exist.util.LockException;
import org.exist.util.serializer.Receiver;
import org.exist.util.serializer.SAXSerializer;
import org.exist.util.serializer.SerializerPool;
import org.exist.xmldb.XmldbURI;
import org.exist.xquery.XPathException;
import org.exist.xquery.value.DateTimeValue;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;

public class VersioningTrigger extends FilteringTrigger {

    public final static Logger LOG = LogManager.getLogger(VersioningTrigger.class);

    public final static XmldbURI VERSIONS_COLLECTION = XmldbURI.SYSTEM_COLLECTION_URI.append("versions");

    public final static String BASE_SUFFIX = ".base";
    public final static String TEMP_SUFFIX = ".tmp";
    public final static String DELETED_SUFFIX = ".deleted";
    public final static String BINARY_SUFFIX = ".binary";
    public final static String XML_SUFFIX = ".xml";

    public final static String PARAM_OVERWRITE = "overwrite";

    public final static QName ELEMENT_VERSION = new QName("version", StandardDiff.NAMESPACE, StandardDiff.PREFIX);
    public final static QName ELEMENT_REMOVED = new QName("removed", StandardDiff.NAMESPACE, StandardDiff.PREFIX);
    public final static QName PROPERTIES_ELEMENT = new QName("properties", StandardDiff.NAMESPACE, StandardDiff.PREFIX);
    public final static QName ELEMENT_REPLACED_BINARY = new QName("replaced-binary", StandardDiff.NAMESPACE, StandardDiff.PREFIX);
    public final static QName ATTRIBUTE_REF = new QName("ref");
    public final static QName ELEMENT_REPLACED_XML = new QName("replaced-xml", StandardDiff.NAMESPACE, StandardDiff.PREFIX);

    private final static Object latch = new Object();

    private DBBroker broker;
    private XmldbURI documentPath;
    private DocumentImpl lastRev = null;
    private boolean removeLast = false;
    private Collection vCollection;
    private DocumentImpl vDoc = null;

    private int elementStack = 0;

    private String documentKey = null;
    private String documentRev = null;
    private boolean checkForConflicts = false;

    @Override
    public void configure(final DBBroker broker, final Collection parent, final Map<String, List<?>> parameters)
    throws TriggerException {
        super.configure(broker, parent, parameters);
        
        if (parameters != null) {
			final String allowOverwrite = (String) parameters.get(PARAM_OVERWRITE).get(0);
            if (allowOverwrite != null) {
				checkForConflicts = allowOverwrite.equals("false") || allowOverwrite.equals("no");
			}
        }

        if(LOG.isDebugEnabled()) {
			LOG.debug("checkForConflicts: " + checkForConflicts);
		}
    }

    //XXX: is it safe to delete?
    @Deprecated
    public void finish(final int event, final DBBroker brk, final Txn transaction, XmldbURI documentPath,
            final DocumentImpl document) {

    	if (documentPath == null || documentPath.startsWith(VERSIONS_COLLECTION)) {
			return;
		}

		final Subject activeSubject = brk.getCurrentSubject();
		final BrokerPool brokerPool = brk.getBrokerPool();
    	try(final DBBroker broker = brokerPool.get(Optional.of(brokerPool.getSecurityManager().getSystemSubject()))) {

    		if (vDoc != null && !removeLast) {
    			if(!(vDoc instanceof BinaryDocument)) {
    				try {
    					vDoc.getUpdateLock().acquire(Lock.LockMode.WRITE_LOCK);
    					vCollection.addDocument(transaction, broker, vDoc);
    					broker.storeXMLResource(transaction, vDoc);
    				} catch (final LockException | PermissionDeniedException e) {
    					LOG.error("Versioning trigger could not store base document: " + vDoc.getFileURI() +
    							e.getMessage(), e);
    				} finally {
    					vDoc.getUpdateLock().release(Lock.LockMode.WRITE_LOCK);
    				}
    			}
    		}

    		if (event == STORE_DOCUMENT_EVENT) {
    			try {
    				vCollection = getVersionsCollection(broker, transaction, documentPath.removeLastSegment());

    				final String existingURI = document.getFileURI().toString();
    				final XmldbURI deletedURI = XmldbURI.create(existingURI + DELETED_SUFFIX);
    				lastRev = vCollection.getDocument(broker, deletedURI);
    				if (lastRev == null) {
    					lastRev = vCollection.getDocument(broker, XmldbURI.create(existingURI + BASE_SUFFIX));
    					removeLast = false;
    				} else {
						removeLast = true;
					}
    			} catch (final IOException | TriggerException e) {
    				LOG.error("Caught exception in VersioningTrigger: " + e.getMessage(), e);
    			} catch (final PermissionDeniedException e) {
    				LOG.error("Permission denied in VersioningTrigger: " + e.getMessage(), e);
    			}
			}

    		if (lastRev != null || event == REMOVE_DOCUMENT_EVENT) {
    			try {
    				final long revision = newRevision(broker.getBrokerPool());
    				if (documentPath.isCollectionPathAbsolute()) {
						documentPath = documentPath.lastSegment();
					}
    				final XmldbURI diffUri = XmldbURI.createInternal(documentPath.toString() + '.' + revision);

    				vCollection.setTriggersEnabled(false);

    				try(StringWriter writer = new StringWriter()) {
						final SAXSerializer sax = (SAXSerializer) SerializerPool.getInstance().borrowObject(
								SAXSerializer.class);
						final Properties outputProperties = new Properties();
						outputProperties.setProperty(OutputKeys.OMIT_XML_DECLARATION, "no");
						outputProperties.setProperty(OutputKeys.INDENT, "no");
						sax.setOutput(writer, outputProperties);

						sax.startDocument();
						sax.startElement(ELEMENT_VERSION, null);
						writeProperties(sax, getVersionProperties(revision, documentPath, activeSubject));

						if (event == REMOVE_DOCUMENT_EVENT) {
							sax.startElement(ELEMENT_REMOVED, null);
							sax.endElement(ELEMENT_REMOVED);
						} else {

							//Diff
							if (document instanceof BinaryDocument) {
								//create a copy of the last Binary revision
								final XmldbURI binUri = XmldbURI.create(diffUri.toString() + BINARY_SUFFIX);
								broker.copyResource(transaction, document, vCollection, binUri);

								//Create metadata about the last Binary Version
								sax.startElement(ELEMENT_REPLACED_BINARY, null);
								sax.attribute(ATTRIBUTE_REF, binUri.toString());
								sax.endElement(ELEMENT_REPLACED_BINARY);
							} else if (lastRev instanceof BinaryDocument) {
								//create a copy of the last XML revision
								final XmldbURI xmlUri = XmldbURI.create(diffUri.toString() + XML_SUFFIX);
								broker.copyResource(transaction, document, vCollection, xmlUri);

								//Create metadata about the last Binary Version
								sax.startElement(ELEMENT_REPLACED_XML, null);
								sax.attribute(ATTRIBUTE_REF, xmlUri.toString());
								sax.endElement(ELEMENT_REPLACED_BINARY);
							} else {
								//Diff the XML versions
								final Diff diff = new StandardDiff(broker);
								diff.diff(lastRev, document);
								diff.diff2XML(sax);
							}

							sax.endElement(ELEMENT_VERSION);

							sax.endDocument();
							final String editscript = writer.toString();

							if (removeLast) {
								if (lastRev instanceof BinaryDocument) {
									vCollection.removeBinaryResource(transaction, broker, lastRev.getFileURI());
								} else {
									vCollection.removeXMLResource(transaction, broker, lastRev.getFileURI());
								}
							}

							final IndexInfo info = vCollection.validateXMLResource(transaction, broker, diffUri, editscript);
							vCollection.store(transaction, broker, info, editscript);
						}
					}
    			} catch (final Exception e) {
    				LOG.error("Caught exception in VersioningTrigger: " + e.getMessage(), e);
    			} finally {
    				vCollection.setTriggersEnabled(true);
    			}
    		}
		} catch (final EXistException e) {
			LOG.error("Caught exception in VersioningTrigger: " + e.getMessage(), e);
		}
    }
    
    private void before(final DBBroker brk, final Txn transaction, final DocumentImpl document, final boolean remove)
            throws TriggerException {
        this.documentPath = document.getURI();

        if (documentPath.startsWith(VERSIONS_COLLECTION)) {
			return;
		}

        this.broker = brk;

		final BrokerPool brokerPool = brk.getBrokerPool();
		try(final DBBroker broker = brokerPool.get(Optional.of(brokerPool.getSecurityManager().getSystemSubject()))) {

            final Collection collection = document.getCollection();
            if (collection.getURI().startsWith(VERSIONS_COLLECTION)) {
				return;
			}
            vCollection = getVersionsCollection(broker, transaction, documentPath.removeLastSegment());

            final String existingURI = document.getFileURI().toString();
            final XmldbURI baseURI = XmldbURI.create(existingURI + BASE_SUFFIX);
            final DocumentImpl baseRev = vCollection.getDocument(broker, baseURI);

            final String vFileName;
            if (baseRev == null) {
                vFileName = baseURI.toString();
                removeLast = false;
                // copy existing document to base revision here!
                broker.copyResource(transaction, document, vCollection, baseURI);
            } else if (remove) {
                vFileName = existingURI + DELETED_SUFFIX;
                removeLast = false;
            } else {
                vFileName = existingURI + TEMP_SUFFIX;
                removeLast = true;
            }

            // setReferenced(true) will tell the broker that the document
            // data is referenced from another document and should not be
            // deleted when the original document is removed.
            document.getMetadata().setReferenced(true);


            if(document instanceof BinaryDocument) {
                final XmldbURI binUri = XmldbURI.createInternal(vFileName);
                broker.copyResource(transaction, document, vCollection, binUri);
                vDoc = vCollection.getDocument(broker, binUri);
            } else {
                vDoc = new DocumentImpl(broker.getBrokerPool(), vCollection, XmldbURI.createInternal(vFileName));
                vDoc.copyOf(document, true);
                vDoc.copyChildren(document);
            }
            
            if (!remove) {
                lastRev = vDoc;
            }           
        
        } catch (final PermissionDeniedException e) {
            throw new TriggerException("Permission denied in VersioningTrigger: " + e.getMessage(), e);
        } catch (final Exception e) {
            LOG.error("Caught exception in VersioningTrigger: " + e.getMessage(), e);
        }
    }

    private void after(final DBBroker brk, final Txn transaction, final DocumentImpl document,
            final boolean remove) {
    	if (documentPath == null || documentPath.startsWith(VERSIONS_COLLECTION)) {
			return;
		}

		final Subject activeSubject = brk.getCurrentSubject();
		final BrokerPool brokerPool = brk.getBrokerPool();
		try(final DBBroker broker = brokerPool.get(Optional.of(brokerPool.getSecurityManager().getSystemSubject()))) {

    		if (!remove) {
    			try {
    				vCollection = getVersionsCollection(broker, transaction, documentPath.removeLastSegment());

    				final String existingURI = document.getFileURI().toString();
    				final XmldbURI deletedURI = XmldbURI.create(existingURI + DELETED_SUFFIX);
    				lastRev = vCollection.getDocument(broker, deletedURI);
    				if (lastRev == null) {
    					lastRev = vCollection.getDocument(broker, XmldbURI.create(existingURI + BASE_SUFFIX));
    					removeLast = false;
    				} else {
						removeLast = true;
					}
    			} catch (final IOException | TriggerException e) {
    				LOG.error("Caught exception in VersioningTrigger: " + e.getMessage(), e);
    			} catch (final PermissionDeniedException e) {
    				LOG.error("Permission denied in VersioningTrigger: " + e.getMessage(), e);
    			}
			}

    		if (lastRev != null || remove) {
    			try {

    				final long revision = newRevision(broker.getBrokerPool());
    				if (documentPath.isCollectionPathAbsolute()) {
    					documentPath = documentPath.lastSegment();
    				}
    				final XmldbURI diffUri = XmldbURI.createInternal(documentPath.toString() + '.' + revision);

    				vCollection.setTriggersEnabled(false);

    				try(final StringWriter writer = new StringWriter()) {
						final SAXSerializer sax = (SAXSerializer) SerializerPool.getInstance().borrowObject(
								SAXSerializer.class);
						final Properties outputProperties = new Properties();
						outputProperties.setProperty(OutputKeys.OMIT_XML_DECLARATION, "no");
						outputProperties.setProperty(OutputKeys.INDENT, "no");
						sax.setOutput(writer, outputProperties);

						sax.startDocument();
						sax.startElement(ELEMENT_VERSION, null);
						writeProperties(sax, getVersionProperties(revision, documentPath, activeSubject));

						if (remove) {
							sax.startElement(ELEMENT_REMOVED, null);
							sax.endElement(ELEMENT_REMOVED);
						} else {

							//Diff
							if (document instanceof BinaryDocument) {
								//create a copy of the last Binary revision
								final XmldbURI binUri = XmldbURI.create(diffUri.toString() + BINARY_SUFFIX);
								broker.copyResource(transaction, document, vCollection, binUri);

								//Create metadata about the last Binary Version
								sax.startElement(ELEMENT_REPLACED_BINARY, null);
								sax.attribute(ATTRIBUTE_REF, binUri.toString());
								sax.endElement(ELEMENT_REPLACED_BINARY);
							} else if (lastRev instanceof BinaryDocument) {
								//create a copy of the last XML revision
								final XmldbURI xmlUri = XmldbURI.create(diffUri.toString() + XML_SUFFIX);
								broker.copyResource(transaction, document, vCollection, xmlUri);

								//Create metadata about the last Binary Version
								sax.startElement(ELEMENT_REPLACED_XML, null);
								sax.attribute(ATTRIBUTE_REF, xmlUri.toString());
								sax.endElement(ELEMENT_REPLACED_XML);
							} else {
								//Diff the XML versions
								final Diff diff = new StandardDiff(broker);
								diff.diff(lastRev, document);
								diff.diff2XML(sax);
							}

							sax.endElement(ELEMENT_VERSION);

							sax.endDocument();
							final String editscript = writer.toString();

							if (removeLast) {
								if (lastRev instanceof BinaryDocument) {
									vCollection.removeBinaryResource(transaction, broker, lastRev.getFileURI());
								} else {
									vCollection.removeXMLResource(transaction, broker, lastRev.getFileURI());
								}
							}

							final IndexInfo info = vCollection.validateXMLResource(transaction, broker, diffUri, editscript);
							vCollection.store(transaction, broker, info, editscript);
						}
					}
    			} catch (final Exception e) {
    				LOG.error("Caught exception in VersioningTrigger: " + e.getMessage(), e);
    			} finally {
    				vCollection.setTriggersEnabled(true);
    			}
    		}
		} catch (final EXistException e) {
			LOG.error("Caught exception in VersioningTrigger: " + e.getMessage(), e);
		}
    }

    private Properties getVersionProperties(final long revision, final XmldbURI documentPath,
            final Account commitAccount) throws XPathException {
        final Properties properties = new Properties();
        properties.setProperty("document", documentPath.toString());
        properties.setProperty("revision", Long.toString(revision));
        properties.setProperty("date", new DateTimeValue(new Date()).getStringValue());
        properties.setProperty("user", commitAccount.getName());
        if (documentKey != null) {
            properties.setProperty("key", documentKey);
        }
        return properties;
    }

    public static void writeProperties(final Receiver receiver, final Properties properties) throws SAXException {
        receiver.startElement(PROPERTIES_ELEMENT, null);
        for (final Entry<Object, Object> entry : properties.entrySet()) {
            final QName qn = new QName((String)entry.getKey(), StandardDiff.NAMESPACE, StandardDiff.PREFIX);
            receiver.startElement(qn, null);
            receiver.characters(entry.getValue().toString());
            receiver.endElement(qn);
        }
        receiver.endElement(PROPERTIES_ELEMENT);
    }

    private Collection getVersionsCollection(final DBBroker broker, final Txn transaction,
            final XmldbURI collectionPath) throws IOException, PermissionDeniedException, TriggerException {
        final XmldbURI path = VERSIONS_COLLECTION.append(collectionPath);
        Collection collection = broker.openCollection(path, Lock.LockMode.WRITE_LOCK);
        
        if (collection == null) {
            if(LOG.isDebugEnabled()) {
				LOG.debug("Creating versioning collection: " + path);
			}
            collection = broker.getOrCreateCollection(transaction, path);
            broker.saveCollection(transaction, collection);
        } else {
            transaction.registerLock(collection.getLock(), Lock.LockMode.WRITE_LOCK);
        }
        
        return collection;
    }

    private long newRevision(final BrokerPool pool) {
        final String dataDir = (String) pool.getConfiguration().getProperty(BrokerPool.PROPERTY_DATA_DIR);
        synchronized (latch) {
            final Path f = Paths.get(dataDir, "versions.dbx");
            long rev = 0;
            
            if (Files.isReadable(f)) {
                try(final DataInputStream is = new DataInputStream(Files.newInputStream(f))) {
                    rev = is.readLong();
                } catch (final IOException e) {
                    LOG.error("Failed to read versions.dbx: " + e.getMessage(), e);
                }
            }
            
            ++rev;
            
            try(final DataOutputStream os = new DataOutputStream(Files.newOutputStream(f))) {
                os.writeLong(rev);
            } catch (final IOException e) {
                LOG.error("Failed to write versions.dbx: " + e.getMessage(), e);
            }

            return rev;
        }
    }

    @Override
    public void startElement(final String namespaceURI, final String localName, final String qname,
            Attributes attributes) throws SAXException {
        if (checkForConflicts && isValidating() && elementStack == 0) {
            for (int i = 0; i < attributes.getLength(); i++) {
                if (StandardDiff.NAMESPACE.equals(attributes.getURI(i))) {
                    final String attrName = attributes.getLocalName(i);
                    if (VersioningFilter.ATTR_KEY.getLocalPart().equals(attrName)) {
						documentKey = attributes.getValue(i);
					} else if (VersioningFilter.ATTR_REVISION.getLocalPart().equals(attrName)) {
						documentRev = attributes.getValue(i);
					}
                }
            }
           
            if (documentKey != null && documentRev != null) {
            	if(LOG.isDebugEnabled()) {
					LOG.debug("v:key = " + documentKey + "; v:revision = " + documentRev);
				}

                try {
                    final long rev = Long.parseLong(documentRev);
                    if (VersioningHelper.newerRevisionExists(broker, documentPath, rev, documentKey)) {
                        final long baseRev = VersioningHelper.getBaseRevision(broker, documentPath, rev, documentKey);
                        if(LOG.isDebugEnabled()) {
							LOG.debug("base revision: " + baseRev);
						}
                        throw new TriggerException("Possible version conflict detected for document: " + documentPath);
                    }
                } catch (final XPathException | IOException | PermissionDeniedException e) {
                    LOG.error("Internal error in VersioningTrigger: " + e.getMessage(), e);
                } catch (final NumberFormatException e) {
                    LOG.error("Illegal revision number in VersioningTrigger: " + documentRev);
                }
			}
        }
        
        if (elementStack == 0) {
            // Remove the versioning attributes which were inserted during serialization. We don't want
            // to store them in the db
            final AttributesImpl nattrs = new AttributesImpl();
            for (int i = 0; i < attributes.getLength(); i++) {
                if (!StandardDiff.NAMESPACE.equals(attributes.getURI(i))) {
					nattrs.addAttribute(attributes.getURI(i), attributes.getLocalName(i),
							attributes.getQName(i), attributes.getType(i), attributes.getValue(i));
				}
            }
            attributes = nattrs;
        }
        
        elementStack++;
        super.startElement(namespaceURI, localName, qname, attributes);
    }

    @Override
    public void endElement(final String namespaceURI, final String localName, final String qname) throws SAXException {
        elementStack--;
        super.endElement(namespaceURI, localName, qname);
    }

    @Override
    public void startPrefixMapping(final String prefix, final String namespaceURI) throws SAXException {
        if (StandardDiff.NAMESPACE.equals(namespaceURI)) {
			return;
		}
        super.startPrefixMapping(prefix, namespaceURI);
    }

	@Override
	public void beforeCreateDocument(final DBBroker broker, final Txn transaction, final XmldbURI uri)
			throws TriggerException {
		this.documentPath = uri;
	}

	@Override
	public void afterCreateDocument(final DBBroker broker, final Txn transaction, final DocumentImpl document)
	throws TriggerException {
		after(broker, transaction, document, false);
	}

	@Override
	public void beforeUpdateDocument(final DBBroker broker, final Txn transaction, final DocumentImpl document)
	throws TriggerException {
		before(broker, transaction, document, false);
	}

	@Override
	public void afterUpdateDocument(final DBBroker broker, final Txn transaction, final DocumentImpl document)
	throws TriggerException {
		after(broker, transaction, document, false);
	}

	@Override
	public void beforeCopyDocument(final DBBroker broker, final Txn transaction, final DocumentImpl document,
            final XmldbURI newUri) throws TriggerException {
	}

	@Override
	public void afterCopyDocument(final DBBroker broker, final Txn transaction, final DocumentImpl document,
            final XmldbURI newUri) throws TriggerException {
		after(broker, transaction, document, false);
	}

	@Override
	public void beforeMoveDocument(final DBBroker broker, final Txn transaction, final DocumentImpl document,
            final XmldbURI newUri) throws TriggerException {
		before(broker, transaction, document, true);
	}

	@Override
	public void afterMoveDocument(final DBBroker broker, final Txn transaction, final DocumentImpl document,
            final XmldbURI oldUri) throws TriggerException {
		after(broker, transaction, null, true);
	}

	@Override
	public void beforeDeleteDocument(final DBBroker broker, final Txn transaction, final DocumentImpl document)
            throws TriggerException {
		before(broker, transaction, document, true);
	}

	@Override
	public void afterDeleteDocument(final DBBroker broker, final Txn transaction, final XmldbURI uri)
            throws TriggerException {
		after(broker, transaction, null, true);
	}

	@Override
	public void beforeUpdateDocumentMetadata(final DBBroker broker, final Txn txn, final DocumentImpl document)
            throws TriggerException {
	}

	@Override
	public void afterUpdateDocumentMetadata(final DBBroker broker, final Txn txn, final DocumentImpl document)
            throws TriggerException {
	}
}
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

import org.exist.dom.persistent.DocumentImpl;
import org.exist.dom.persistent.NodeHandle;
import org.exist.dom.QName;
import org.exist.util.serializer.AttrList;
import org.exist.security.PermissionDeniedException;
import org.exist.storage.serializers.CustomMatchListener;
import org.exist.xquery.XPathException;
import org.exist.xmldb.XmldbURI;
import org.xml.sax.SAXException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

public class VersioningFilter extends CustomMatchListener {

    private final static Logger LOG = LogManager.getLogger(VersioningFilter.class);

    public final static QName ATTR_REVISION = new QName("revision", StandardDiff.NAMESPACE, StandardDiff.PREFIX);
    public final static QName ATTR_KEY = new QName("key", StandardDiff.NAMESPACE, StandardDiff.PREFIX);
    public final static QName ATTR_PATH = new QName("path", StandardDiff.NAMESPACE, StandardDiff.PREFIX);

    private int elementStack = 0;

    @Override
    public void startElement(final QName qname, final AttrList attribs) throws SAXException {
        if (elementStack == 0) {
            final NodeHandle node = getCurrentNode();
            if (node != null) {
                final DocumentImpl doc = node.getOwnerDocument();
                final XmldbURI uri = doc.getURI();
                if (!uri.startsWith(XmldbURI.SYSTEM_COLLECTION_URI)) {
                    
                    if (doc.getCollection().getConfiguration(getBroker()).triggerRegistered(VersioningTrigger.class)) {
                        try {
                            final long rev = VersioningHelper.getCurrentRevision(getBroker(), doc.getURI());
                            final long time = System.currentTimeMillis();
                            final String key = Long.toHexString(time) + Long.toHexString(rev);
                            attribs.addAttribute(ATTR_REVISION, rev == 0 ? "0" : Long.toString(rev));
                            attribs.addAttribute(ATTR_KEY, key);
                            attribs.addAttribute(ATTR_PATH, doc.getURI().toString());
                        } catch (XPathException | IOException | PermissionDeniedException e) {
                            LOG.error("Exception while retrieving versioning info: " + e.getMessage(), e);
                        }
                    }
                }
            }
        }
        ++elementStack;
        nextListener.startElement(qname, attribs); 
    }

    @Override
    public void endElement(final QName qname) throws SAXException {
        --elementStack;
        nextListener.endElement(qname);
    }
}
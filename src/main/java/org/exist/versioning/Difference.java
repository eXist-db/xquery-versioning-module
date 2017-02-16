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

import org.exist.dom.persistent.NodeProxy;
import org.exist.dom.persistent.AttrImpl;
import org.exist.dom.persistent.DocumentImpl;
import org.exist.dom.QName;
import org.exist.numbering.NodeId;
import org.exist.storage.DBBroker;
import org.exist.storage.serializers.Serializer;
import org.exist.util.serializer.Receiver;
import org.exist.util.serializer.AttrList;
import org.xml.sax.SAXException;

import javax.xml.stream.XMLStreamReader;

public abstract class Difference implements Comparable<Difference> {

    public final static int INSERT = 0;
    public final static int DELETE = 1;
    public final static int APPEND = 2;
    public final static int UPDATE = 3;

    public final static QName ELEMENT_INSERT = new QName("insert", StandardDiff.NAMESPACE, StandardDiff.PREFIX);
    public final static QName ATTR_REF = new QName("ref", "", "");
    public final static QName ATTR_NAMESPACE = new QName("namespace", "", "");
    public final static QName ATTR_NAME = new QName("name", "", "");
    public final static QName ATTR_EVENT = new QName("event", "", "");
    public final static QName ELEMENT_ATTRIBUTE = new QName("attribute", StandardDiff.NAMESPACE, StandardDiff.PREFIX);
    public final static QName ELEMENT_START = new QName("start", StandardDiff.NAMESPACE, StandardDiff.PREFIX);
    public final static QName ELEMENT_END = new QName("end", StandardDiff.NAMESPACE, StandardDiff.PREFIX);
    public final static QName ELEMENT_COMMENT = new QName("comment", StandardDiff.NAMESPACE, StandardDiff.PREFIX);
    public final static QName ELEMENT_APPEND = new QName("append", StandardDiff.NAMESPACE, StandardDiff.PREFIX);
    public final static QName ELEMENT_DELETE = new QName("delete", StandardDiff.NAMESPACE, StandardDiff.PREFIX);

    protected final int type;
    protected final NodeProxy refChild;

    public Difference(final int type, final NodeProxy refChild) {
        this.type = type;
        this.refChild = refChild;
    }

    public abstract void serialize(final DBBroker broker, final Receiver handler);

    @Override
    public boolean equals(final Object obj) {
        final Difference other = (Difference) obj;
        return refChild.getNodeId().equals(other.refChild.getNodeId());
    }

    @Override
    public int compareTo(final Difference other) {
        return refChild.getNodeId().compareTo(other.refChild.getNodeId());
    }

    public static class Insert extends Difference {

        protected final DocumentImpl otherDoc;
        protected DiffNode[] nodes;

        public Insert(final NodeProxy reference, final DocumentImpl otherDoc) {
            super(INSERT, reference);
            this.otherDoc = otherDoc;
        }

        public Insert(final int type, final NodeProxy reference, final DocumentImpl otherDoc) {
            super(type, reference);
            this.otherDoc = otherDoc;
        }

        protected void addNodes(final DiffNode[] nodesToAdd) {
            if (nodes == null) {
                nodes = nodesToAdd;
            } else {
                final DiffNode n[] = new DiffNode[nodes.length + nodesToAdd.length];
                System.arraycopy(nodes, 0, n, 0, nodes.length);
                System.arraycopy(nodesToAdd, 0, n, nodes.length, nodesToAdd.length);
                nodes = n;
            }
        }

        @Override
        public void serialize(final DBBroker broker, final Receiver handler) {
            try {
                final AttrList attribs = new AttrList();
                attribs.addAttribute(ATTR_REF, refChild.getNodeId().toString());
                handler.startElement(ELEMENT_INSERT, attribs);
                serializeChildren(broker, handler);
                handler.endElement(ELEMENT_INSERT);
            } catch (final SAXException e) {
                e.printStackTrace();
            }
        }

        protected void serializeChildren(final DBBroker broker, final Receiver handler) throws SAXException {
            AttrList attribs;
            for (int i = 0; i < nodes.length; i++) {
                switch (nodes[i].nodeType) {
                    case XMLStreamReader.ATTRIBUTE:
                        attribs = new AttrList();
                        final AttrImpl attr = (AttrImpl) broker.objectWith(otherDoc, nodes[i].nodeId);
                        attribs.addAttribute(new QName(attr.getLocalName(), attr.getNamespaceURI(), attr.getPrefix()),
                                attr.getValue(), attr.getType());
                        handler.startElement(ELEMENT_ATTRIBUTE, attribs);
                        handler.endElement(ELEMENT_ATTRIBUTE);
                        break;
                    case XMLStreamReader.START_ELEMENT:
                        // check if there's a complete element to write, not just a start or end tag
                        // if yes, just copy the element, if no, write a start-tag node
                        boolean isClosed = false;
                        final NodeId nodeId = nodes[i].nodeId;
                        for (int j = i; j < nodes.length; j++) {
                            if (nodes[j].nodeType == XMLStreamReader.END_ELEMENT &&
                                    nodes[j].nodeId.equals(nodeId)) {
                                isClosed = true;
                                final NodeProxy proxy = new NodeProxy(otherDoc, nodes[i].nodeId);
                                final Serializer serializer = broker.getSerializer();
                                serializer.reset();
                                serializer.setProperty(Serializer.GENERATE_DOC_EVENTS, "false");
                                serializer.setReceiver(handler);
                                serializer.toReceiver(proxy, false);
                                i = j;
                                break;
                            }
                        }
                        if (!isClosed) {
                            attribs = new AttrList();
                            if (nodes[i].qname.hasNamespace())
                                attribs.addAttribute(ATTR_NAMESPACE, nodes[i].qname.getNamespaceURI());
                            attribs.addAttribute(ATTR_NAME, nodes[i].qname.getStringValue());
                            handler.startElement(ELEMENT_START, attribs);
                            handler.endElement(ELEMENT_START);
                        }
                        break;
                    case XMLStreamReader.END_ELEMENT:
                        attribs = new AttrList();
                        if (nodes[i].qname.hasNamespace())
                            attribs.addAttribute(ATTR_NAMESPACE, nodes[i].qname.getNamespaceURI());
                        attribs.addAttribute(ATTR_NAME, nodes[i].qname.getStringValue());
                        handler.startElement(ELEMENT_END, attribs);
                        handler.endElement(ELEMENT_END);
                        break;
                    case XMLStreamReader.COMMENT:
                        attribs = new AttrList();
                        handler.startElement(ELEMENT_COMMENT, attribs);
                        handler.characters(nodes[i].value);
                        handler.endElement(ELEMENT_COMMENT);
                        break;
                    default:
                        final NodeProxy proxy = new NodeProxy(otherDoc, nodes[i].nodeId);
                        final Serializer serializer = broker.getSerializer();
                        serializer.reset();
                        serializer.setProperty(Serializer.GENERATE_DOC_EVENTS, "false");
                        serializer.setReceiver(handler);
                        serializer.toReceiver(proxy, false);
                        break;
                }
            }
        }
    }

    public final static class Append extends Insert {

        public Append(final NodeProxy reference, final DocumentImpl otherDoc) {
            super(APPEND, reference, otherDoc);
        }

        @Override
        public void serialize(final DBBroker broker, final Receiver handler) {
            try {
                final AttrList attribs = new AttrList();
                attribs.addAttribute(ATTR_REF, refChild.getNodeId().toString());
                handler.startElement(ELEMENT_APPEND, attribs);
                serializeChildren(broker, handler);
                handler.endElement(ELEMENT_APPEND);
            } catch (final SAXException e) {
                e.printStackTrace();
            }
        }
    }

    public final static class Delete extends Difference {

        protected final int event;

        public Delete(final NodeProxy reference) {
            this(-1, reference);
        }

        public Delete(final int event, final NodeProxy reference) {
            super(DELETE, reference);
            this.event = event;
        }

        @Override
        public void serialize(final DBBroker broker, final Receiver handler) {
            try {
                final AttrList attribs = new AttrList();
                if (event == XMLStreamReader.START_ELEMENT || event == XMLStreamReader.END_ELEMENT) {
                    String ev = event == XMLStreamReader.START_ELEMENT ? "start" : "end";
                    attribs.addAttribute(ATTR_EVENT, ev);
                }
                attribs.addAttribute(ATTR_REF, refChild.getNodeId().toString());
                handler.startElement(ELEMENT_DELETE, attribs);
                handler.endElement(ELEMENT_DELETE);
            } catch (final SAXException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
        }
    }
}

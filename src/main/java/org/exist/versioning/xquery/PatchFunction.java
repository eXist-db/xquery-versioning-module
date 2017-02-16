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
package org.exist.versioning.xquery;

import org.exist.dom.persistent.DocumentImpl;
import org.exist.dom.persistent.NodeProxy;
import org.exist.dom.QName;
import org.exist.dom.memtree.DocumentBuilderReceiver;
import org.exist.dom.memtree.InMemoryXMLStreamReader;
import org.exist.dom.memtree.MemTreeBuilder;
import org.exist.dom.memtree.NodeImpl;
import org.exist.numbering.NodeId;
import org.exist.stax.ExtendedXMLStreamReader;
import org.exist.versioning.DiffException;
import org.exist.versioning.Patch;
import org.exist.xquery.BasicFunction;
import org.exist.xquery.Cardinality;
import org.exist.xquery.FunctionSignature;
import org.exist.xquery.XPathException;
import org.exist.xquery.XQueryContext;
import org.exist.xquery.value.NodeValue;
import org.exist.xquery.value.Sequence;
import org.exist.xquery.value.SequenceType;
import org.exist.xquery.value.Type;

import javax.xml.stream.XMLStreamException;
import java.io.IOException;

public class PatchFunction extends BasicFunction {

    public final static FunctionSignature signatures[] = {
            new FunctionSignature(
                    new QName( "patch", VersioningModule.NAMESPACE_URI, VersioningModule.PREFIX ),
                    "Apply a patch to a document. The patch will be applied to the document of the node " +
                    "passed in first parameter. The second parameter should contain a version document as generated " +
                    "by eXist's VersioningTrigger. Note: though an arbitrary node can be passed in $a, the patch will " +
                    "always be applied to the entire document to which this node belongs.",
                    new SequenceType[] {
                            new SequenceType(Type.NODE, Cardinality.EXACTLY_ONE),
                            new SequenceType(Type.NODE, Cardinality.ZERO_OR_ONE)
                    },
                    new SequenceType( Type.ITEM, Cardinality.EXACTLY_ONE )
            ),
            new FunctionSignature(
                    new QName( "annotate", VersioningModule.NAMESPACE_URI, VersioningModule.PREFIX ),
                    "Apply a patch to a document. The patch will be applied to the document of the node " +
                            "passed in first parameter. The second parameter should contain a version document as generated " +
                            "by eXist's VersioningTrigger. Note: though an arbitrary node can be passed in $a, the patch will " +
                            "always be applied to the entire document to which this node belongs.",
                    new SequenceType[] {
                            new SequenceType(Type.NODE, Cardinality.EXACTLY_ONE),
                            new SequenceType(Type.NODE, Cardinality.ZERO_OR_ONE)
                    },
                    new SequenceType( Type.ITEM, Cardinality.EXACTLY_ONE )
            )
    };

    public PatchFunction(final XQueryContext context, final FunctionSignature signature) {
        super(context, signature);
    }

    @Override
    public Sequence eval(final Sequence[] args, final Sequence contextSequence) throws XPathException {
        context.pushDocumentContext();
        try {
            NodeValue nv = (NodeValue) args[0].itemAt(0);
            ExtendedXMLStreamReader reader = null;
            try {
                if (nv.getImplementationType() == NodeValue.IN_MEMORY_NODE) {
                    final NodeImpl node = (NodeImpl) nv;
                    reader = new InMemoryXMLStreamReader(node.getOwnerDocument(), node.getOwnerDocument());
                } else {
                    final NodeProxy proxy = (NodeProxy) nv;
                    reader = context.getBroker().newXMLStreamReader(new NodeProxy(proxy.getOwnerDocument(), NodeId.DOCUMENT_NODE, proxy.getOwnerDocument().getFirstChildAddress()), false);
                }

                nv = (NodeValue) args[1].itemAt(0);
                if (nv.getImplementationType() == NodeValue.IN_MEMORY_NODE) {
                    throw new XPathException("patch cannot be applied to in-memory documents");
                }
                final NodeProxy diffProxy = (NodeProxy) nv;
                final DocumentImpl diff = diffProxy.getOwnerDocument();

                final MemTreeBuilder builder = context.getDocumentBuilder();
                final DocumentBuilderReceiver receiver = new DocumentBuilderReceiver(builder);
                final Patch patch = new Patch(context.getBroker(), diff);
                if (isCalledAs("annotate")) {
                    patch.annotate(reader, receiver);
                } else {
                    patch.patch(reader, receiver);
                }
                final NodeValue result = (NodeValue) builder.getDocument().getDocumentElement();
                return result == null ? Sequence.EMPTY_SEQUENCE : result;
            } finally {
                if(reader != null) {
                    reader.close();
                }
            }
        } catch (IOException | XMLStreamException | DiffException e) {
            throw new XPathException(this, e.getMessage(), e);
        } finally {
            context.popDocumentContext();
        }
    }
}

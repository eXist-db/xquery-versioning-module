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

import org.exist.security.PermissionDeniedException;
import org.exist.security.xacml.AccessContext;
import org.exist.source.StringSource;
import org.exist.storage.BrokerPool;
import org.exist.storage.DBBroker;
import org.exist.storage.XQueryPool;
import org.exist.xmldb.XmldbURI;
import org.exist.xquery.CompiledXQuery;
import org.exist.xquery.XPathException;
import org.exist.xquery.XQuery;
import org.exist.xquery.XQueryContext;
import org.exist.xquery.value.IntegerValue;
import org.exist.xquery.value.Sequence;

import java.io.IOException;

public class VersioningHelper {

    private final static String GET_CURRENT_REV =
            "declare namespace v=\"http://exist-db.org/versioning\";\n" +
            "declare variable $collection external;\n" +
            "declare variable $document external;\n" +
            "max(" +
            "   for $r in collection($collection)//v:properties[v:document = $document]/v:revision\n" +
            "   return xs:long($r)" +
            ")";

    private final static StringSource GET_CURRENT_REV_SOURCE = new StringSource(GET_CURRENT_REV);

    private final static String GET_CONFLICTING_REV =
            "declare namespace v=\"http://exist-db.org/versioning\";\n" +
            "declare variable $collection external;\n" +
            "declare variable $document external;\n" +
            "declare variable $base external;\n" +
            "declare variable $key external;\n" +
            "collection($collection)//v:properties[v:document = $document]" +
            "   [v:revision > $base][v:key != $key]";

    private final static StringSource GET_CONFLICTING_REV_SOURCE = new StringSource(GET_CONFLICTING_REV);

    private final static String GET_BASE_REV_FOR_KEY =
            "declare namespace v=\"http://exist-db.org/versioning\";\n" +
            "declare variable $collection external;\n" +
            "declare variable $document external;\n" +
            "declare variable $base external;\n" +
            "declare variable $key external;\n" +
            "let $p := collection($collection)//v:properties[v:document = $document]\n" +
            "let $withKey := for $r in $p[v:revision > $base][v:key = $key] " +
            "                   order by $r/v:revision descending return $r\n" +
            "return\n" +
            "   if ($withKey) then\n" +
            "       xs:long($withKey[1]/v:revision)\n" +
            "   else\n" +
            "       xs:long($p[v:revision = $base]/v:revision)";
    
    private final static StringSource GET_BASE_REV_FOR_KEY_SOURCE = new StringSource(GET_BASE_REV_FOR_KEY);
    
    public static long getCurrentRevision(final DBBroker broker, final XmldbURI docPath)
            throws XPathException, IOException, PermissionDeniedException {
        final String docName = docPath.lastSegment().toString();
        final XmldbURI collectionPath = docPath.removeLastSegment();
        final XmldbURI path = VersioningTrigger.VERSIONS_COLLECTION.append(collectionPath);

        final BrokerPool brokerPool = broker.getBrokerPool();
        final XQuery xquery = brokerPool.getXQueryService();
        final XQueryPool xqueryPool = brokerPool.getXQueryPool();
        CompiledXQuery compiled = xqueryPool.borrowCompiledXQuery(broker, GET_CURRENT_REV_SOURCE);
        final XQueryContext context;
        if(compiled == null) {
            context = new XQueryContext(brokerPool, AccessContext.VALIDATION_INTERNAL);
        } else {
            context = compiled.getContext();
        }
        context.declareVariable("collection", path.toString());
        context.declareVariable("document", docName);
        if(compiled == null) {
            compiled = xquery.compile(broker, context, GET_CURRENT_REV_SOURCE);
        } else {
            compiled.getContext().updateContext(context);
            context.getWatchDog().reset();
        }

        try {
            final Sequence s = xquery.execute(broker, compiled, Sequence.EMPTY_SEQUENCE);
            if (s.isEmpty()) {
                return 0;
            }
            final IntegerValue iv = (IntegerValue) s.itemAt(0);
            return iv.getLong();
        } finally {
            xqueryPool.returnCompiledXQuery(GET_CURRENT_REV_SOURCE, compiled);
        }
    }

    public static boolean newerRevisionExists(final DBBroker broker, final XmldbURI docPath, final long baseRev,
            final String key) throws XPathException, IOException, PermissionDeniedException {
        final String docName = docPath.lastSegment().toString();
        final XmldbURI collectionPath = docPath.removeLastSegment();
        final XmldbURI path = VersioningTrigger.VERSIONS_COLLECTION.append(collectionPath);

        final BrokerPool brokerPool = broker.getBrokerPool();
        final XQuery xquery = brokerPool.getXQueryService();
        final XQueryPool xqueryPool = brokerPool.getXQueryPool();
        final XQueryContext context;
        CompiledXQuery compiled = xqueryPool.borrowCompiledXQuery(broker, GET_CONFLICTING_REV_SOURCE);
        if(compiled == null) {
            context = new XQueryContext(brokerPool, AccessContext.VALIDATION_INTERNAL);
        } else {
            context = compiled.getContext();
        }
        context.declareVariable("collection", path.toString());
        context.declareVariable("document", docName);
        context.declareVariable("base", new IntegerValue(baseRev));
        context.declareVariable("key", key);
        if(compiled == null) {
            compiled = xquery.compile(broker, context, GET_CONFLICTING_REV_SOURCE);
        } else {
            compiled.getContext().updateContext(context);
            context.getWatchDog().reset();
        }

        try {
            final Sequence s = xquery.execute(broker, compiled, Sequence.EMPTY_SEQUENCE);
            return !s.isEmpty();
        } finally {
            xqueryPool.returnCompiledXQuery(GET_CONFLICTING_REV_SOURCE, compiled);
        }
    }

    public static long getBaseRevision(final DBBroker broker, final XmldbURI docPath, final long baseRev,
        final String sessionKey) throws XPathException, IOException, PermissionDeniedException {
        final String docName = docPath.lastSegment().toString();
        final XmldbURI collectionPath = docPath.removeLastSegment();
        final XmldbURI path = VersioningTrigger.VERSIONS_COLLECTION.append(collectionPath);

        final BrokerPool brokerPool = broker.getBrokerPool();
        final XQuery xquery = brokerPool.getXQueryService();
        final XQueryPool xqueryPool = brokerPool.getXQueryPool();
        final XQueryContext context;
        CompiledXQuery compiled = xqueryPool.borrowCompiledXQuery(broker, GET_BASE_REV_FOR_KEY_SOURCE);
        if (compiled == null) {
            context = new XQueryContext(brokerPool, AccessContext.VALIDATION_INTERNAL);
        } else {
            context = compiled.getContext();
        }
        context.declareVariable("collection", path.toString());
        context.declareVariable("document", docName);
        context.declareVariable("base", new IntegerValue(baseRev));
        context.declareVariable("key", sessionKey);

        if (compiled == null) {
            compiled = xquery.compile(broker, context, GET_BASE_REV_FOR_KEY_SOURCE);
        } else {
            compiled.getContext().updateContext(context);
            context.getWatchDog().reset();
        }

        try {
            final Sequence s = xquery.execute(broker, compiled, Sequence.EMPTY_SEQUENCE);
            if (s.isEmpty()) {
                return 0;
            } else {
                final IntegerValue iv = (IntegerValue) s.itemAt(0);
                return iv.getLong();
            }
        } finally {
            xqueryPool.returnCompiledXQuery(GET_BASE_REV_FOR_KEY_SOURCE, compiled);
        }
    }
}

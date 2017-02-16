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

import org.exist.numbering.NodeId;
import org.exist.dom.QName;

public class DiffNode {

    public final static int UNCHANGED = 0;
    public final static int INSERTED = 1;
    public final static int APPENDED = 2;
    public final static int DELETED = 3;

    protected int status = UNCHANGED;

    protected final NodeId nodeId;
    protected final int nodeType;
    protected String value = null;
    protected QName qname = null;
    
    public DiffNode(final NodeId nodeId, final int nodeType, final String value) {
        this.nodeId = nodeId;
        this.nodeType = nodeType;
        this.value = value;
    }

    public DiffNode(final NodeId nodeId, final int nodeType, final QName qname) {
        this.nodeId = nodeId;
        this.nodeType = nodeType;
        this.qname = qname;
    }

    public void setStatus(final int status) {
        this.status = status;
    }

    @Override
    public boolean equals(final Object obj) {
        final DiffNode other = (DiffNode) obj;
        if (nodeType != other.nodeType) {
            return false;
        }
        if (qname != null) {
            return qname.equals(other.qname);
        } else {
            return value.equals(other.value);
        }
    }

    @Override
    public int hashCode() {
        if (qname == null) {
            return (value.hashCode() << 1) + nodeType;
        } else {
            return (qname.hashCode() << 1) + nodeType;
        }
    }

    @Override
    public String toString() {
        if (qname == null) {
            return nodeType + " " + nodeId.toString() + " " + value;
        } else {
            return nodeType + " " + nodeId.toString() + " " + qname;
        }
    }
}
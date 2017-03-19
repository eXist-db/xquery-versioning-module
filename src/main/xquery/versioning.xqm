xquery version "3.0";

(:
 : Versioning Module for eXist-db XQuery
 : Copyright (C) 2008 eXist-db <exit-open@lists.sourceforge.net>
 : http://exist-db.org
 :
 : This program is free software; you can redistribute it and/or modify
 : it under the terms of the GNU General Public License as published by
 : the Free Software Foundation; either version 1, or (at your option)
 : any later version.
 :
 : This program is distributed in the hope that it will be useful,
 : but WITHOUT ANY WARRANTY; without even the implied warranty of
 : MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 : GNU General Public License for more details.
 :
 : You should have received a copy of the GNU General Public License
 : along with this program; if not, write to the Free Software
 : Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
 :)
module namespace v = "http://exist-db.org/versioning"; 

import module namespace version = "http://exist-db.org/xquery/versioning"
    at "java:org.exist.versioning.xquery.VersioningModule";
import module namespace util = "http://exist-db.org/xquery/util";
import module namespace xmldb = "http://exist-db.org/xquery/xmldb";

declare variable $v:VERSIONS_COLLECTION := "/db/system/versions";

(:
	Return all revisions of the specified document 
	as a sequence of xs:integer revision numbers 
	in ascending order.

	@param $doc a node in the document for which revisions should be retrieved.
	@return a sequence of xs:integer revision numbers
:)
declare function v:revisions($doc as node()) as xs:integer* {
    let $collection := util:collection-name($doc)
    let $doc-name := util:document-name($doc)
    let $version-collection := concat($v:VERSIONS_COLLECTION, $collection)
    for $version in collection($version-collection)//v:properties[v:document = $doc-name]
    let $rev := xs:long($version/v:revision)
    order by $rev ascending
    return
        $rev
};

(:~
	Return all version docs, including the full diff, for the specified
	document. This is mainly for internal use.

	@param $doc a node in the document for which revisions should be retrieved.
	@return zero or more v:version elements describing the changes 
	made in a revision.
:)
declare function v:versions($doc as node()) as element(v:version)* {
    let $collection := util:collection-name($doc)
    let $doc-name := util:document-name($doc)
    let $version-collection := concat($v:VERSIONS_COLLECTION, $collection)
    for $version in collection($version-collection)/v:version[v:properties/v:document = $doc-name]
    order by xs:long($version/v:properties/v:revision) ascending
    return
        $version
};

(:~
	Restore a certain revision of a document by applying a
	sequence of diffs and return it as an in-memory node. If the
	revision argument is empty or smaller than the first actual
	revision of the document, the function will return the base
	version of the document. If the revision number is greater than
	the latest revision, the latest version will be returned.

	@param $doc a node in the document for which a revision should
	be retrieved.
	@param $rev the revision which should be restored
	@return a sequence of nodes corresponding to the restored document
	(TODO: return a document node instead?) 
:)
declare function v:doc($doc as node(), $rev as xs:integer?) as node()* {
    let $collection := util:collection-name($doc)
    let $doc-name := util:document-name($doc)
    let $version-collection := concat($v:VERSIONS_COLLECTION, $collection)
    let $base-name := $version-collection || "/" || $doc-name || ".base"
    return
        if (not(doc-available($base-name))) then
            ()
        else if (exists($rev)) then
            v:apply-patch(
                doc($base-name),
				for $version in
                	collection($version-collection)/v:version[v:properties[v:document = $doc-name][v:revision <= $rev]][v:diff]
					order by xs:long($version/v:properties/v:revision) ascending
				return
					$version
            )
		else
			doc($base-name)
};

(:~
	Apply a given patch on a document. This function is used by v:doc 
	internally.
:)
declare function v:apply-patch($doc as node(), $diffs as element(v:version)*) {
    if (empty($diffs)) then
        $doc
    else
        v:apply-patch(version:patch($doc, $diffs[1]), subsequence($diffs, 2))
};

(:~
	For the document passed as first argument, retrieve the revision
	specified in the second argument. Generate a diff between both version,
	i.e. HEAD and the given revision. The empty sequence is returned if the 
	given revision is invalid, i.e. v:doc returns the empty sequence.

	@param $doc a node in the document for which the diff should be generated
	@param $rev a valid revision number
:)
declare function v:diff($doc as node(), $rev as xs:integer) as element(v:version)? {
    let $base := v:doc($doc, $rev)
	return
		if (empty($base)) then
			()
		else
			let $col := xmldb:create-collection($v:VERSIONS_COLLECTION, "temp")
			let $stored := xmldb:store($col, util:document-name($doc), $base)
			let $diff := version:diff(doc($stored), $doc)
			let $deleted := xmldb:remove($col, util:document-name($doc))
			return
			     $diff
};

(:~
	Return an XML document in which all changes between
	$rev and $rev - 1 are annotated.

	@param $doc a node in the document which should be annotated
	@param $rev the revision whose changes will be annotated
:)
declare function v:annotate($doc as node(), $rev as xs:integer) {
    let $collection := util:collection-name($doc)
    let $doc-name := util:document-name($doc)
    let $version-collection := concat($v:VERSIONS_COLLECTION, $collection)
    let $revisions := v:revisions($doc)
    let $p := index-of($revisions, $rev)
	return
		if (empty($p)) then
			()
		else
			let $previous := 
				if ($p eq 1) then
					doc($version-collection || "/" || $doc-name || ".base")
				else
					v:doc($doc, $revisions[$p - 1])
			let $diff := collection($version-collection)/v:version[v:properties[v:document = $doc-name][v:revision = $rev]]
			return
				version:annotate($previous, $diff)
};

(:~
	Check if there are any revisions in the database which are newer than the
	version identified by the specified base revision and key. If versioning is
	active, the base revision and key are added to the document root element
	as attributes whenever a document is serialized. The combination of the two
	attributes allows eXist to determine if a newer revision of the document
	exists in the database, which usually means that another user/client has
	committed it in the meantime.

	If one or more newer revisions exist in the database, v:find-newer-revision will
	return the version document of the newest revision or an empty sequence
	otherwise.

	@param $doc a node in the document which should be checked
	@param $base the base revision as provided in the v:revision
	attribute of the document which was retrieved from the db.
	@param $key the key as provided in the v:key attribute of the document
	which was retrieved from the db.
	@return a v:version element or the empty sequence if there's no newer revision
	in the database
:)
declare function v:find-newer-revision($doc as node(), $base as xs:integer, $key as xs:string) as element(v:version)? {
    let $collection := util:collection-name($doc)
    let $doc-name := util:document-name($doc)
    let $version-collection := concat($v:VERSIONS_COLLECTION, $collection)
	let $newer := 
		for $v in collection($version-collection)/v:version[v:properties[v:document = $doc-name][v:revision > $base][v:key != $key]]
    	order by xs:long($v/v:properties/v:revision) descending
		return $v
	return $newer[1]
};

(:~
	Returns an XML fragment showing the version history of the 
	document to which the specified node belongs. All revisions
	are listed with date and user, but without the detailed diff.

	@param $doc an arbitrary node in a document
:)
declare function v:history($doc as node()) as element(v:history) {
    let $collection := util:collection-name($doc)
    let $doc-name := util:document-name($doc)
    let $version-collection := concat($v:VERSIONS_COLLECTION, $collection)
	return
		<v:history>
			<v:document>{base-uri($doc)}</v:document>
			<v:revisions>
			{
				for $v in collection($version-collection)//v:properties[v:document = $doc-name]
				order by xs:long($v/v:revision) ascending
				return
					<v:revision rev="{$v/v:revision}">
					{ $v/v:date, $v/v:user}
					</v:revision>
			}
			</v:revisions>
		</v:history>
};

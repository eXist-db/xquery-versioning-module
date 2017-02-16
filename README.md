# Versioning Module for eXist-db XQuery #
[![Java 8](https://img.shields.io/badge/java-8-blue.svg)](http://java.oracle.com) [![License](https://img.shields.io/badge/license-GPL%201.0-blue.svg)](https://www.gnu.org/licenses/gpl-1.0.html)

This repository holds the for the eXist-db XQuery Versioning extension module.

### Building a EXPath Package for eXist-db ###

Building requires that you have - Git, Java JDK version 8 and Maven 3 installed.

```bash
$ git clone https://www.github.com/exist-db/xquery-versioning-module.git
$ mvn package
```

You will then find a file named similar to `target/xquery-versioning-module-1.0.xar`.

### Example `collection.xconf` Trigger Configuration
```xml
<collection xmlns="http://exist-db.org/collection-config/1.0">
    <triggers>
        <trigger event="create,update,delete"
            class="org.exist.versioning.VersioningTrigger">
            <parameter name="overwrite" value="no"/>
        </trigger>
    </triggers>
</collection>
```


### API Overview

Namespace URI: `http://exist-db.org/xquery/versioning`

Namespace Prefix: `versioning`

Class: `org.exist.versioning.xquery.VersioningModule`


1. To find the differences between two nodes:
    ```xquery
    versioning:diff($a as node(), $b as node()) as node()
    ```

2. To apply a patch to a node:
    ```xquery
    versioning:patch($node as node(), $patch as node()) as item()
    ```

3. To annotate a node:
    ```xquery
    versioning:annotate($node as node(), $patch as node()) as item()
    ```


### Utility API Overview

Namespace URI: `http://exist-db.org/versioning`

Namespace Prefix: `v`

file: `versioning.xqm`

1. Return all revisions of the specified document as a sequence of xs:integer revision numbers in ascending order:
    ```xquery
    v:revisions($doc as node()) as xs:integer*
    ```

2. Return all version docs, including the full diff, for the specified document:
    ```xquery
    v:versions($doc as node()) as element(v:version)*
    ```

3. Restore a certain revision of a document by applying a sequence of diffs and return it as an in-memory node:
    ```xquery
    v:doc($doc as node(), $rev as xs:integer?) as node()*
    ```

4. Apply a given patch on a document:
    ```xquery
    v:apply-patch($doc as node(), $diffs as element(v:version)*)
    ```

5. For the document passed as first argument, retrieve the revision specified in the second argument. Generate a diff between both version, i.e. **HEAD** and the given revision:
    ```xquery
    v:diff($doc as node(), $rev as xs:integer) as element(v:version)?
    ```

6. Return an XML document in which all changes between ``$rev` and ``$rev - 1` are annotated.
    ```xquery
    v:annotate($doc as node(), $rev as xs:integer)
    ```

7. Check if there are any revisions in the database which are newer than the version identified by the specified base revision and key:
    ```xquery
    v:find-newer-revision($doc as node(), $base as xs:integer, $key as xs:string) as element(v:version)?
    ```

8. Returns an XML fragment showing the version history of the  document to which the specified node belongs. All revisions are listed with date and user, but without the detailed diff:
    ```xquery
    v:history($doc as node()) as element(v:history)
    ```

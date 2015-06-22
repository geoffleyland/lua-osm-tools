# lua-osm-tools - Tools for handling OSM data

My set of tools for dealing with OSM data and converting it to routable
road networks.  The tools include:

  - osmdb: a format for storing OSM data pretty much verbatim in a database.
    xml and pbf are a pretty horrible formats to work with so the first thing
    I do tends to be read it all into a sqlite db.

  - readosm-pbf: a library for reading osm pbf files (not xml).
    Because it gives LuaJIT a few more opportunities to compile traces it ends
    up quicker than [my binding](https://github.com/geoffleyland/lua-readosm)
    to [readosm](https://www.gaia-gis.it/fossil/readosm/index),
    even though readosm is quicker if you're using it from C.

  - osm-tools: a tool with subcommands to:

      - read-verbatim: dump a pbf file to a database.


# osmdb

osmdb is a simple db format for storing osm data.
I'm sure this must exist, right?
Anyway, it's not a geographic database or anything like that - it's just
a fairly literal copy of the data.
It's not really intended for you to add to the database once you've read it -
it's more of a import-the-whole-map-once-then-read-it kind of database.

Tables are:

  - elements - just an element id and type ("node", "way", or "relation")
  - info - element metadata: version, changeset, user, uid and timestamp.
    This is in a separate table from elements so it's easy to choose to not
    store metadata.
  - tags: tags for every element: a parent id, a key and a value.
  - locations: a latitude and longitude for node elements
  - nodes: ordered lists of node ids for way elements
  - members: ordered lists of members (role and id) for relations.

`osmdb.create(dbname)` creates a new database and its tables.

`osmdb.open(dbname)` opens and existing database.

`db:begin()` begins a transaction.  It's a good idea to `begin` before
importing a lot of elements into the database.

`db:commit()` commits a transaction, builds indexes and vaccuums the database.
(This probably only works on SQLite).

`db:insert_element(type, object)` inserts an element into the database.
The type and object format are exactly what you get from either of the
osm readers I've built, so you can just go:

    for type, element in f:lines() do
      db:insert_element(type, element)
    end

to read a whole file into the database.  This is what `osm-tools import` does.

`db:elements(type, with_children)` is an iterator that returns all the
elements in the database.
You can use `type` to select a single type of element,
for example to only read nodes.
If `with_children` is true or the string `"with_children"` then:

  - a way will contain a `nodes` table containing a list of node objects.
  - a relation will contain a `members` table containing
    `{ role="role", member=<object> }`
    elements, where the member is element objects.

Otherwise:

   - a way will contain a `node_refs` table, with a list of node ids.
  - a relation will contain a `member_refs` table containing
    `{role="role", id=id }` elements.

This format is intended to be exactly the same as what you get from the OSM
readers.

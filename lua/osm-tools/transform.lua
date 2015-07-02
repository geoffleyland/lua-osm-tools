local ok, lro = pcall(require, "osm-tools.read-osmpbf")
lro = ok and lro or nil
local ok, cro = pcall(require, "readosm")
cro = ok and cro or nil
local osmdb = require"osm-tools.osmdb"


------------------------------------------------------------------------------

local db, count, start, transformer
local wanted_node_ids

local function count_one()
  count = count + 1
  if count % 100000 == 0 then
    local elapsed = os.clock() - start
    io.stderr:write(("Read %5dk elements in %4ds (%3dk elements/s, %3.1fMB)...   \r"):
      format(count / 1000, elapsed, count / elapsed / 1000, collectgarbage('count') / 1024))
  end
end


local function read_way(type, element)
  element = transformer(type, element)
  if element then
    for _, n in ipairs(element.node_refs) do
      wanted_node_ids[tonumber(n)] = true
    end
    db:insert_element(type, element)
  end
  count_one()
end


local function read_node(type, element)
  if wanted_node_ids[tonumber(element.id)] then
    element = transformer(type, element) or element
    db:insert_element(type, element)
  end
  count_one()
end


local function read_relation(type, element)
  local element = transformer(type, element)
  if element then
    db:insert_element(type, element)
  end
  count_one()
end


local function read_osm(osmname, db_or_dbname)
  start = os.clock()

  if type(db_or_dbname) == "string" then
    db = osmdb.create(db_or_dbname)
  else
    db = db_or_dbname
  end
  assert(db, "Couldn't open db.")
  db:begin()

  wanted_node_ids = {}
  count = 0

  local ro = (function()
      if osmname:match(".pbf$") then
        return assert(lro or cro, "You need to install read-osmpbf or lua-readosm to read PBF OSM files")
      else
        return assert(cro, "You need to install lua-readosm to read XML-format OSM files")
      end
    end)()

  local f = ro.open(osmname, "ways")

  if f.lines then
    for t, e in f:lines() do read_way(t, e) end
    f:close()

    f = ro.open(osmname, "nodes")
    for t, e in f:lines() do read_node(t, e) end
    f:close()

    f = ro.open(osmname, "relations")
    for t, e in f:lines() do read_relation(t, e) end
    f:close()

  else
    f:parse(read_way)
    f:close()

    f = ro.open(osmname, "nodes")
    f:parse(read_node)
    f:close()

    f = ro.open(osmname, "relations")
    f:parse(read_relation)
    f:close()
  end

  local elapsed = os.clock() - start
  io.stderr:write(("Read %5dk elements in %4ds (%3dk elements/s)...             \n"):
    format(count / 1000, elapsed, count / elapsed / 1000))

  db:commit()
end


local function transform(osm_file, db_file, transform_in)
  io.stderr:write(("Converting %s -> %s\n"):format(osm_file, db_file))
  transformer = transform_in
  db = osmdb.create(db_file)
  read_osm(osm_file, db)
  return db
end


------------------------------------------------------------------------------

return { transform = transform }

------------------------------------------------------------------------------

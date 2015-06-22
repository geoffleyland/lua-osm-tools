local ok, lro = pcall(require, "read-osmpbf")
lro = ok and lro or nil
local ok, cro = pcall(require, "readosm")
cro = ok and cro or nil
local osmdb = require"osmdb"


------------------------------------------------------------------------------

local db, count, start

local function count_one()
  count = count + 1
  if count % 10000 == 0 then
    local elapsed = os.clock() - start
    io.stderr:write(("Read %5dk elements in %4ds (%3dk elements/s, %3.1fMB)...   \r"):
      format(count / 1000, elapsed, count / elapsed / 1000, collectgarbage('count') / 1024))
  end
end


local function read_element(type, element)
  db:insert_element(type, element)
  count_one()
end


local function import(osmname, db_or_dbname)
  if type(db_or_dbname) == "string" then
    db = osmdb.create(db_or_dbname)
  else
    db = db_or_dbname
  end
  assert(db, "Couldn't open db.")
  db:begin()

  local ro = (function()
      if osmname:match(".xml$") then
        return assert(cro, "You need to install lua-readosm to read XML-format OSM files")
      else
        return assert(lro or cro, "You need to install read-osmpbf or lua-readosm to read PBF OSM files")
      end
    end)()

  local f = ro.open(osmname)
  count = 0
  start = os.clock()

  if f.lines then
    for type, element in f:lines() do
      read_element(type, element)
    end
  else
    f:parse(read_element)
  end

  f:close()
  local elapsed = os.clock() - start
  io.stderr:write(("Read %5dk elements in %4ds (%3dk elements/s).              \n"):
    format(count / 1000, elapsed, count / elapsed /1000))

  io.stderr:write("Building indexes...\n")
  db:commit()
  io.stderr:write(("Finished in %4ds.\n"):
    format(os.clock() - start))

  return db
end


------------------------------------------------------------------------------

return { import = import }

------------------------------------------------------------------------------

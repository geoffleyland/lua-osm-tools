local zlib = require"zlib"
local pb = require"pb"
local osmpbf = require"osm-tools.protocol-buffer-defs.osm"


------------------------------------------------------------------------------
-- Read a file header

local acceptable_features =
{
  ["OsmSchema-V0.6"] = true,
  ["DenseNodes"] = true,
}

local function read_header(data)
  local header = osmpbf.HeaderBlock()
  header:Parse(data)
  for _, v in ipairs(header.required_features) do
    if not acceptable_features[v] then
      error(("osmpbfread: unsupported feature '%s'"):format(v))
    end
  end
end


------------------------------------------------------------------------------
-- For what it's worth, we try to do as little as possible when reading a file
-- so that we don't spend too much time pulling data the user doesn't want
-- into lua-land.
-- So instead of populating a table with data fields, we give a table a
-- a metatable that will go and fetch the data when it's asked for.
-- It actually seems to work (if you're not looking at all the data),
-- and speeds things up a little without costing much in the worst case.

local function id_field(msg) return msg.id end
local function version_field(msg) return msg.info and msg.info.version or nil end
local function changeset_field(msg) return msg.info and msg.info.changeset or nil end
local function uid_field(msg) return msg.info and msg.info.uid or nil end
local function user_field(msg, block)
  return msg.info and msg.info.user_sid and block.stringtable.s[msg.info.user_sid+1] or nil
end
-- We copy readosm and return a timestamp string.
-- This avoids complications with UTC and local time.
local function timestamp_field(msg)
  return msg.info and os.date("!%Y-%m-%dT%H:%M:%SZ", msg.info.timestamp) or nil
end


local NO_TAGS = setmetatable({}, { __newindex = function() error("read only table") end })

local function tag_field(msg, block)
  if not msg.keys then return NO_TAGS end

  local tags = {}
  for i, k in pairs(msg.keys) do
    tags[block.stringtable.s[k+1]] = block.stringtable.s[msg.vals[i]+1]
  end
  return tags
end


local node_fields =
{
  id          = id_field,
  version     = version_field,
  timestamp   = timestamp_field,
  changeset   = changeset_field,
  uid         = uid_field,
  user        = user_field,
  latitude = function(msg, block)
      return (block.lat_offset + msg.lat * block.granularity) * 1e-9
    end,
  longitude = function(msg, block)
      return (block.lon_offset + msg.lon * block.granularity) * 1e-9
    end,
}

local way_fields =
{
  id          = id_field,
  tags        = tag_field,
  version     = version_field,
  timestamp   = timestamp_field,
  changeset   = changeset_field,
  uid         = uid_field,
  user        = user_field,
  node_refs = function(msg)
      local ids = {}
      local id = 0
      for i, v in ipairs(msg.refs) do
        id = id + v
        ids[i] = id
      end
      return ids
    end,
}

local relation_fields =
{
  id          = id_field,
  tags        = tag_field,
  version     = version_field,
  timestamp   = timestamp_field,
  changeset   = changeset_field,
  uid         = uid_field,
  user        = user_field,
  member_refs = function(msg, block)
      local m = {}
      local id = 0
      for i, v in ipairs(msg.memids) do
        id = id + v
        m[i] =
        {
          id    = id,
          role  = block.stringtable.s[msg.roles_sid[i]+1],
          member_type  = msg.types[i]:lower()
        }
      end
      return m
    end,
}

local MESSAGE_KEY, BLOCK_KEY = {}, {}

local function make_metatable(fields)
  return
  {
    __index = function (o, k)
        local f = fields[k]
        local v = f and f(o[MESSAGE_KEY], o[BLOCK_KEY], o) or nil
        o[k] = v
        return v
      end
  }
end


local node_mt           = make_metatable(node_fields)
local way_mt            = make_metatable(way_fields)
local relation_mt       = make_metatable(relation_fields)


------------------------------------------------------------------------------

local function no_tags() return NO_TAGS end

local function some_tags(t, _, o)
  local tags = {}
  while true do
    local si = t.indexes[t.string_index]
    if not si or si == 0 then break end
    tags[t.strings[si+1]] = t.strings[t.indexes[t.string_index+1]+1]
    t.string_index = t.string_index + 2
  end
  return tags
end

local dense_mt_no_tags = make_metatable{ tags = no_tags }
local dense_mt_tags = make_metatable{ tags = some_tags }

local function read_dense_nodes(block, elements, config)
  local id = 0
  local lat, lon = block.lat_offset * 1e-9, block.lon_offset * 1e-9
  local dense = elements.dense
  local granularity = block.granularity * 1e-9
  local date_granularity = block.date_granularity * 1e-3
  local keys_vals = dense.keys_vals
  local next_string_index = 1

  local tag_info = keys_vals and { indexes = keys_vals, strings = block.stringtable.s } or nil
  local dense_mt = keys_vals and dense_mt_tags or dense_mt_no_tags

  -- There are two copies of this code, one with, and one without node info.
  -- Handling info is slow for a number of reasons:
  -- - it seems like initialising a larger table takes longer (possibly
  --   because the table is more than 4 entries)
  -- - getting the user string takes a while
  -- - the delta encoding
  -- - info is optional anyway, so if we didn't have two loops, we'd have to
  --   check for it every time through the loop (and the JIT might not be able
  --   to remove the if?)
  -- So we ask the user if they want info, and we run a different loop
  -- if they do or if they don't or there's no info anyway.
  if not (config.info and dense.denseinfo) then
    for i, v in ipairs(dense.id) do
      id = id + v
      lat = lat + dense.lat[i]
      lon = lon + dense.lon[i]
      if keys_vals then
        tag_info.string_index = next_string_index
        while true do
          local si = keys_vals[next_string_index]
          if not si or si == 0 then break end
          next_string_index = next_string_index + 2
        end
        next_string_index = next_string_index + 1
      end
      local n =
      {
        id = id,
        latitude = lat * granularity,
        longitude = lon * granularity,
        [MESSAGE_KEY] = tag_info,
      }
      coroutine.yield("node", setmetatable(n, dense_mt))
    end
  else
    local info = dense.denseinfo
    local timestamp, changeset, uid, user_sid = 0, 0, 0, 0
    for i, v in ipairs(dense.id) do
      id = id + v
      lat = lat + dense.lat[i]
      lon = lon + dense.lon[i]
      if keys_vals then
        tag_info.string_index = next_string_index
        while true do
          local si = keys_vals[next_string_index]
          if not si or si == 0 then break end
          next_string_index = next_string_index + 2
        end
        next_string_index = next_string_index + 1
      end
      timestamp = timestamp + info.timestamp[i]
      changeset = changeset + info.changeset[i]
      uid = uid + info.uid[i]
      user_sid = user_sid + info.user_sid[i]
      local n =
      {
        id = id,
        latitude = lat * granularity,
        longitude = lon * granularity,
        version = info and info.version[i],
        timestamp = os.date("!%Y-%m-%dT%H:%M:%SZ", timestamp * date_granularity),
        changeset = changeset,
        uid = uid,
        user = block.stringtable.s[user_sid+1],
        [MESSAGE_KEY] = tag_info,
      }
      coroutine.yield("node", setmetatable(n, dense_mt))
    end
  end
end


local function make_element(mt, msg, block)
  return setmetatable({ [MESSAGE_KEY] = msg, [BLOCK_KEY] = block }, mt)
end

local function read_elements(data, config)
  local block = osmpbf.PrimitiveBlock()
  block:Parse(data)

  if block.primitivegroup then for _, elements in ipairs(block.primitivegroup) do
    if elements.nodes and config.nodes then for _, n in ipairs(elements.nodes) do
      coroutine.yield("node", make_element(node_mt, n, block))
    end end
    if elements.ways and config.ways then for _, w in ipairs(elements.ways) do
      coroutine.yield("way", make_element(way_mt, w, block))
    end end
    if elements.relations and config.relations then for _, r in ipairs(elements.relations) do
      coroutine.yield("relation", make_element(relation_mt, r, block))
    end end

    if elements.dense and config.nodes then
      read_dense_nodes(block, elements, config)
    end

  end end
end


------------------------------------------------------------------------------

local readers =
{
  OSMHeader = read_header,
  OSMData = read_elements,
}


local function read_blob(fin, config)
  local length_data = fin:read(4)
  if not length_data then return end

  local blob_length = 0
  for i = 1, 4 do
    blob_length = blob_length * 2^8 + length_data:byte(i)
  end

  local header = osmpbf.BlobHeader()
  header:Parse(fin:read(blob_length))

  local reader = readers[header.type]
  if not reader then
    error(("osmpbfread: unknown datatype '%s'"):format(header.type))
  end

  local blob = osmpbf.Blob()
  blob:Parse(fin:read(header.datasize))

  local blob_data = blob.raw and blob.raw or zlib.inflate()(blob.zlib_data)
  reader(blob_data, config)

  return true
end


------------------------------------------------------------------------------

local osmpbf_file_mt =
{
  __index =
  {
    lines = function(t)
        return coroutine.wrap(function() while read_blob(t.file, t.config) do end end)
      end,
    close = function(t)
        t.file:close()
      end
  }
}


local no_config =
{
  nodes         = true,
  ways          = true,
  relations     = true,
  info          = true,
}


local function open(filename, what)
  what = what and what:lower()
  config = not what and no_config or
  {
    nodes       = what:find("node"),
    ways        = what:find("way"),
    relations   = what:find("relation"),
    info        = what:find("info"),
  }
  local file = assert(io.open(filename, "r"))
  return setmetatable({ file = file, config = config }, osmpbf_file_mt)
end


------------------------------------------------------------------------------

return { open = open }

------------------------------------------------------------------------------

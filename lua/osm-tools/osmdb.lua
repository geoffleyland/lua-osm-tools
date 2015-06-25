local patengi = require"patengi"

------------------------------------------------------------------------------
-- IDs aren't unique across nodes, types and ways.
-- It's tempting to wonder why on earth they did that, but let's not dwell on
-- it.
-- Let's just create a unique ID that's 4*osm_id + type_code.
-- We'll call in an "eid" for "element id".  My first idea "uid" (for
-- "unique_id") is already taken by the user_id field

local type_code =
{
  node          = 0,
  way           = 1,
  relation      = 2,
}

local NODE_CODE = type_code.node


------------------------------------------------------------------------------

local osmdb = {}
osmdb.__index = osmdb


local function build_queries(db)
  return
  {
    insert_element = db:prepare[[
      INSERT INTO elements
        (eid, type)
      VALUES
        (:eid, :type);
      ]],
    insert_info = db:prepare[[
      INSERT INTO info
        (eid, version, changeset, user, uid, timestamp)
      VALUES
        (:eid, :version, :changeset, :user, :uid, :timestamp);
      ]],
    insert_location = db:prepare[[
      INSERT INTO locations
        (eid, latitude, longitude)
      VALUES
        (:eid, :latitude, :longitude);
      ]],
    insert_node = db:prepare[[
      INSERT INTO nodes
        (parent_eid, ord, node_eid)
      VALUES
        (:parent_eid, :ord, :node_eid);
      ]],
    insert_member = db:prepare[[
      INSERT INTO members
        (parent_eid, ord, role, member_eid)
      VALUES
        (:parent_eid, :ord, :role, :member_eid);
      ]],
    insert_tag = db:prepare[[
      INSERT INTO tags
        (parent_eid, key, value)
      VALUES
        (:parent_eid, :key, :value);
      ]],

    select_all_elements = db:prepare[[
      SELECT eid, type
      FROM elements;
      ]],
    select_some_elements = db:prepare[[
      SELECT eid, type
      FROM elements
      WHERE type = :1;
      ]],
    select_element = db:prepare[[
      SELECT eid, type
      FROM elements
      WHERE eid = :eid;
      ]],
    select_info = db:prepare[[
      SELECT version, changeset, user, uid, timestamp
      FROM info
      WHERE eid = :eid;
      ]],
    select_location = db:prepare[[
      SELECT latitude, longitude
      FROM locations
      WHERE eid = :eid;
      ]],
    select_tags = db:prepare[[
      SELECT key, value
      FROM tags
      WHERE parent_eid = :parent_eid;
      ]],
    select_nodes = db:prepare[[
      SELECT node_eid
      FROM nodes
      WHERE parent_eid = :parent_eid
      ORDER BY ord;
      ]],
    select_members = db:prepare[[
      SELECT role, type, member_eid
      FROM members
      WHERE parent_eid = :parent_eid
      ORDER BY ord;
      ]],
  }
end


function osmdb:new(db)
  return setmetatable({ db = db, Q = build_queries(db), values = {} }, osmdb)
end


function osmdb:close()
  self:commit()
  self.db:close()
end


------------------------------------------------------------------------------

function osmdb.create(filename)
  os.remove(filename)
  local db = patengi.open(filename)

  db:exec[[
  CREATE TABLE elements
  (
    eid INTEGER PRIMARY KEY,
    type VARCHAR
  );]]
  db:exec[[
  CREATE TABLE info
  (
    eid INTEGER PRIMARY KEY,
    version INTEGER,
    changeset INTEGER,
    user VARCHAR,
    uid INTEGER,
    timestamp DATETIME
  );]]
  db:exec[[
  CREATE TABLE tags
  (
    parent_eid INTEGER,
    key VARCHAR,
    value VARCHAR
  );]]
  db:exec[[
  CREATE TABLE locations
  (
    eid INTEGER PRIMARY KEY,
    latitude REAL,
    longitude REAL
  );]]
  db:exec[[
  CREATE TABLE nodes
  (
    parent_eid INTEGER,
    ord INTEGER,
    node_eid INTEGER
  );]]
  db:exec[[
  CREATE TABLE members
  (
    parent_eid INTEGER,
    ord INTEGER,
    role VARCHAR,
    type VARCHAR,
    member_eid INTEGER
  );]]

  return osmdb:new(db)
end


function osmdb.open(filename)
  return osmdb:new(patengi.open(filename))
end


------------------------------------------------------------------------------


function osmdb:begin()
  self.db:exec("BEGIN;")
end


function osmdb:commit()
  self.db:exec("COMMIT;")
  self.db:exec("CREATE INDEX ni ON nodes (parent_eid, ord);")
  self.db:exec("CREATE INDEX mi ON members (parent_eid, ord);")
  self.db:exec("CREATE INDEX ti ON tags (parent_eid);")
  self.db:exec("CREATE INDEX tki ON tags (key);")
  self.db:exec("VACUUM;")
  self.db:exec("ANALYZE;")
end


function osmdb:_insert_element(o, type)
  local values = self.values
  values.eid = tonumber(o.id) * 4 + type_code[type]
  values.type = type
  self.Q.insert_element:exec(values)

  values.parent_eid = values.eid
  for k, v in pairs(o.tags) do
    values.key = k
    values.value = v
    self.Q.insert_tag:exec(values)
  end

  if o.version or o.changeset or o.user or o.uid or o.timestamp then
    values.version = tonumber(o.version)
    values.changeset = tonumber(o.changeset)
    values.user = o.user
    values.uid = tonumber(o.uid)
    values.timestamp = o.timestamp and o.timestamp
    self.Q.insert_info:exec(values)
  end

  return values.eid
end


function osmdb:insert_node(o)
  self:_insert_element(o, "node")

  local values = self.values
  values.latitude = tonumber(o.latitude)
  values.longitude = tonumber(o.longitude)
  self.Q.insert_location:exec(values)
end


function osmdb:insert_way(o)
  local values = self.values
  values.parent_eid = self:_insert_element(o, "way")

  for i, n in ipairs(o.node_refs) do
    values.ord = i
    values.node_eid = tonumber(n) * 4 + NODE_CODE
    self.Q.insert_node:exec(values)
  end
end


function osmdb:insert_relation(o)
  local values = self.values
  values.parent_eid = self:_insert_element(o, "relation")

  for i, n in ipairs(o.member_refs) do
    values.ord = i
    values.role = n.role
    values.type = n.member_type
    values.member_eid = tonumber(n.id) * 4 + type_code[n.member_type]
    self.Q.insert_member:exec(values)
  end
end


local element_inserters =
{
  node          = osmdb.insert_node,
  way           = osmdb.insert_way,
  relation      = osmdb.insert_relation,
}

function osmdb:insert_element(type, o)
  local f = element_inserters[type]
  if f then
    f(self, o)
  else
    io.stderr:write(("Unknown data type '%s'\n"):format(type))
  end
end


------------------------------------------------------------------------------

function osmdb:read_element(element, with_children)
  with_children = with_children == "with_children" or with_children == true

  element.id = math.floor(element.eid / 4)

  local version, changeset, user, uid, timestamp =
    self.Q.select_info:uexec(element.eid)
  if version then
    element.version = version
    element.changeset = changeset
    element.user = user
    element.uid = uid
    element.timestamp = timestamp
  end

  element.tags = {}
  for key, value in self.Q.select_tags:urows(element.eid) do
    element.tags[key] = value
  end

  if element.type == "node" then
    element.latitude, element.longitude =
      self.Q.select_location:uexec(element.eid)
  elseif element.type == "way" then
    if with_children then
      element.nodes = {}
      for node_eid in self.Q.select_nodes:urows(element.eid) do
        local n = self.Q.select_element:nexec(node_eid, "element")
        element.nodes[#element.nodes+1] = self:read_element(n)
      end
    else
      element.node_refs = {}
      for node_eid in self.Q.select_nodes:urows(element.eid) do
        element.node_refs[#element.node_refs+1] = math.floor(node_eid / 4)
      end
    end
  elseif element.type == "relation" then
    if with_children then
      element.members = {}
      for role, type, member_eid in self.Q.select_members:urows(element.eid) do
        local m = self.Q.select_element:nexec(member_eid)
        if m then
          element.members[#element.members+1] =
            { role = role, type = type, member = self:read_element(m) }
        end
      end
    else
      element.member_refs = {}
      for role, type, member_eid in self.Q.select_members:urows(element.eid) do
        element.member_refs[#element.member_refs+1] =
          { role = role, type = type, id = math.floor(member_eid / 4), eid = member_eid }
      end
    end
  end
  return element
end


local function get_iterator(self, type)
  if not type or type == "" then
    return self.Q.select_all_elements:nrows()
  else
    return self.Q.select_some_elements:nrows(type)
  end
end


function osmdb:elements(type, with_children)
  return coroutine.wrap(function()
      for element in get_iterator(self, type) do
        self:read_element(element, with_children)
        coroutine.yield(element)
      end
    end)
end


------------------------------------------------------------------------------

return osmdb

------------------------------------------------------------------------------

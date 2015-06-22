local patengi = require"patengi"

------------------------------------------------------------------------------

local osmdb = {}
osmdb.__index = osmdb


local function build_queries(db)
  return
  {
    insert_element = db:prepare[[
      INSERT INTO elements
        (id, type)
      VALUES
        (:id, :type);
      ]],
    insert_info = db:prepare[[
      INSERT INTO info
        (id, version, changeset, user, uid, timestamp)
      VALUES
        (:id, :version, :changeset, :user, :uid, :timestamp);
      ]],
    insert_location = db:prepare[[
      INSERT INTO locations
        (id, latitude, longitude)
      VALUES
        (:id, :latitude, :longitude);
      ]],
    insert_node = db:prepare[[
      INSERT INTO nodes
        (parent_id, ord, node_id)
      VALUES
        (:parent_id, :ord, :node_id);
      ]],
    insert_member = db:prepare[[
      INSERT INTO members
        (parent_id, ord, role, member_id)
      VALUES
        (:parent_id, :ord, :role, :member_id);
      ]],
    insert_tag = db:prepare[[
      INSERT INTO tags
        (parent_id, key, value)
      VALUES
        (:parent_id, :key, :value);
      ]],

    select_all_elements = db:prepare[[
      SELECT id, type
      FROM elements;
      ]],
    select_some_elements = db:prepare[[
      SELECT id, type
      FROM elements
      WHERE type = :1;
      ]],
    select_element = db:prepare[[
      SELECT id, type
      FROM elements
      WHERE id = :id;
      ]],
    select_info = db:prepare[[
      SELECT version, changeset, user, uid, timestamp
      FROM info
      WHERE id = :id;
      ]],
    select_location = db:prepare[[
      SELECT latitude, longitude
      FROM locations
      WHERE id = :id;
      ]],
    select_tags = db:prepare[[
      SELECT key, value
      FROM tags
      WHERE parent_id = :parent_id;
      ]],
    select_nodes = db:prepare[[
      SELECT node_id
      FROM nodes
      WHERE parent_id = :parent_id
      ORDER BY ord;
      ]],
    select_members = db:prepare[[
      SELECT role, member_id
      FROM members
      WHERE parent_id = :parent_id
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
    id INTEGER PRIMARY KEY,
    type VARCHAR
  );]]
  db:exec[[
  CREATE TABLE info
  (
    id INTEGER PRIMARY KEY,
    version INTEGER,
    changeset INTEGER,
    user VARCHAR,
    uid INTEGER,
    timestamp DATETIME
  );]]
  db:exec[[
  CREATE TABLE tags
  (
    parent_id INTEGER,
    key VARCHAR,
    value VARCHAR
  );]]
  db:exec[[
  CREATE TABLE locations
  (
    id INTEGER PRIMARY KEY,
    latitude REAL,
    longitude REAL
  );]]
  db:exec[[
  CREATE TABLE nodes
  (
    parent_id INTEGER,
    ord INTEGER,
    node_id INTEGER
  );]]
  db:exec[[
  CREATE TABLE members
  (
    parent_id INTEGER,
    ord INTEGER,
    role VARCHAR,
    member_id INTEGER
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
  self.db:exec("CREATE INDEX ni ON nodes (parent_id, ord);")
  self.db:exec("CREATE INDEX mi ON members (parent_id, ord);")
  self.db:exec("CREATE INDEX ti ON tags (parent_id);")
  self.db:exec("CREATE INDEX tki ON tags (key);")
  self.db:exec("VACUUM;")
  self.db:exec("ANALYZE;")
end


function osmdb:insert_tags(o)
  local values = self.values
  values.parent_id = tonumber(o.id)
  for k, v in pairs(o.tags) do
    values.key = k
    values.value = v
    self.Q.insert_tag:exec(values)
  end
end


function osmdb:_insert_element(o, type)
  local values = self.values
  values.id = tonumber(o.id)
  values.type = type
  self.Q.insert_element:exec(values)
  self:insert_tags(o)

  if o.version or o.changeset or o.user or o.uid or o.timestamp then
    values.version = tonumber(o.version)
    values.changeset = tonumber(o.changeset)
    values.user = o.user
    values.uid = tonumber(o.uid)
    values.timestamp = o.timestamp and os.date("%Y-%m-%d %H:%M:%S", o.timestamp)
    self.Q.insert_info:exec(values)
  end
end


function osmdb:insert_node(o)
  self:_insert_element(o, "node")

  local values = self.values
  values.latitude = tonumber(o.latitude)
  values.longitude = tonumber(o.longitude)
  self.Q.insert_location:exec(values)
end


function osmdb:insert_way(o)
  self:_insert_element(o, "way")

  local values = self.values
  values.parent_id = tonumber(o.id)
  for i, n in ipairs(o.node_refs) do
    values.ord = i
    values.node_id = tonumber(n)
    self.Q.insert_node:exec(values)
  end
end


function osmdb:insert_relation(o)
  self:_insert_element(o, "relation")

  local values = self.values
  values.parent_id = tonumber(o.id)
  for i, n in ipairs(o.member_refs) do
    values.ord = i
    values.role = n.role
    values.member_id = tonumber(n.id)
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
  local version, changeset, user, uid, timestamp =
    self.Q.select_info:uexec(element.id)
  if version then
    element.version = version
    element.changeset = changeset
    element.user = user
    element.uid = uid
    element.timestamp = timestamp
  end

  element.tags = {}
  for key, value in self.Q.select_tags:urows(element.id) do
    element.tags[key] = value
  end

  if element.type == "node" then
    element.latitude, element.longitude =
      self.Q.select_location:uexec(element.id)
  elseif element.type == "way" then
    if with_children then
      element.nodes = {}
      for node_id in self.Q.select_nodes:urows(element.id) do
        local n = self.Q.select_element:nexec(node_id, "element")
        element.nodes[#element.nodes+1] = self:read_element(n)
      end
    else
      element.node_refs = {}
      for node_id in self.Q.select_nodes:urows(element.id) do
        element.node_refs[#element.node_refs+1] = node_id
      end
    end
  elseif element.type == "relation" then
    if with_children then
      element.members = {}
      for role, member_id in self.Q.select_members:urows(element.id) do
        local m = self.Q.select_element:nexec(member_id)
        if m then
          element.members[#element.members+1] =
            { role = role, member = self:read_element(m) }
        end
      end
    else
      element.member_refs = {}
      for role, member_id in self.Q.select_members:urows(element.id) do
        element.member_refs[#element.member_refs+1] =
            { role = role, id = member_id }
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

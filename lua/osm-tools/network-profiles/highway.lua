local scores =
{
  highway =
  {
    abandoned              = -1,
    bridleway              = -1,
    construction           = -1,
    crossing               = -1,
    cycleway               = -1,
    footway                = -1,
    paper                  = -1,
    path                   = -1,
    ["path-disabled"]      = -1,
    pedestrian             = -1,
    platform               = -1,
    proposed               = -3,
    raceway                = -1,
    rest_area              = -1,
    service                = -1,
    services               = -1,
    steps                  = -1,
    subway                 = -1,
    tidal_path             = -3,
    track                  = -3,
    traffic_signals        = -1,
    ["unclassified;track"] = -1,
    undefined              = -1,
    unknown                = -1,
    unmarked_route         = -1,
  },
  vehicle =
  {
    no                     = -1,
  },
  motorcar =
  {
    no                     = -1,
  },
  motor_vehicle =
  {
    no                     = -1,
  },
  psv =
  {
    yes                    =  2,
  },
  type =
  {
    proposed               = -3,
  },
  service =
  {
    beach_access           = -1,
    driveway               = -1,
    ["Pylon road"]         = -1,
    slipway                = -1,
    yard                   = -1,
  },
  building =
  {
    yes                    = -1,
  },
  access =
  {
    no                     = -1,
  },
}


local function reverse_in_place(t)
  local L = #t+1
  for i = 1, math.floor((L-1) / 2) do
    t[i], t[L-i] = t[L-i], t[i]
  end
end


local function add_name(name, list, map)
  if name and not map[name] then
    list[#list+1] = name
    map[name] = true
  end
end


local function filter_highways(type, o)
  if type == "way" then
    if not o.tags.highway then return end
    local s = 0
    for field, value in pairs(o.tags) do
      local f = scores[field]
      if f then
        s = s + (f[value] or 0)
      end
    end
    if s >= 0 then
      local new_tags = {}

      local oneway, reverse = o.tags.oneway
      if oneway and oneway == "-1" then
        reverse = true
        oneway = "yes"
      end
      oneway = (oneway and oneway:lower() == "yes") or
               (o.tags.junction and o.tags.junction:lower() == "roundabout")
      new_tags.oneway = oneway
      if reverse then
        reverse_in_place(o.node_refs)
      end

      local level
      if o.tags.bridge and o.tags.bridge:lower() ~= "no" then level = 1 end
      if o.tags.tunnel and o.tags.tunnel:lower() ~= "no" then level = -1 end
      new_tags.level = level

      local names, map = {}, {}
      add_name(o.tags.name, names, map)
      add_name(o.tags.alt_name, names, map)
      add_name(o.tags.old_name, names, map)
      add_name(o.tags.ref, names, map)
      add_name(o.tags.old_ref, names, map)
      local name = table.concat(names, ";")
      new_tags.name = name ~= "" and name or nil


      new_tags.speed = o.tags.maxspeed or "50"

      return { id = o.id, node_refs = o.node_refs, tags = new_tags }
    end
  elseif type == "node" then
    local new_tags = {}
    local hw = o.tags.highway
    if hw == "traffic_signals" then new_tags.lights = 1 end
    if hw == "give_way" then new_tags.give_way = 1 end
    if hw == "stop" then new_tags.stop = 1 end
    if o.tags.traffic_calming then new_tags.bump = 1 end
    return { id = o.id, latitude = o.latitude, longitude = o.longitude, tags = new_tags }
  end
end

return filter_highways

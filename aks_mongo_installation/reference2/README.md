

rs.initiate()
conf=rs.conf()
conf.members[0].host="mongo-node-1:27017"
rs.reconfig(conf)
rs.add("mongo-node-2")
rs.add("mongo-node-3")

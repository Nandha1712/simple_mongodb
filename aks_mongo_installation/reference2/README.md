

rs.initiate()
conf=rs.conf()
conf.members[0].host="mongo-0:27017"
rs.reconfig(conf)
rs.add("mongo-1")
rs.add("mongo-2")

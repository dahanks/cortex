graph = TitanFactory.open("conf/audrey.properties");
g = graph.traversal();
g.V().values("name");
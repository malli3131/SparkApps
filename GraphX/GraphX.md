## GraphX -- Graph Processing

### Overview:
* GraphX is one of the Ecosystem projects of Spark, it is used for graphs and graphs parallel computation.
* GraphX is a Graph abstraction on top of Spark Core i.e., RDD.
* Graph is directed multigraph with properties attached to each Vertex and Edge.
* GraphX provides a set of operators to do Graph computations.
* GraphX offers huge collection of algorithms and builders.

### Property Graph:
* Property graph is directed multigraph with user defined objects attached to each vertex and edge.
* Directed multigraph is directed graph with potentially multiple parallel edges sharing with same source and destination vertex.
* The ability to support parallel edges simplifies modeling scenarios where there can be multiple relationships (e.g., co-worker and friend) between the same vertices.
* Each vertex is keyed by a unique 64-bit long identifier (VertexId).
* GraphX does not impose any ordering constraints on the vertex identifiers. Similarly, edges have corresponding source and destination vertex identifiers.
* The property graph is parameterized over the vertex (VD) and edge (ED) types.
* These are the types of the objects associated with each vertex and edge respectively.
* Property graphs are immutable, distributed, and fault-tolerant.
* The graph is partitioned across the executors using a range of vertex partitioning heuristics.
* We have VertexRDD[VD] and EdgeRDD[ED]
* The classes VertexRDD[VD] and EdgeRDD[ED] extend and are optimized versions of RDD[(VertexId, VD)] and RDD[Edge[ED]] respectively. Both VertexRDD[VD] and EdgeRDD[ED] provide additional functionality built around graph computation and leverage internal optimizations.
* Graph is defined as ```val graph: Graph[(String, String), String]```

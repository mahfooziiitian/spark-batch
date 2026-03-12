import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc
from graphframes import GraphFrame

# Set environment variables
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["JAVA_HOME"] = os.environ.get("JAVA_HOME_11", "/path/to/spark")

def create_spark_session():
    """Initialize Spark session with GraphFrames package"""
    return SparkSession.builder \
        .appName("GraphFramesExample") \
        .config("spark.jars.packages", "graphframes:graphframes:0.8.2-spark3.0-s_2.12") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()

def create_sample_graph(spark):
    """Create sample vertices and edges DataFrames"""
    # Create vertices DataFrame
    vertices = spark.createDataFrame([
        ("a", "Alice", 34, "Engineer"),
        ("b", "Bob", 36, "Manager"),
        ("c", "Charlie", 30, "Designer"),
        ("d", "Diana", 28, "Analyst"),
        ("e", "Eve", 32, "Developer")
    ], ["id", "name", "age", "role"])

    # Create edges DataFrame
    edges = spark.createDataFrame([
        ("a", "b", "friend", 0.8),
        ("b", "c", "follow", 0.6),
        ("c", "a", "follow", 0.7),
        ("a", "d", "colleague", 0.9),
        ("d", "e", "mentor", 0.95),
        ("e", "c", "friend", 0.75)
    ], ["src", "dst", "relationship", "weight"])

    return GraphFrame(vertices, edges)

def analyze_graph(graph):
    """Perform various graph analyses"""
    print("=== GRAPH ANALYSIS ===")
    
    # Basic graph info
    print(f"Number of vertices: {graph.vertices.count()}")
    print(f"Number of edges: {graph.edges.count()}")
    
    # Show all vertices and edges
    print("\n--- Vertices ---")
    graph.vertices.show()
    
    print("\n--- Edges ---")
    graph.edges.show()
    
    # Filter vertices by age
    print("\n--- Vertices with age > 32 ---")
    filtered_vertices = graph.filterVertices("age > 32")
    filtered_vertices.vertices.show()
    
    # Calculate in-degrees and out-degrees
    print("\n--- In-degrees ---")
    in_degrees = graph.inDegrees
    in_degrees.show()
    
    print("\n--- Out-degrees ---")
    out_degrees = graph.outDegrees
    out_degrees.show()
    
    # Find triangles
    print("\n--- Triangles ---")
    triangles = graph.triangleCount()
    triangles.show()
    
    # Connected components
    print("\n--- Connected Components ---")
    components = graph.connectedComponents()
    components.show()
    
    # PageRank
    print("\n--- PageRank ---")
    pagerank = graph.pageRank(resetProbability=0.15, maxIter=10)
    pagerank.vertices.select("id", "name", "pagerank").orderBy(desc("pagerank")).show()
    
    # Find motifs (patterns)
    print("\n--- Motifs: (a)-[e1]->(b); (b)-[e2]->(c) ---")
    motifs = graph.find("(a)-[e1]->(b); (b)-[e2]->(c)")
    motifs.show()

def shortest_paths_analysis(graph):
    """Analyze shortest paths between vertices"""
    print("\n=== SHORTEST PATHS ANALYSIS ===")
    
    # Shortest paths from vertex 'a'
    landmarks = ["a", "c"]
    shortest_paths = graph.shortestPaths(landmarks=landmarks)
    shortest_paths.show(truncate=False)

def main():
    """Main function to run the graph analysis"""
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Create graph
        graph = create_sample_graph(spark)
        
        # Perform analysis
        analyze_graph(graph)
        shortest_paths_analysis(graph)
        
    finally:
        # Stop Spark session
        spark.stop()

if __name__ == "__main__":
    main()

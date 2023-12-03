from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import networkx as nx
import time
import logging

# Configure logging
logging.basicConfig(filename='spark_app.log', filemode='w', level=logging.INFO)

def bronKerbosch1_parallel(R, P, X, G):
    if len(P) == 0 and len(X) == 0:
        return [R]
    
    results = []
    for v in P.copy():
        subgraph = G.subgraph(P.intersection(set(G.neighbors(v))).union({v}))
        subresult = bronKerbosch1_parallel(R.union({v}), P.intersection(set(G.neighbors(v))), X.intersection(set(G.neighbors(v))), subgraph)
        results.extend(subresult)
        P.remove(v)
        X.add(v)
    
    return results

def parallel_bronKerbosch(sc, graph):
    nodes = list(graph.nodes())
    
    # Broadcast the graph to all workers
    broadcast_graph = sc.broadcast(graph)
    # Parallelize the nodes
    parallel_nodes = sc.parallelize(nodes)
    
    # Use map to apply the function in parallel
    result_rdd = parallel_nodes.map(lambda v: bronKerbosch1_parallel({v}, set(graph.neighbors(v)), set(), broadcast_graph.value))

    # List of sets
    results = [item for sublist in result_rdd.collect() for item in sublist]
    # Return the list of unique sets transposed as a list
    return list(map(set, set(map(frozenset, results))))

def main():
    G1 = nx.gnp_random_graph(100, 0.5, directed=False)

    # Configure Spark to use multiple cores
    conf = SparkConf().setAppName("BronKerbosch")
    sc = SparkContext(conf=conf)

    try:
        start_time = time.time()
        results = parallel_bronKerbosch(sc, G1)
        end_time = time.time()
        # Take the bigger set inside the list
        results = max(results, key=len)
        logging.info("Results: %s", results)
        logging.info("Time elapsed: %s", end_time - start_time)
    except Exception as e:
        logging.error("An error occurred: %s", str(e))
    finally:
        sc.stop()

if __name__ == "__main__":
    main()

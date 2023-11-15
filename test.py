
from pyspark import SparkContext

def main():
    # Initialize Spark Context
    sc = SparkContext(appName="ClusterTest")

    # Create an RDD
    data = range(1, 1001)  # A list of numbers from 1 to 1000
    rdd = sc.parallelize(data)

    # Simple operation: count the number of elements
    count = rdd.count()
    print(f"Total elements in the RDD: {count}")

    # Another operation: sum of all elements
    total_sum = rdd.reduce(lambda a, b: a + b)
    print(f"Total sum of elements: {total_sum}")

    # Stop the SparkContext
    sc.stop()

if __name__ == "__main__":
    main()
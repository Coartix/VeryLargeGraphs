from pyspark import SparkContext
import random
import time

def is_point_inside_unit_circle(_):
    x, y = random.random(), random.random()
    return x*x + y*y < 1

def main():
    sc = SparkContext(appName="PiCalculation")
    
    num_samples = 1000000000000  # Adjust this for more or less computational load

    # Parallelize the process
    count = sc.parallelize(range(0, num_samples)) \
              .filter(is_point_inside_unit_circle) \
              .count()

    # Calculate and print the approximate value of Pi
    pi_estimate = 4 * count / num_samples
    print("Approximate value of Pi is:", pi_estimate)

    sc.stop()

if __name__ == "__main__":
    # benchmark
    start_time = time.time()
    main()
    end_time = time.time()
    print("Time elapsed: ", end_time - start_time)
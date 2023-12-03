from pyspark import SparkContext
import random
import time
import logging

# Configure logging
logging.basicConfig(filename='spark_app.log', filemode='w', level=logging.INFO)

def is_point_inside_unit_circle(_):
    x, y = random.random(), random.random()
    return x*x + y*y < 1

def main():
    sc = SparkContext(appName="PiCalculation")
    
    try:
        start_time = time.time()
        num_samples = int(1e9)  # Adjust this for more or less computational load
        # Parallelize the process
        count = sc.parallelize(range(0, num_samples)) \
                .filter(is_point_inside_unit_circle) \
                .count()

        # Calculate and print the approximate value of Pi
        pi_estimate = 4 * count / num_samples
        end_time = time.time()

        logging.info("Approximate value of Pi is: %s", pi_estimate)
        logging.info("Time elapsed: %s", end_time - start_time)
    except Exception as e:
        print("An error occurred: ", str(e))
    finally:
        sc.stop()

if __name__ == "__main__":
    main()
# --------------------------------------------------------
#
# PYTHON PROGRAM DEFINITION
#
# The knowledge a computer has of Python can be specified in 3 levels:
# (1) Prelude knowledge --> The computer has it by default.
# (2) Borrowed knowledge --> The computer gets this knowledge from 3rd party libraries defined by others
#                            (but imported by us in this program).
# (3) Generated knowledge --> The computer gets this knowledge from the new functions defined by us in this program.
#
# When launching in a terminal the command:
# user:~$ python3 this_file.py
# our computer first processes this PYTHON PROGRAM DEFINITION section of the file.
# On it, our computer enhances its Python knowledge from levels (2) and (3) with the imports and new functions
# defined in the program. However, it still does not execute anything.
#
# --------------------------------------------------------

import pyspark
import pyspark.streaming

import os
import shutil
import time
import datetime
from haversine import haversine, Unit
# ------------------------------------------
# FUNCTION process_line
# ------------------------------------------
def process_line(line):
    # 1. We create the output variable
    res = ()

    # 2. We get the parameter list from the line
    params_list = line.strip().split(",")

    #(00) Date => The date of the measurement. String <%Y-%m-%d %H:%M:%S> (e.g., "2013-01-01 13:00:02").
    #(01) Bus_Line => The bus line. Int (e.g., 120).
    #(02) Bus_Line_Pattern => The pattern of bus stops followed by the bus. String (e.g., "027B1001"). It can be empty (e.g., "").
    #(03) Congestion => On whether the bus is at a traffic jam (No -> 0 and Yes -> 1). Int (e.g., 0).
    #(04) Longitude => Longitude position of the bus. Float (e.g., -6.269634).
    #(05) Latitude = > Latitude position of the bus. Float (e.g., 53.360504).
    #(06) Delay => Delay of the bus in seconds (negative if ahead of schedule). Int (e.g., 90).
    #(07) Vehicle => An identifier for the bus vehicle. Int (e.g., 33304)
    #(08) Closer_Stop => An idenfifier for the closest bus stop given the current bus position. Int (e.g., 7486). It can be no bus stop, in which case it takes value -1 (e.g., -1).
    #(09) At_Stop => On whether the bus is currently at the bus stop (No -> 0 and Yes -> 1). Int (e.g., 0).

    # 3. If the list contains the right amount of parameters
    if (len(params_list) == 10):
        # 3.1. We set the right type for the parameters
        params_list[1] = int(params_list[1])
        params_list[3] = int(params_list[3])
        params_list[4] = float(params_list[4])
        params_list[5] = float(params_list[5])
        params_list[6] = int(params_list[6])
        params_list[7] = int(params_list[7])
        params_list[8] = int(params_list[8])
        params_list[9] = int(params_list[9])

        # 3.2. We assign res
        res = tuple(params_list)

    # 4. We return res
    return res

# Using haversine lib to calculate the distance between two bus station using long,lat
# This function returns tuples (time,point1,point2,distance)

def get_distance_function(point1, point2):
        
   #print(f"Start->point1:{point1},point2:{point2}")
   p1 = (point1[1], point1[2])
   p2 = (point2[1], point2[2])
   
   dist = haversine(p1, p2, unit=Unit.METERS)
   time_gap = abs(point2[0] - point1[0]).total_seconds()

   speed = dist / time_gap
   #print(f"point1:{p1},point2:{p2},distance:{dist},speed:{speed}")
   # distance zero for exceeding speed
   # noisy data
   if (speed > max_speed_accepted):
      dist = 0
   final_time = point2[0]
   point = (point2[1], point2[2])
   if point2[0] < point1[0]:
      final_time = point1[0]
      point = (point1[1], point1[2])
   #print(f"End->{final_time}-{point[0]},{point[1],{dist + point1[3] + point2[3]}}")
   return (final_time, point[0], point[1], dist + point1[3] + point2[3])

def make_fill_bucket(param,bucket_size):
   bucket_index = int(param[1] / bucket_size)
   range_interval = f"{int(bucket_index * bucket_size)}_{int(bucket_index * bucket_size) + bucket_size}"
   return (range_interval,(bucket_index, 1))	
        
        
# ------------------------------------------
# FUNCTION my_state_update
# ------------------------------------------
def my_state_update(time_interval_list_of_collected_new_values, cur_agg_val):
    # 1. We create the output variable
    res = 0

    # 2. If this is the first time we find the key, we initialise it
    if (cur_agg_val is None):
        cur_agg_val = 0.0

    # 3. We update the state
    res = sum(time_interval_list_of_collected_new_values) + cur_agg_val

    # 4. We return res
    return res
            
# ------------------------------------------
# FUNCTION my_model
# ------------------------------------------
def my_model(ssc,
             monitoring_dir,
             bucket_size,
             max_speed_accepted,
             day_picked
            ):
    # 1. Operation C1: 'textFileStream' to load the dataset into a DStream
    inputDStream = ssc.textFileStream(monitoring_dir)

    date_format = "%Y-%m-%d %H:%M:%S"
    #filter based on date picked
    selectedDateDStream = inputDStream.filter(lambda x: day_picked in x)
    
    #split the lines and sort by vehicle id
    formattedDStream = selectedDateDStream.map(process_line)\
                 .transform(lambda rdd : rdd.sortBy(lambda x: datetime.datetime.strptime(x[0], date_format)))\
                 .transform(lambda rdd : rdd.sortBy(lambda x:x[7]))
                 
    # 5 is lattitude
    # 4 is longitude
    # adding extra column for distance between two point-will be used later

    latilongdistanceDStream = formattedDStream.map(lambda x: (x[7], \
                                                     (datetime.datetime.strptime(x[0], date_format), x[5], x[4], 0)))                 
    
    vehicleDistanceDStream = latilongdistanceDStream.reduceByKey(get_distance_function) \
                                   .map(lambda x: (x[0], x[1][3] / 1000))
          
    agreegatedDStream = vehicleDistanceDStream.updateStateByKey(my_state_update)
                                 
    bucketisedDStream = agreegatedDStream.map(lambda row: make_fill_bucket(row,bucket_size)) 
    
    bucketIndexCountDStream = bucketisedDStream.reduceByKey(lambda x,y: (x[0],x[1]+y[1]))
    
    prenSolRDD = bucketIndexCountDStream.map(lambda row: (row[1][0],(row[0],row[1][1])))
                       
    solutionRDD = prenSolRDD.map(lambda row:(row[0],row[1][0],row[1][1]))\
                            .transform(lambda rdd: rdd.sortBy(lambda row: row[0]))
    # ---------------------------------------

    # Operation A1: 'pprint' to get all results
    solutionRDD.pprint()

# ------------------------------------------
# FUNCTION get_source_dir_file_names
# ------------------------------------------
def get_source_dir_file_names(local_False_databricks_True, source_dir, verbose):
    # 1. We create the output variable
    res = []

    # 2. We get the FileInfo representation of the files of source_dir
    fileInfo_objects = []
    if local_False_databricks_True == False:
        fileInfo_objects = os.listdir(source_dir)
    #else:
    #    fileInfo_objects = dbutils.fs.ls(source_dir)

    # 3. We traverse the fileInfo objects, to get the name of each file
    for item in fileInfo_objects:
        # 3.1. We get a string representation of the fileInfo
        file_name = str(item)

        # 3.2. If the file is processed in DBFS
        if local_False_databricks_True == True:
            # 3.2.1. We look for the pattern name= to remove all useless info from the start
            lb_index = file_name.index("name='")
            file_name = file_name[(lb_index + 6):]

            # 3.2.2. We look for the pattern ') to remove all useless info from the end
            ub_index = file_name.index("',")
            file_name = file_name[:ub_index]

        # 3.3. We append the name to the list
        res.append(file_name)
        if verbose == True:
            print(file_name)

    # 4. We sort the list in alphabetic order
    res.sort()

    # 5. We return res
    return res

# ------------------------------------------
# FUNCTION streaming_simulation
# ------------------------------------------
def streaming_simulation(local_False_databricks_True,
                         source_dir,
                         monitoring_dir,
                         time_step_interval,
                         verbose,
                         dataset_file_names
                        ):

    # 1. We check what time is it
    start = time.time()

    # 2. We set a counter in the amount of files being transferred
    count = 0

    # 3. If verbose mode, we inform of the starting time
    if (verbose == True):
        print("Start time = " + str(start))

    # 4. We transfer the files to simulate their streaming arrival.
    for file in dataset_file_names:
        # 4.1. We copy the file from source_dir to dataset_dir
        if local_False_databricks_True == False:
            shutil.copyfile(source_dir + file, monitoring_dir + file)
        else:
            dbutils.fs.cp(source_dir + file, monitoring_dir + file)

        # 4.2. If verbose mode, we inform from such transferrence and the current time.
        if (verbose == True):
            print("File " + str(count) + " transferred. Time since start = " + str(time.time() - start))

        # 4.3. We increase the counter, as we have transferred a new file
        count = count + 1

        # 4.4. We wait the desired transfer_interval until next time slot.
        time_to_wait = (start + (count * time_step_interval)) - time.time()
        if (time_to_wait > 0):
            time.sleep(time_to_wait)

# ------------------------------------------
# FUNCTION create_ssc
# ------------------------------------------
def create_ssc(sc,
               time_step_interval,
               monitoring_dir,
               bucket_size,
               max_speed_accepted,
               day_picked
              ):
    # 1. We create the new Spark Streaming context acting every time_step_interval.
    ssc = pyspark.streaming.StreamingContext(sc, time_step_interval)

    # 2. We model the data processing to be done each time_step_interval.
    my_model(ssc,
             monitoring_dir,
             bucket_size,
             max_speed_accepted,
             day_picked
            )

    # 3. We return the ssc configured and modelled.
    return ssc

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(sc,
            local_False_databricks_True,
            source_dir,
            monitoring_dir,
            checkpoint_dir,
            time_step_interval,
            verbose,
            bucket_size,
            max_speed_accepted,
            day_picked
           ):

    # 1. We get the names of the files of our dataset
    dataset_file_names = get_source_dir_file_names(local_False_databricks_True, source_dir, verbose)

    # 2. We setup the Spark Streaming context.
    # This sets up the computation that will be done when the system receives data.
    ssc = pyspark.streaming.StreamingContext.getActiveOrCreate(checkpoint_dir,
                                                               lambda: create_ssc(sc,
                                                                                  time_step_interval,
                                                                                  monitoring_dir,
                                                                                  bucket_size,
                                                                                  max_speed_accepted,
                                                                                  day_picked
                                                                                 )
                                                               )

    # 3. We start the Spark Streaming Context in the background to start receiving data.
    #    Spark Streaming will start scheduling Spark jobs in a separate thread.
    ssc.start()
    ssc.awaitTerminationOrTimeout(time_step_interval)

    # 4. We simulate the streaming arrival of files (i.e., one by one) from source_dir to monitoring_dir.
    streaming_simulation(local_False_databricks_True,
                         source_dir,
                         monitoring_dir,
                         time_step_interval,
                         verbose,
                         dataset_file_names
                        )

    # 5. We stop the Spark Streaming Context
    ssc.stop(False)
    if (not sc._jvm.StreamingContext.getActive().isEmpty()):
        sc._jvm.StreamingContext.getActive().get().stop(False)

# --------------------------------------------------------
#
# PYTHON PROGRAM EXECUTION
#
# Once our computer has finished processing the PYTHON PROGRAM DEFINITION section its knowledge is set.
# Now its time to apply this knowledge.
#
# When launching in a terminal the command:
# user:~$ python3 this_file.py
# our computer finally processes this PYTHON PROGRAM EXECUTION section, which:
# (i) Specifies the function F to be executed.
# (ii) Define any input parameter such this function F has to be called with.
#
# --------------------------------------------------------
if __name__ == '__main__':
    # 1. We use as many input arguments as needed

    # 1.1 We use as many input arguments as needed
    bucket_size = 1
    max_speed_accepted = 28.0
    day_picked = "2013-01-07"

    # 1.2. We specify the time interval each of our micro-batches (files) appear for its processing.
    time_step_interval = 5

    # 1.3. We configure verbosity during the program run
    verbose = False

    # 2. Local or Databricks
    local_False_databricks_True = False

    # 3. We set the path to my_dataset, my_monitoring, my_checkpoint and my_result
    my_local_path = "../../../../3_Code_Examples/L07-23_Spark_Environment/"
    my_databricks_path = "/"

    source_dir = "my_datasets/A02_ex4_micro_dataset_1/"    
    monitoring_dir = "my_datasets/my_monitoring/"
    checkpoint_dir = "my_datasets/my_checkpoint/"

    #if local_False_databricks_True == False:
    #    source_dir = my_local_path + source_dir
    #    monitoring_dir = my_local_path + monitoring_dir
    #    checkpoint_dir = my_local_path + checkpoint_dir
    #else:
    #    source_dir = my_databricks_path + source_dir
    #    monitoring_dir = my_databricks_path + monitoring_dir
    #    checkpoint_dir = my_databricks_path + checkpoint_dir

    # 4. We remove the directories
    if local_False_databricks_True == False:
        # 4.1. We remove the monitoring_dir
        if os.path.exists(monitoring_dir):
            shutil.rmtree(monitoring_dir)

        # 4.2. We remove the checkpoint_dir
        if os.path.exists(checkpoint_dir):
            shutil.rmtree(checkpoint_dir)
    #else:
        # 4.1. We remove the monitoring_dir
    #    dbutils.fs.rm(monitoring_dir, True)

        # 4.2. We remove the checkpoint_dir
    #    dbutils.fs.rm(checkpoint_dir, True)

    # 5. We re-create the directories again
    if local_False_databricks_True == False:
        # 5.1. We re-create the monitoring_dir
        os.mkdir(monitoring_dir)

        # 5.2. We re-create the checkpoint_dir
        os.mkdir(checkpoint_dir)
    #else:
        # 5.1. We re-create the monitoring_dir
    #    dbutils.fs.mkdirs(monitoring_dir)

        # 5.2. We re-create the checkpoint_dir
    #    dbutils.fs.mkdirs(checkpoint_dir)

    # 6. We configure the Spark Context
    sc = pyspark.SparkContext.getOrCreate()
    sc.setLogLevel('WARN')
    print("\n\n\n")

    # 7. We call to our main function
    my_main(sc,
            local_False_databricks_True,
            source_dir,
            monitoring_dir,
            checkpoint_dir,
            time_step_interval,
            verbose,
            bucket_size,
            max_speed_accepted,
            day_picked
           )

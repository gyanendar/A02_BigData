# Gyanendar Manohar
# R00207241
# Assignment #2
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
import pyspark.sql.functions

import os
import shutil
import time
import pyspark.sql.functions as f

# ------------------------------------------
# FUNCTION my_model
# ------------------------------------------
def my_model(spark,
             monitoring_dir,
             checkpoint_dir,
             time_step_interval,
             vehicle_id,
             day_picked,
             delay_limit
            ):
    # 1. We create the DataStreamWritter
    myDSW = None

    # 2. We set the frequency for the time steps
    # 2. We set the frequency for the time steps
    my_window_duration_frequency = str(2 * time_step_interval) + " seconds"
    my_sliding_duration_frequency = str(1 * time_step_interval) + " seconds"
    my_frequency = str(time_step_interval) + " seconds"

    # 3. Operation C1: We create the DataFrame from the dataset and the schema

    # 3.1. We define the Schema of our DF.
    my_schema = pyspark.sql.types.StructType(
        [pyspark.sql.types.StructField("date", pyspark.sql.types.StringType(), False),
         pyspark.sql.types.StructField("busLineID", pyspark.sql.types.IntegerType(), False),
         pyspark.sql.types.StructField("busLinePatternID", pyspark.sql.types.StringType(), False),
         pyspark.sql.types.StructField("congestion", pyspark.sql.types.IntegerType(), False),
         pyspark.sql.types.StructField("longitude", pyspark.sql.types.FloatType(), False),
         pyspark.sql.types.StructField("latitude", pyspark.sql.types.FloatType(), False),
         pyspark.sql.types.StructField("delay", pyspark.sql.types.IntegerType(), False),
         pyspark.sql.types.StructField("vehicleID", pyspark.sql.types.IntegerType(), False),
         pyspark.sql.types.StructField("closerStopID", pyspark.sql.types.IntegerType(), False),
         pyspark.sql.types.StructField("atStop", pyspark.sql.types.IntegerType(), False)
        ])

    # 3.2. We use it when loading the dataset
    inputSDF = spark.readStream.format("csv") \
                               .option("delimiter", ",") \
                               .option("quote", "") \
                               .option("header", "false") \
                               .schema(my_schema) \
                               .load(monitoring_dir)

    # 4. Operation T1: We add the current timestamp
    # Add new column arrivalTime in format HH:mm:ss
    # This is needed in final output
    time_inputSDF = inputSDF.withColumn("my_time", pyspark.sql.functions.current_timestamp())\
                            .withColumn("arrivalTime",f.date_format(inputSDF["date"],"HH:mm:ss"))

    # Apply first filtering on date, vechile id, and atStop
    # atStop shouln't be 0
    # vehicleID should match with the provided input parameter
    # date should match
    #
    # Keep necessary columns and discrad the rest
    #
    filteredSDF = time_inputSDF.select(f.col("arrivalTime"),\
                                      f.col("busLineID").alias("lineID"),\
                                      f.col("closerStopID").alias("stationID"),\
                                      f.col("my_time"))\
                              .where((f.col("atStop") != 0) & \
                                     (f.col("vehicleID") == vehicle_id) &                                            
                                     (f.to_date(f.lit(day_picked),"yyyy-MM-dd") == \
                                      f.to_date(time_inputSDF["date"],"yyyy-MM-dd HH:mm:ss")) )
    
    # Adding new column   timeLineStationID by concatenating arrivalTime, lineID & stationID                               
    
       
    # Now need to do gorupby and for that watermarking is prerequisite
    # do watermarking and groupby on my_time
    #
    # Generating new column on fly by appending arrivalTime,Lineid,stationID and collecting this in set
    # the collected set will have data like ['10:14:00,72,750', '09:16:00,72,900', '10:12:00,50,450', '10:09:00,50,279', '10:15:00,72,750']
    
    groupedSDF = filteredSDF.withWatermark("my_time", "0 seconds") \
    			     .groupBy(pyspark.sql.functions.window("my_time", my_window_duration_frequency, my_frequency))\
    			     .agg(f.collect_set(f.concat(f.col("arrivalTime"),
    			                                 f.lit(','), 
    			                                 f.col("lineID"),
    			                                 f.lit(','),
    			                                 f.col("stationID"))).alias("timeLineStationID"))
    
    # Create udf for sorting list 
    def sort_list(string_list):
        
        string_list.sort()
        
        return string_list
                
    #Register the UDF        
    getlist_udf = f.udf(sort_list, pyspark.sql.types.ArrayType(pyspark.sql.types.StringType()))
    
    # pass the new column timeLineStationID to defned UDF sorted_list which will sort the parameter passed to it
    # as content of list is string starting with time stamp, the sortlist will have item in sorted order as per time
    
    sortedListSDF = groupedSDF.select(getlist_udf(groupedSDF['timeLineStationID']).alias('timeLineStationID'))
    
    # Now need to form column from list 
    # Explode will do this job 
    
    explodeSDF = sortedListSDF.withColumn("combinedCol",f.explode(f.col('timeLineStationID')))\
                          .drop('timeLineStationID')
    
    # FInal output should have three colum
    # Split the combinedcol and give name to it based on content
    solutionSDF =  explodeSDF.select(f.split(f.col('combinedCol'),',').getItem(0).alias("arrival_time"),
                                     f.split(f.col('combinedCol'),',').getItem(1).alias("lineID"),
                                     f.split(f.col('combinedCol'),',').getItem(2).alias("stationID"))
    
    
    # I got correct output once in 10 run. For rest nine runs, I see multple group per batch 
	#-------------------------------------------                                     
	#Batch: 2
	#-------------------------------------------
	#+--------------------+--------------------+
	#|              window|   timeLineStationID|
	#+--------------------+--------------------+
	#|{2021-12-05 05:51...|[08:14:00,72,750,...|
	#|{2021-12-05 05:51...|[08:14:00,72,750,...|
	#+--------------------+--------------------+
    # This caused repeated content in the final output. This problem is realted to timing and I am not able to do much here
    # Output is not consistent across run
    # ---------------------------------------
    
       
    
    # Operation O1: We create the DataStreamWritter, to print by console the results in complete mode
    myDSW = solutionSDF.writeStream\
                       .format("console") \
                       .trigger(processingTime=my_frequency) \
                       .option("checkpointLocation", checkpoint_dir) \
                       .outputMode("append")

    # We return the DataStreamWritter
    return myDSW

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
                         num_batches,
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

    # 5. Let's try to sort out the patch for passing the last file
    if (len(dataset_file_names) > 0):
        # 5.1. We get the name again of the last file
        file = dataset_file_names[-1]

        # 5.2. We copy the file from source_dir to dataset_dir
        if local_False_databricks_True == False:
            shutil.copyfile(source_dir + file, monitoring_dir + file[:-4] + "_redundant.csv")
        else:
            dbutils.fs.cp(source_dir + file, monitoring_dir + file[:-4] + "_redundant.csv")

        # 5.3. If verbose mode, we inform from such transferrence and the current time.
        if (verbose == True):
            print("File " + str(count) + " transferred. Time since start = " + str(time.time() - start))

        # 5.4. We increase the counter, as we have transferred a new file
        count = count + 1

        # 5.5. We wait the desired transfer_interval until next time slot.
        time_to_wait = (start + (count * time_step_interval)) - time.time()
        if (time_to_wait > 0):
            time.sleep(time_to_wait)

    # 6. We wait for another time_interval
    time.sleep(time_step_interval * num_batches)

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(spark,
            local_False_databricks_True,
            source_dir,
            monitoring_dir,
            checkpoint_dir,
            time_step_interval,
            num_batches,
            verbose,
            vehicle_id,
            day_picked,
            delay_limit
           ):

    # 1. We get the names of the files of our dataset
    dataset_file_names = get_source_dir_file_names(local_False_databricks_True, source_dir, verbose)

    # 2. We get the DataStreamWriter object derived from the model
    dsw = my_model(spark,
                   monitoring_dir,
                   checkpoint_dir,
                   time_step_interval,
                   vehicle_id,
                   day_picked,
                   delay_limit
                  )

    # 3. We get the StreamingQuery object derived from starting the DataStreamWriter
    ssq = dsw.start()

    # 4. We simulate the streaming arrival of files (i.e., one by one) from source_dir to monitoring_dir
    streaming_simulation(local_False_databricks_True,
                         source_dir,
                         monitoring_dir,
                         time_step_interval,
                         verbose,
                         num_batches,
                         dataset_file_names
                        )

    # 5. We stop the StreamingQuery object
    try:
      ssq.stop()
    except:
      print("Thread streaming_simulation finished while Thread ssq is still computing")

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
    vehicle_id = 33145
    day_picked = "2013-01-02"
    delay_limit = 60

    # 1.2. We specify the time interval each of our micro-batches (files) appear for its processing.
    time_step_interval = 5

    # 1.3. We specify the num of batches
    num_batches = 4

    # 1.4. We configure verbosity during the program run
    verbose = False

    # 2. Local or Databricks
    local_False_databricks_True = False

    # 3. We set the path to my_dataset and my_result
    my_local_path = "../../../../3_Code_Examples/L07-23_Spark_Environment/"
    my_databricks_path = "/"

    source_dir = "my_datasets/A02_ex2_micro_dataset_1/"    
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

    # 6. We configure the Spark Session
    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    print("\n\n\n")

    # 7. We run my_main
    my_main(spark,
            local_False_databricks_True,
            source_dir,
            monitoring_dir,
            checkpoint_dir,
            time_step_interval,
            num_batches,
            verbose,
            vehicle_id,
            day_picked,
            delay_limit
           )

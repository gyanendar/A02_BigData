# Gyanendar Manohar
# R00207241
# --------------------------------------------------------
# dublinbikes.py
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
import time
import pyspark.sql.functions as f
import datetime
# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(spark,
            my_dataset_dir,
            start_time,
            end_time             
           ):

    # 1. We define the Schema of our DF.
    my_schema = pyspark.sql.types.StructType(
        [pyspark.sql.types.StructField("STATION ID", pyspark.sql.types.IntegerType(), False),
         pyspark.sql.types.StructField("TIME", pyspark.sql.types.StringType(), False),
         pyspark.sql.types.StructField("LAST UPDATED", pyspark.sql.types.StringType(), False),         
         pyspark.sql.types.StructField("NAME", pyspark.sql.types.StringType(), False),
         pyspark.sql.types.StructField("BIKE STANDS", pyspark.sql.types.IntegerType(), False),
         pyspark.sql.types.StructField("AVAILABLE BIKE STAND", pyspark.sql.types.IntegerType(), False),
         pyspark.sql.types.StructField("AVAILABLE BIKES", pyspark.sql.types.IntegerType(), False),
         pyspark.sql.types.StructField("STATUS", pyspark.sql.types.StringType(), False),
         pyspark.sql.types.StructField("ADDRESS", pyspark.sql.types.StringType(), False),
         pyspark.sql.types.StructField("LATTITUDE", pyspark.sql.types.FloatType(), False),
         pyspark.sql.types.StructField("LONGITUDE", pyspark.sql.types.FloatType(), False)       
         ])

    # 2. Operation C2: 'read'
    inputDF = spark.read.format("csv") \
        .option("delimiter", ",") \
        .option("quote", "") \
        .option("header", "false") \
        .schema(my_schema) \
        .load(my_dataset_dir)

    
    #
    # selected data should be from weekday and bike stand should be open
    #
    
    

    filteredDF = inputDF.select(f.col("STATION ID").alias("STATION_ID"),
                                f.col("TIME"),
                                f.col("AVAILABLE BIKES").alias("AVAILABLE_BIKES"),
                                f.col("AVAILABLE BIKE STAND").alias("AVAILABLE_BIKE_STAND"))\
                        .where((f.dayofweek(f.col("TIME")) >1) & \
                               (f.col("STATUS") =="Open") &\
                               (f.dayofweek(f.col("TIME")) <7))
    
    # Creating temporary view to visulaise filtered rows 
    filteredDF.createOrReplaceTempView('filtered_table')        
    
    # Select data between from 7AM until 7PM (anything from 8PM until 6.59AM not selected) 
    # Group by HOURS and station id and get average number of bikes available at the the station
    # 
    # This will give average number of bikes available for block of an hour per station for whole month

    # applying min and max on bikes, need two column for each agg function

    query = """select STATION_ID, 
                      hrs,
                      round(avg(AVAILABLE_BIKES),2) as bikes,
                      round(avg(AVAILABLE_BIKES),2) as bikes1 from 
    			(select STATION_ID, 
    			        DATE(TIME) as date, 
    			        HOUR(TIME) as hrs,
    			        AVAILABLE_BIKES,
    			        AVAILABLE_BIKE_STAND from filtered_table 
    			                             where HOUR(TIME) between 7 and 19) 
    			group by STATION_ID,hrs order by hrs"""
    
    averageBikePerhrDF = spark.sql(query)
    
    averageBikePerhrDF.persist()
    # Get minimum and maxinum average number of bikes available per station per hour
    
    minmaxbikeperhrDF = averageBikePerhrDF.groupby(["hrs"]).agg({"bikes":"min","bikes1":"max"})\
                                             .withColumnRenamed("min(bikes)", "min_bike")\
                                             .withColumnRenamed("max(bikes1)", "max_bike")
    minmaxbikeperhrDF.persist()
    # Rename hrs colums for selecting after join                                        
    hourBikeStationDF = averageBikePerhrDF.select(f.col('hrs').alias('HOUR'),\
                                    f.col('bikes'),\
                                    f.col('STATION_ID'))                                         
    
    hourBikeStationDF.persist()
    # Join with minmaxbikeperhrDF & hourBikeStationDF on  hours and bikes number 
    # to get station_id which has minimum number of bikes for the hour 
    minbikeStationidDF = minmaxbikeperhrDF.join(hourBikeStationDF,
                                   [hourBikeStationDF['HOUR'] == minmaxbikeperhrDF['hrs'] ,
                                   hourBikeStationDF['bikes'] == minmaxbikeperhrDF['min_bike']],
                                   "inner")
                                   
    minbikeStationDF = minbikeStationidDF.select(f.col("hrs").alias('min_hrs'),
                                                 f.col('min_bike'),
                                                 f.col('station_id').alias('min_bike_station_id'))


    # Join minmaxbikeperhrDF with hourBikeStationDF to get station id where maximum number of bikes available for the hour
    
    maxbikeStationidDF = minmaxbikeperhrDF.join(hourBikeStationDF,
                                   [hourBikeStationDF['HOUR'] == minmaxbikeperhrDF['hrs'] ,
                                   hourBikeStationDF['bikes'] == minmaxbikeperhrDF['max_bike']],
                                   "inner")
                                   
    maxbikeStationDF = maxbikeStationidDF.select(f.col("hrs").alias('max_hrs'),
                                                 f.col('station_id').alias('max_bike_station_id'),
                                                 f.col('max_bike'))                                   
    
    # Join minbikeStationDF & maxbikeStationDF on max_hrs,min_hrs to join these to dataframe
    # finally will have dataframe with min & max bike count and station id(where min and max bike count reported
    
    combinedDF = minbikeStationDF.join(maxbikeStationDF,
                                       maxbikeStationDF['max_hrs'] == minbikeStationDF['min_hrs'],
                                       "inner")
                                       
    # Final select with min/max bike count and respective station id per hour window
    
    solutionDF = combinedDF.select(f.col("min_hrs").alias("HOUR"),
                                   f.col("min_bike"),
                                   f.col("max_bike"),
                                   f.col("min_bike_station_id"),
                                   f.col("max_bike_station_id"))
    
    # ---------------------------------------

    # Operation A1: 'collect'
    resVAL = solutionDF.collect()
    for item in resVAL:
        print(item)

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
    
    start_time = "07:00:00"
    end_time = "19:00:00"

    my_dataset_dir = "bycleDataSet/"
    
    # 4. We configure the Spark Session
    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    print("\n\n\n")


    # 5. We call to our main function


    my_main(spark,
            my_dataset_dir,
            start_time,
            end_time           
           )




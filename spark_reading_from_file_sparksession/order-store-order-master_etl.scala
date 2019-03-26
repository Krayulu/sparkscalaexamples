import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.types._



val spark = (SparkSession

    .builder

    .appName("Spark Order store event processing")

    .getOrCreate())


var input_path = 'D:\My learnings\PJI\sample\global-order_store_order.csv'

var output_path = 'D:\My learnings\PJI\sample\master-order_store_order.csv'

  
#Reading the input file to a dataframe

val df = spark.read.format("csv").option("header", "true").load(input_path)

 
#Creating a Temp view table which is binded to current spark session

df.createOrReplaceTempView("global_order_store_order_view")
 

#Removing Duplicates based on combination of (event_id,event_type) over event_timestamp and creating 'master' data frame

val master = spark.sql("SELECT insert_timestamp, a.event_id, business_date, a.store_order_number,a.event_type,business_date_order_taken,tax_amount, discount_amount, subtotal_amount,  event_timestamp, makeline_print_date from (SELECT insert_timestamp, event_id, business_date, store_order_number,event_type,business_date_order_taken,tax_amount, discount_amount, subtotal_amount,  event_timestamp, makeline_print_date, ROW_NUMBER() OVER (PARTITION BY event_id, event_type ORDER BY event_timestamp DESC ) AS index from global_order_store_order_view ) a where a.index=1")
 

#Writing output single file instead of creating mutiple out files

master.coalesce(1).write.format('com.databricks.spark.csv').save(output_path,header = 'true')

#either obove line over below line can be used

master.coalesce(1).write.csv(output_path,header = 'true')      
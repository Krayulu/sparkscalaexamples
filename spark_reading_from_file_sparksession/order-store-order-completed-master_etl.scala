import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.types._

  
val spark = (SparkSession

    .builder

    .appName("Spark Order store completed event processing")

    .getOrCreate())
 

var input_path = 'D:\My learnings\PJI\sample\master-order_store_order.csv'

var output_path = 'D:\My learnings\PJI\sample\completed-master-order_store_order\*.csv'
 

#Reading the input file to a dataframe

val df = spark.read.format("csv").option("header", "true").load(input_path)

 
#Creating a Temp view table which is binded to current spark session

df.createOrReplaceTempView("master_order_store_order_view")
 

#Selecting only events whose event_type is completed from 'completed_master' data frame

val completed_master = spark.sql("SELECT a.insert_timestamp,a.event_id,a.business_date,a.store_order_number,a.event_type,a.business_date_order_taken,a.tax_amount,a.discount_amount,a.subtotal_amount,a.event_timestamp,a.makeline_print_date FROM (SELECT * FROM ( SELECT *, ROW_NUMBER() OVER(PARTITION BY store_order_number ORDER BY event_timestamp DESC) rn FROM master_order_store_order_view WHERE event_type LIKE '%Completed%' ) WHERE rn=1)a")

 
#Writing output single file instead of creating mutiple out files

master.coalesce(1).write.format('com.databricks.spark.csv').save(output,header = 'true')

#either obove line over below line can be used

master.coalesce(1).write.csv(output_path,header = 'true')
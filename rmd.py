#        ######################################################################################################################################
#        #PROGRAM_NAME: :       RMD_CF.PY
#        #AUTHOR        :       LAXMI KAKARLAPUDI
#        #DESCRIPTION   :       this will create RMD columns for common flight
#        #User Story    :       US######
#        #DEPENDENCIES:
#        #     --SPARK 2.3
#        #     --HIVE
#        #     --<rdf_published>.<'engine_family'_rmd_flight>
#        #     --<rdf_published>.<'engine_family'_rmd_param_result>
#        #
#        #
#        #
#        #
#        #
###############################################################################################################################################

##############################
# Import python dependencies
##############################
#from pyspark_llap.sql.session import HiveWarehouseSession

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import concat, col, lit, trim
from pyspark.sql.functions import date_format, col, size, radians, length
import math
import datetime
from datetime import datetime, timedelta
from pyspark.sql import  SQLContext, HiveContext
from pyspark.sql.functions import   unix_timestamp
from functools import reduce
from pyspark.sql import DataFrame


###############################
# Init Spark/Hive context vars
###############################
v_sparkAppName = "X1_RMD_COMMON_FLIGHT_STAGE"
spark = SparkSession.builder.config(conf=SparkConf()).appName( v_sparkAppName ).enableHiveSupport().getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
#hive warehouse connector
#hive = HiveWarehouseSession.session(spark).build()

###############################
# Define Dictionaries
dictionary_dfRMD = {}
dictionary_dfPARAM = {}
dictionary_dfJ = {}
dictionary_dfF = {}
my_list = ['cf34','cf6','cfm56','ge90','genx','gp7270','leap']
###############################

########################
# Initialize Variables
########################
v_startDatetime                                         = datetime.now()
v_endDatetime                                           = datetime.now()
v_finalHiveTableSchemaName                              = 'rdf_etl'
v_sourceHiveDataSchemaName                              = 'rdf_published'
v_finalRMDstgTableName                          = 'rmd_stg_cf'
v_extHiveFinalTablePath                         = 'hdfs://NNXPLRPRD/apps/hive/warehouse/' + v_finalHiveTableSchemaName + '.db/external/'



#________________________________________________________________________________
#
#      \\
#  \    \\
#   #####-###)---  processing the rmd logic
#  /    //
#      //
#________________________________________________________________________________

# Print start log message
print ('[{}]: RMD Common Flight'.format( v_startDatetime ))
print ('[{}]: **** START ****'.format( v_startDatetime ))



#function to append all the dataframe using union
def unionAll(*dfs):
    return reduce(DataFrame.union, dfs)

inserted_on_tstamp =  datetime.now()
### RMD FLIGHT logic

for i in my_list:
    dictionary_dfRMD[i] = spark.sql("select distinct coalesce(st_message_seq_id, tk_message_seq_id, cl_message_seq_id, cr1_message_seq_id, pf_message_seq_id) "
                          +"as message_seq_id_rmd , rmd_flight_def_id, serlzd_eng_ser_num, engine_position,airport_depart,airport_dest"
                          +" from rdf_published."+i+"_rmd_flight ")



### RMD PARAM  logic ###

for i in my_list:
    dictionary_dfPARAM[i] = spark.sql("select distinct "
      +"concat(coalesce(src_icao_cd,'z'),coalesce(serlzd_eng_ser_num,'x'),coalesce(engine_position,'y'),coalesce(unix_timestamp(install_datetime),-2))"
      +" as nkey , message_date_time, message_seq_id from rdf_published."+i+"_rmd_param_result "
      +"where report_type in ('Engine Start','TakeOff','Climb','Cruise','PostFlight','Post Flight Summary')")


# JOIN PARAM AND FLIGHT (RMD)

for i in my_list:
    dictionary_dfJ[i] = dictionary_dfRMD[i].join(dictionary_dfPARAM[i], (dictionary_dfRMD[i].message_seq_id_rmd == dictionary_dfPARAM[i].message_seq_id))
                   #     (dictionary_dfRMD[i].engine_position == dictionary_dfPARAM[i].engine_position)& \
                   #     (dictionary_dfRMD[i].serlzd_eng_ser_num == dictionary_dfPARAM[i].serlzd_eng_ser_num)\
     #                  )


# FINAL DATA FRAME

for i in my_list:
    dictionary_dfF[i] = dictionary_dfJ[i].withColumn("engine_family", lit(str((i))))

#union of all dataframes
final_df = unionAll(dictionary_dfF['cf34'], dictionary_dfF['cf6'], dictionary_dfF['cfm56'], dictionary_dfF['ge90'], dictionary_dfF['genx'], dictionary_dfF['gp7270'], dictionary_dfF['leap'])
#final_df.printSchema()
# set final table name and external path
v_finalRMDstgTableFullName = (v_finalHiveTableSchemaName.lower() +'.'+ v_finalRMDstgTableName.lower())
v_finalRMDTablefullpath    = (v_extHiveFinalTablePath + v_finalRMDstgTableName)
print v_finalRMDTablefullpath

print ('[{}]: Creating/Overwiting final Hive table "{}" ...'.format( datetime.now(), v_finalRMDstgTableFullName ))
#save final RMD dataframe as external file
final_df.write.mode("overwrite").parquet( v_finalRMDTablefullpath )


## get column name and data type in strin g varibale
v_dfColWithTypesString = ''
for v_colName, v_colType in final_df.dtypes:

    v_dfColWithTypesString = ( v_dfColWithTypesString + ', ' + v_colName + ' ' + v_colType ).lstrip(',')

# Drop Hive table if it exists

spark.sql( "DROP TABLE IF EXISTS {}".format( v_finalRMDstgTableFullName ))

#Create final RMD table

spark.sql(
                ("""CREATE EXTERNAL TABLE {}
                        ( {} )
                        STORED AS PARQUET
                        LOCATION '{}'
                """).format(
                                                v_finalRMDstgTableFullName,
                                                v_dfColWithTypesString,
                                                v_finalRMDTablefullpath
                                        ))

print ('[{}]: Created table "{}": OK'.format( datetime.now(), v_finalRMDstgTableName ))
v_endDatetime = datetime.now()
# Print total run-time
print ('[{}]: RMD for Common Flight Completed Successfully - Total Run-time: {} '.format( datetime.now(), ( v_endDatetime - v_startDatetime ) ))

print ('[{}]: **** END **** \n\n'.format( datetime.now() ))


#!/user/bin/python
# -*- coding: utf-8 -*-

#        ######################################################################################################################################
#        #PROGRAM_NAME: :       FINAL_NEW.PY
#        #AUTHOR        :       LAXMI KAKARLAPUDI
#        #DESCRIPTION   :       this will create Final columns for common flight 
#        #User Story    :       US######
#        #DEPENDENCIES:
#        #     --SPARK 2.3
#        #     --HIVE
#        #
#        #
#        #
###############################################################################################################################################

##############################
# Import python dependencies
##############################
#from pyspark_llap.sql.session import HiveWarehouseSession

from pyspark import SparkConf
from pyspark.sql import SparkSession, Row
import math
import sys, getopt
import datetime
from datetime import datetime, timedelta
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import concat, col, lit, trim, collect_list, concat_ws,when,split,signum,datediff
import pyspark.sql.functions as F
from pyspark.sql.functions import udf
import sys
import subprocess as sp
from pyspark.sql.functions import date_format, col, size, radians, length, abs,atan2, pow, cos, sin
from pyspark.sql.functions import  from_unixtime,  unix_timestamp
from functools import reduce
from pyspark.sql import DataFrame
#from pyspark.sql.types import *


###############################
# Init Spark/Hive context vars
###############################
v_sparkAppName = "common_flight"
spark = SparkSession.builder.config(conf=SparkConf()).appName( v_sparkAppName ).enableHiveSupport().getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
spark.sql("SET hive.exec.dynamic.partition = true")
spark.sql("SET hive.exec.dynamic.partition.mode = nonstrict ")

###############################
# Define Exceptions
###############################

########################
# Initialize Variables
########################
v_startDatetime                                         = datetime.now()
v_endDatetime                                           = datetime.now()
v_finalHiveTableSchemaName                              = 'rdf_stage_published'
v_sourceHiveDataSchemaName1                              = 'rdf_etl'
v_sourceHiveDataSchemaName2                    = 'rdf_stage'
v_sourceHiveDataSchemaName3                    = 'phm'
v_extHiveFinalTablePath                         = 'hdfs://NNXPLRPRD/apps/hive/warehouse/' + v_finalHiveTableSchemaName + '.db/external/'
v_partitioncol =                                 'best_yearmonth int'


#________________________________________________________________________________
#
#      \\
#  \    \\
#   #####-###)---  processing the CommonFLight logic -- GENX
#  /    //
#      //
#________________________________________________________________________________

# Print start log message
print ('[{}]:  Common Flight'.format( v_startDatetime ))
print ('[{}]: **** START ****'.format( v_startDatetime ))

########### DEFINE VARIABLES ##############
v_yearmonth = spark.sql("select date_format(add_months(current_date,-12),'yyyyMM') as pdate").collect()[0]["pdate"]
inserted_on_tstamp =  datetime.now()

#### TABLES USED ################
#v_enginefam = "GENX"
v_phm = v_sourceHiveDataSchemaName1+'.phm_flight_cf_cdc_stg'
v_aircraft = v_sourceHiveDataSchemaName3+'.aircraft_type'
v_ffd = v_sourceHiveDataSchemaName1+'.ffd_cf_stg'
v_fadenorm = v_sourceHiveDataSchemaName1+'.fa_denorm_cf_stg'
v_env = v_sourceHiveDataSchemaName1+'.common_cf_env_data_daily_table'
v_envver = v_sourceHiveDataSchemaName1+'.common_cf_env_ver_data_daily_table'
v_denorm =  v_sourceHiveDataSchemaName1+'.denorm_cf_stg'
v_fdm = v_sourceHiveDataSchemaName1+'.fdm_cf_stg'
v_phmrmd        = v_sourceHiveDataSchemaName1+'.phm_rmd_cf_stg'
v_metar = v_sourceHiveDataSchemaName1+'.metar_cf_stg'
v_aerosol = v_sourceHiveDataSchemaName1+'.aerosol_cf_stg'


######### Defining udf for partition #####################################
def fx_run_subprocess_cmd( v_args_list ):
	proc = sp.Popen( v_args_list, stdout=sp.PIPE, stderr=sp.PIPE )
	proc.communicate()
	return proc.returncode
#######################################################################################################################
#Define Exceptions
#######################################################################################################################
class Error(Exception):
        '''Base class for other exceptions'''
        pass
class InvalidNumOfArgsError(Error):
        '''Raised when the amount of input arguments doesn't meet set required'''
        pass
class UnknownInputArgsError(Error):
        '''Raised when unknown arguments are passed as input'''
        pass
class UnknownEngineFamilyError(Error):
        '''Raised when an unknown engine family is passed as input'''
        pass
class InvalidShowCountsOptError(Error):
        '''Raised when an invalid showcounts option value is passed as input'''
        pass
#######################################################################################################################
#Validate input Argument
######################################################################################################################
try:
        # Get input arguments from command line where this application is being executed
    opts, unparsed_args = getopt.getopt(sys.argv[1:],'e:s:',['enginefamily=','showcounts='])

        # If more/less than one arguments are being passed in then fail else pass
    if (len( opts ) < 1 or len( opts ) > 1):
        raise InvalidNumOfArgsError
    elif (len( unparsed_args ) > 0):
        raise UnknownInputArgsError
    else:
        for arg, val in opts:

        # Evaluate engine family input argument
            if arg in ('-e', '--enginefamily'):
                if val != '':
                    v_enginefamily = val.strip().upper()

                    # Test engine family value against valid list
                    if v_enginefamily in ('CT7','GENX','GE90','GP7270','CF34','CF6','CFM56','JT','PW','LEAP'):

                        print ('[{}]:  COMMON FLIGHT for {}'.format( datetime.now(), v_enginefamily ))
                    else:
                        raise UnknownEngineFamilyError
                else:
                    v_enginefamily = val

###### --------------- End of input argument validation ----


#
############### 1. PHM AND FFD LOGIC BEGIN ##################################################################################################################
    phm_source = spark.sql("select * from "+v_phm
                      +" where engine_family = '"+v_enginefamily+"'"
                      )

    phm_source.createOrReplaceTempView("phm_source")


## reading phm flight and joining with aircrsft type ##
    phm =  spark.sql(" select  * from phm_source phm"
                +" Left Join " +v_aircraft+" A "
                +" on phm.aircraft_type = A.aircrft_typ_cd"
                )
    phm.createOrReplaceTempView("phm_vw")

    df_phm = spark.sql("select phm_flight_def_id, src_icao_cd as phm_icao_cd , serlzd_eng_ser_num as phm_esn,aircraft_type as phm_aircraft_type, "
                   +" engine_type as phm_engine_type,engine_position as phm_engine_position, "
                   +" upper( regexp_replace( trim(src_tl_num), '[^a-zA-Z0-9]', '')) as phm_tail_number, "
                   +" install_datetime as phm_install_date, removal_datetime as phm_removal_date, flight_start_time as phm_flight_start_time, "
                   +" flight_end_time as phm_flight_end_time,cleansed_airport_depart_icao, "
                   +" cleansed_airport_destination_icao  , cleansed_airport_depart_iata, cleansed_airport_destination_iata,"
                   +" engine_family as phm_engine_family, best_datetime,phm_st_id, phm_st_date_time,  master_flght_id as phm_flight_key_no,"
                   +" no_of_engs as no_of_engs, vld_arfrmr_nm as aircraft_family "
                   +" from phm_vw  ")
    df_phm.createOrReplaceTempView( "df_phm_vw" )

### Reading FFD staging table #####
    df_ffd = spark.sql("select *  from "+v_ffd+" where ffd_engine_family = '"+v_enginefamily+"'"  )


    df_ffd.createOrReplaceTempView( "df_ffd_vw" )

# PHM and FFD stitching
    phm_ffd = spark.sql("Select distinct first(phm_flight_def_id) as phm_flight_def_id,first(phm_icao_cd) as phm_icao_cd, first(phm_esn) as phm_esn, "
                   +" first(phm_aircraft_type) as phm_aircraft_type, first(phm_engine_type) as phm_engine_type, "
                   +" first(phm_engine_position) as phm_engine_position,first(phm_tail_number) as phm_tail_number, "
                   +" first(phm_install_date) as phm_install_date, "
                   +" first(phm_removal_date) as phm_removal_date, first(phm_flight_start_time) as phm_flight_start_time, "
                   +" first(phm_flight_end_time) as phm_flight_end_time, "
                   +" first(cleansed_airport_depart_icao) as cleansed_airport_depart_icao,"
                   +" first(cleansed_airport_destination_icao) as cleansed_airport_destination_icao, "
                   +" first(cleansed_airport_depart_iata) as cleansed_airport_depart_iata,"
                   +" first(cleansed_airport_destination_iata) as cleansed_airport_destination_iata,"
                   +" first(phm_engine_family) as phm_engine_family, first(best_datetime), first(phm_st_id), first( phm_st_date_time), "
                   +" first(phm_flight_key_no) as phm_flight_key_no,first(no_of_engs) as no_of_engs, first(aircraft_family) as aircraft_family,"
                   +" max(adi_flight_record_number) as adi_flight_record_number, max(FFD_ESN) as FFD_ESN, "
                   +" collect_list(adi_flight_record_number) as matched_adi_flight_rec_nos, max(ffd_engine_family) as ffd_engine_family "
                   +" from   df_phm_vw "
                   +" Left outer Join df_FFD_vw "
                   +" on     ( phm_esn = ffd_esn "
                   +"          and phm_engine_position = ffd_engine_position "
                   +"          and phm_tail_number = ffd_tail_number "
                   +"and flight_date_exact between (phm_flight_start_time - Interval '20' MINUTE) and phm_flight_end_time "
                   +"               ) "
                   +"    group by phm_flight_def_id "
                   )
    phm_ffd.createOrReplaceTempView( "phm_ffd_vw" )
    ffd_all_adi = spark.sql( " Select distinct adi_flight_record_number, ffd_esn, ffd_engine_family from df_ffd_vw ")
    phm_ffd_adi = spark.sql( " Select explode(matched_adi_flight_rec_nos), ffd_esn , ffd_engine_family from phm_ffd_vw ")
    ffd_not_mapped_adi = ffd_all_adi.subtract(phm_ffd_adi)
    ffd_not_mapped_adi.createOrReplaceTempView( "ffd_not_mapped_adi_vw" )
    phm_ffd_not_mapped = spark.sql(" Select  null as phm_flight_def_id, null as phm_icao_cd, null as phm_esn, null as phm_aircraft_type, "
                              +" null as phm_engine_type, null as phm_engine_position, null as phm_tail_number, null as phm_install_date,"
                           +" null as phm_removal_date,  null as phm_flight_start_time, null as phm_flight_end_time, "
                           +" null as  cleansed_airport_depart_icao , null as cleansed_airport_destination_icao,"
                           +" null as cleansed_airport_depart_iata, null as cleansed_airport_destination_iata,"
                           +" null as phm_engine_family, null as best_datetime,null as phm_st_id, null as phm_st_date_time, "
                           +" null as phm_flight_key_no,null as no_of_engs, null as aircraft_family,adi_flight_record_number, ffd_esn,"
                           +" collect_list(adi_flight_record_number) as matched_adi_flight_rec_nos, ffd_engine_family"
                           +" from ffd_not_mapped_adi_vw "
                           +" group by adi_flight_record_number, ffd_esn,ffd_engine_family")
    phm_ffd_not_mapped.createOrReplaceTempView( "phm_ffd_not_mapped_vw" )
    phm_ffd_all = phm_ffd_not_mapped.union(phm_ffd)
    phm_ffd_all.createOrReplaceTempView( "phm_ffd_all_vw" )
    phm_ffd_prefinal = spark.sql("select phm_flight_def_id, phm_icao_cd, phm_esn, phm_aircraft_type, phm_engine_type, phm_engine_position, phm_tail_number, "
                        +"phm_install_date, phm_removal_date,phm_flight_start_time, phm_flight_end_time,cleansed_airport_depart_icao,"
                        +" cleansed_airport_destination_icao,cleansed_airport_depart_iata,cleansed_airport_destination_iata,"
                        +"phm_engine_family,best_datetime as best_date_time,phm_st_id, phm_st_date_time, phm_flight_key_no,no_of_engs,aircraft_family,"
                        +"b.flight_key_no,a.adi_flight_record_number,b.FFD_icao_cd,b.FFD_aircraft_type,b.FFD_engine_type, "
                        +" b.flight_date_exact,a.FFD_ESN,b.FFD_engine_position,b.ffd_tail_number, "
                        +"b.FFD_install_date,b.ffd_depart_icao, b.ffd_arrival_icao, matched_adi_flight_rec_nos, b.ffd_engine_family "
                                                +" from phm_ffd_all_vw a "
                                                +"  left join df_ffd_vw b "
                                                +" on a.adi_flight_record_number = b.adi_flight_record_number "
                                                +" and a.ffd_esn = b.ffd_esn "
                                                )

    phm_ffd_prefinal.createOrReplaceTempView( "phm_ffd_prefinal_vw" )

### coalescing columns

    phm_ffd_final = spark.sql("select phm_flight_def_id, CASE when flight_key_no IS NOT NULL then 't' else 'f' end as has_duplicates,"
                          +"  CASE when ffd_esn IS NOT NULL    then 't' else 'f' end as is_overlapping,"
                          +" coalesce(phm_icao_cd,ffd_icao_cd) as phm_icao_cd, "
                          +" coalesce(phm_esn,ffd_esn) as phm_esn, coalesce(phm_aircraft_type,ffd_aircraft_type) as phm_aircraft_type, "
                          +" coalesce(phm_engine_type, ffd_engine_type) as phm_engine_type, "
                          +" coalesce(phm_engine_position, ffd_engine_position) as phm_engine_position,"
                         +" coalesce(phm_tail_number, ffd_tail_number) as phm_tail_number, "
                         +" coalesce(phm_install_date,ffd_install_date) as phm_install_date, phm_removal_date, phm_flight_start_time,phm_flight_end_time,"
                         +" cleansed_airport_depart_icao,cleansed_airport_destination_icao, cleansed_airport_depart_iata, cleansed_airport_destination_iata,"
                         +" phm_engine_family,ffd_engine_family , best_date_time, phm_st_id,phm_st_date_time, "
                         +" coalesce(flight_key_no,phm_flight_key_no) as flight_key_no,no_of_engs,aircraft_family, adi_flight_record_number, flight_date_exact ffd_flight_date_exact, ffd_depart_icao,ffd_arrival_icao,"
                         +" matched_adi_flight_rec_nos "
                         +"  from phm_ffd_prefinal_vw"
                         )

    phm_ffd_final.createOrReplaceTempView( "phm_ffd_final_vw" )




    print(" PHM FFD JOINED ##########")
################ END OF PHM FFD LOGIC #################################################################


######################## 2. PHM_FFD  IS STITCHED TO FA(fa_denorm IS A STAGING TABLE - JOIN OF FA AND DENORM) #############################################




# Reading phm_ffd dataframe created in above step ####
    phm_ffd_new = spark.sql("select *, coalesce(phm_flight_start_time, ffd_flight_date_exact - Interval '25' minutes) as start_dt,"
                   +" coalesce(phm_flight_end_time, ffd_flight_date_exact + Interval '25' minutes) as end_dt,  "
                   +" coalesce(phm_st_date_time,ffd_flight_date_exact) as date1 "
                   +" from phm_ffd_final_vw ")
    phm_ffd_new.createOrReplaceTempView( "phm_ffd_new_vw" )

### Reading fa_denorm staging table #####
    fa_denorm = spark.sql("select *  from "+v_fadenorm +" where fa_engine_family = '"+v_enginefamily+"'" )

    fa_denorm.createOrReplaceTempView( "fa_denorm_vw" )

# PHM,FFD -- FA  stitching
    phm_ffd_fa_all = spark.sql(" select * ,"
          +" row_number() over( partition by phm_flight_def_id order by "
          +" abs(unix_timestamp(date1) - unix_timestamp(faware_dt)), "
         +" signum(unix_timestamp(date1) - unix_timestamp(faware_dt)), "
          +" internal_id "
          +"                 ) as match_number "
          +"  from phm_ffd_new_vw  "
          +" Left outer join "
          +" fa_denorm_vw "
          +"on  phm_tail_number = fa_tail_number "
          +" and (faware_dt  between start_dt and end_dt)"
          +" and phm_esn = FA_esn and phm_engine_position = FA_eng_position "
          )

    phm_ffd_fa_all.createOrReplaceTempView("phm_ffd_fa_all_vw")
### steps to remove duplicate phm_def_id
    phm_ffd_fa_notnull = spark.sql("select * from phm_ffd_fa_all_vw where match_number = 1 and phm_flight_def_id is not null ")
    phm_ffd_fa_notnull.createOrReplaceTempView("phm_ffd_fa_notnull_vw")

    phm_ffd_fa_null = spark.sql("select * from phm_ffd_fa_all_vw where phm_flight_def_id is  null ")
    phm_ffd_fa_null.createOrReplaceTempView("phm_ffd_fa_null_vw")

    phm_ffd_fa_final = phm_ffd_fa_notnull.union(phm_ffd_fa_null)
    phm_ffd_fa_final.createOrReplaceTempView("phm_ffd_fa_final_vw")



############# CLEANING ###############

    phm_ffd_fa_pre = spark.sql("select phm_flight_def_id,has_duplicates,is_overlapping, coalesce(phm_icao_cd,fa_icao_cd) as customer_icao_code, "
                     +" coalesce(phm_esn,fa_esn) as engine_serial_number, coalesce(phm_aircraft_type,FA_aircraft_type) as aircraft_type,"
                     +" phm_engine_type as engine_type, coalesce(phm_engine_position,FA_eng_position) as engine_position,"
                     +" coalesce(phm_tail_number,FA_tail_number) as tail_number, coalesce(phm_install_date,FA_eng_instl_dt) as installation_date,"
                     +" coalesce(phm_removal_date,FA_eng_rmvl_dt) as removal_date,phm_flight_start_time,phm_flight_end_time,"
                     +" cleansed_airport_depart_icao,cleansed_airport_destination_icao,cleansed_airport_depart_iata,cleansed_airport_destination_iata,"
                     +"phm_engine_family, ffd_engine_family,fa_engine_family,best_date_time, phm_st_id, phm_st_date_time,"
                     +" coalesce(no_of_engs,no_of_engines) as no_of_engines,aircraft_family, flight_key_no,adi_flight_record_number,"
                     +" ffd_flight_date_exact,ffd_depart_icao,ffd_arrival_icao, matched_adi_flight_rec_nos, faware_id, "
                     +" Planned_Flight_time_UTC as fa_planned_time_utc, Estimated_Depart_time_UTC as fa_estimated_depart_utc,"
                     +" Estimated_Dest_Time_UTC as fa_estimated_dest_utc,faware_depart,faware_destination, timeline_begin_date, timeline_end_date,"
                     +" engine_family_mdm,engine_model_mdm,engine_series_mdm,esn_sister_array,esn_sister_comment,eng_pos_1_esn,"
                     +" eng_pos_2_esn,eng_pos_3_esn,eng_pos_4_esn  from phm_ffd_fa_final_vw"
                     )
    phm_ffd_fa_pre.createOrReplaceTempView("phm_ffd_fa_pre_vw")
    phm_ffd_fa = spark.sql("select PFF.*, "
                     +" size(matched_adi_flight_rec_nos) as matched_adi_flight_rec_count , "
                     +" Coalesce(cleansed_airport_depart_icao,ffd_depart_icao,faware_depart) as best_depart_icao, "
                     +"       Case when size(matched_adi_flight_rec_nos) > 1 then null "
                     +"            when cleansed_airport_depart_icao is not null then 'PHM' "
                     +"            when ffd_depart_icao is not null then 'FFD' "
                     +"            when faware_depart  is not null then 'Flightaware' "
                     +"            else null "
                     +"       end as Depart_src, "
                     +" Coalesce(cleansed_airport_destination_icao,ffd_arrival_icao,faware_destination) as best_arrival_icao, "
                     +"       Case when size(matched_adi_flight_rec_nos) > 1 then null "
                     +"            when cleansed_airport_destination_icao is not null then 'PHM' "
                     +"            when ffd_arrival_icao is not null then 'FFD' "
                     +"            when faware_destination is not null then 'Flightaware' "
                     +"            else null "
                     +"        end as destination_src, "
                     +" Coalesce(phm_flight_start_time,ffd_flight_date_exact,fa_estimated_depart_utc,fa_planned_time_utc) as best_flight_start_time, "
                     +" Coalesce(best_date_time,ffd_flight_date_exact) as best_datetime,  "
                     +" Coalesce(phm_flight_end_time,fa_estimated_dest_utc) as best_flight_end_time, "
                     +" Coalesce(phm_flight_def_id,flight_key_no,faware_id) as best_id "
                     +" from phm_ffd_fa_pre_vw PFF "
                      )


    phm_ffd_fa.createOrReplaceTempView("phm_ffd_fa_vw")
    print(" PHM FFD FA JOINED $$$$$$$$$$$$ ")

########## ENG OF PHM FFD FA #########################################################################################################################
########################### STEP 3 ################################################################################################################

################# METAR LOGIC BEGIN ############################


    cf_pre = spark.sql("select *, ROW_NUMBER() OVER(order by phm_flight_def_id) as interm_key,"
              +"  coalesce(phm_flight_start_time, ffd_flight_date_exact) as cf_start_time"
      +" ,(coalesce(phm_flight_start_time, ffd_flight_date_exact)  - Interval '5' hours) as start_minus_5 "
      +" ,(coalesce(phm_flight_start_time, ffd_flight_date_exact)  + interval '5' hours) as start_plus_5 "
       +", coalesce(phm_flight_end_time, ffd_flight_date_exact) as cf_end_time "
      +", (coalesce(phm_flight_end_time, ffd_flight_date_exact)  - Interval '5' hours) as end_minus_5"
      +", (coalesce(phm_flight_end_time, ffd_flight_date_exact)  + Interval '5' hours) as end_plus_5"
      +",from_unixtime(unix_timestamp((coalesce(phm_flight_start_time, ffd_flight_date_exact)  - Interval '5' hours)), 'YYYYMM') as start_minus_5m, "
       +" from_unixtime(unix_timestamp((coalesce(phm_flight_start_time, ffd_flight_date_exact)  + interval '5' hours)), 'YYYYMM') as start_plus_5m ,"
       +" from_unixtime(unix_timestamp((coalesce(phm_flight_end_time, ffd_flight_date_exact)  - Interval '5' hours)), 'YYYYMM') as end_minus_5m ,"
       +" from_unixtime(unix_timestamp((coalesce(phm_flight_end_time, ffd_flight_date_exact)  + Interval '5' hours)), 'YYYYMM') as end_plus_5m, "
      +" cleansed_airport_depart_icao as depart_str,cleansed_airport_destination_icao as dest_str "
      +"    from phm_ffd_fa_vw "
                   )

    cf_pre.createOrReplaceTempView( "cf_pre_vw" )
### reading metar staging table ( cleaned columns from metar_airport_weather_data )

    cf_metar_year_month_agg = spark.sql("select * from "+v_metar  )


    cf_metar_year_month_agg.createOrReplaceTempView( "cf_metar_year_month_agg_vw" )



################# DEPART LOGIC ########################
    cf_depart_metar = spark.sql("select A.*, B.metar_arr as depart_metar_arr"
                +" from  cf_pre_vw A"
                +" Left Join  cf_metar_year_month_agg_vw B"
                +" on "
                +" B.airport_str = A.depart_str and "
                +" B.metar_year_month between start_minus_5m and start_plus_5m"
                )
    cf_depart_metar.createOrReplaceTempView( "cf_depart_metar_vw" )

    depart_metar_id = spark.sql(" select first(id) as depart_id, first(depart_interm_key) as depart_interm_key from "
                           +" ( "
                               +"select reading[0] as id, reading[1] as dt,  "
                               +"abs((unix_timestamp(cast(reading[1] as timestamp),'ddMMMyyyy:HH:mm:SS.SSSSSS'))  -  (unix_timestamp(cast(cf_start_time as timestamp),'ddMMMyyyy:HH:mm:SS.SSSSSS')) ) as par1, "
                               +"signum((unix_timestamp(cast(reading[1] as timestamp),'ddMMMyyyy:HH:mm:SS.SSSSSS'))  -  (unix_timestamp(cast(cf_start_time as timestamp),'ddMMMyyyy:HH:mm:SS.SSSSSS')) ) as par2, depart_interm_key"
                                     +" from ("
                                        +"select cf_start_time, split(depart_metar_arr, '@') as reading, depart_interm_key "
                                        +" from ( "
                                          +"select cf_start_time, explode(depart_metar_arr) as depart_metar_arr , interm_key as depart_interm_key"
                                          +" from cf_depart_metar_vw  "
                                        +" ) x "
                                     +" ) Y  order by 3,4 "
                          +" )z  group by depart_interm_key" )


    depart_metar_id.createOrReplaceTempView( "depart_metar_id_vw" )

################# DESTINATION LOGIC ###############################
    cf_dest_metar = spark.sql("select A.*, B.metar_arr as dest_metar_arr"
           #   +"  B.airport_str as depart_str  "
                +" from  cf_pre_vw A"
                +" Left Join  cf_metar_year_month_agg_vw B"
                +" on "
                +" B.airport_str = A.dest_str and "
                +" B.metar_year_month between end_minus_5m and end_plus_5m"
                )
    cf_dest_metar.createOrReplaceTempView( "cf_dest_metar_vw" )

    dest_metar_id = spark.sql(" select first(id) as dest_id, first(dest_interm_key) as dest_interm_key from "
                           +" ( "
                               +"select reading[0] as id, reading[1] as dt,  "
                               +"abs((unix_timestamp(cast(reading[1] as timestamp),'ddMMMyyyy:HH:mm:SS.SSSSSS'))  -  (unix_timestamp(cast(cf_end_time as timestamp),'ddMMMyyyy:HH:mm:SS.SSSSSS')) ) as par1, "
                               +"signum((unix_timestamp(cast(reading[1] as timestamp),'ddMMMyyyy:HH:mm:SS.SSSSSS'))  -  (unix_timestamp(cast(cf_end_time as timestamp),'ddMMMyyyy:HH:mm:SS.SSSSSS')) ) as par2, dest_interm_key"
                                     +" from ("
                                        +"select cf_end_time, split(dest_metar_arr, '@') as reading, dest_interm_key "
                                        +" from ( "
                                          +"select cf_end_time, explode(dest_metar_arr) as dest_metar_arr ,interm_key as dest_interm_key"
                                          +" from cf_dest_metar_vw  "
                                        +" ) x "
                                     +" ) Y  order by 3,4 "
                          +" )z  group by dest_interm_key" )


    dest_metar_id.createOrReplaceTempView( "dest_metar_id_vw" )




### NOW JOIN cf_pre, depart_metar_id, dest_metar_id #######################

    phm_ffd_fa_cm =spark.sql("select C.*, A.depart_id as metar_id_depart, B.dest_id as metar_id_destination "
                                                 +" from cf_pre_vw C "
                                                 +" Left Join depart_metar_id_vw  A "
                                                 +" on  C.interm_key = A.depart_interm_key"
                                                 +" Left Join dest_metar_id_vw B "
                                                 +" on C.interm_key = B.dest_interm_key "
                                                 )



    phm_ffd_fa_cm.createOrReplaceTempView( "phm_ffd_fa_cm_vw")


################ CLEANING COLUMNS ####################
    phm_ffd_fa_cm1 = spark.sql("select * "
                                         +" from phm_ffd_fa_cm_vw").drop('cf_start_time','start_minus_5','start_plus_5','cf_end_time','end_minus_5','end_plus_5','start_minus_5m','start_plus_5m','end_minus_5m','end_plus_5m','interm_key')

#phm_ffd_fa_env_denorm_fdm_rmd_cm1.repartition('tail_number')
    phm_ffd_fa_cm1.createOrReplaceTempView("phm_ffd_fa_cm1_vw")
    print("PHM FFD FA METAR JOINED")
################################# END OF PHM FFA FA METAR LOGIC ######################################################################################

######################################## STEP 4 ###############################################################################################

#GCD logic
    cf_mapping_airport_env_data = spark.sql("Select * from "+v_aerosol  )

#cf_mapping_airport_env_data.repartition('airport_code')
    cf_mapping_airport_env_data.createOrReplaceTempView("cf_mapping_airport_env_data_vw")


    cf_mapping_gcd1 = spark.sql(" select distinct best_depart_icao , best_arrival_icao "
                  +" from phm_ffd_fa_cm1_vw  "
                  +" where best_depart_icao is not NULL   and best_arrival_icao  is not NULL"
                  +" group by best_depart_icao, best_arrival_icao ")
    cf_mapping_gcd1.createOrReplaceTempView("cf_mapping_gcd1_vw")

    cf_mapping_gcd2 = spark.sql("select distinct a.best_depart_icao ,  c.latitude_rad as dep_lat_rad, c.longitude_rad as dept_long_rad, "
                           +" b.best_arrival_icao,  d.latitude_rad as arr_lat_rad, d.longitude_rad as arr_long_rad "
                           +" from cf_mapping_gcd1_vw a, cf_mapping_gcd1_vw b, cf_mapping_airport_env_data_vw c, cf_mapping_airport_env_data_vw d "
                           +" where a.best_depart_icao = c.airport_code "
                           +" and a.best_depart_icao = b.best_depart_icao "
                           +" and a.best_arrival_icao = b.best_arrival_icao "
                           +" and b.best_arrival_icao = d.airport_code "
                           )
#cf_mapping_gcd3.repartition('best_depart_icao','best_arrival_icao1')
    cf_mapping_gcd2.createOrReplaceTempView("cf_mapping_gcd2_vw")

## GCD CALCULATION

    cf_mapping_gcd3 = spark.sql("select *, (3959.0 * atan2\
(            \
        pow( \
                pow( \
                        cos(arr_lat_rad)*sin(abs(dept_long_rad - arr_long_rad))\
                        ,2\
                      ) + \
                pow( \
                        cos(dep_lat_rad)*sin(arr_lat_rad) - \
                       (\
                                sin(dep_lat_rad)*cos(arr_lat_rad)*cos(abs(dept_long_rad - arr_long_rad))\
                        )\
                      ,2\
                      )\
                ,\
                0.5\
              )\
\
\
,\
sin(dep_lat_rad)*sin(arr_lat_rad) + (cos(dep_lat_rad)*cos(arr_lat_rad)*cos(abs(dept_long_rad - arr_long_rad)))))as gcd from cf_mapping_gcd2_vw")
    cf_mapping_gcd3.createOrReplaceTempView("cf_mapping_gcd3_vw")

### JOINING phm_ffd_fa_env_denorm_fdm_rmd_cm_cfstg with gcd_cfstg
    phm_ffd_fa_cm_GCD = spark.sql(" select  N.*, Q.gcd  as great_circle_distance"
                                                +" from  phm_ffd_fa_cm1_vw  N"
                                                +" Left Join cf_mapping_gcd3_vw Q "
                                                +" on N.best_depart_icao = Q.best_depart_icao "
                                                +" and N.best_arrival_icao = Q.best_arrival_icao "
                                                )

    phm_ffd_fa_cm_GCD.createOrReplaceTempView("phm_ffd_fa_cm_GCD_vw")
    print("PHM FFD FA METAR GCD JOINED")

########################## END OF PHM FFD FA METAR GCD LOGIC ########################################################################################

################# step 5  BEGIN OF JOINING PHH FFD FA METAT GCG WITH ENV  #########################

#Reading phm_ffd_fa staging tables

    phm_ffd_fa_cm_GCD1 = spark.sql("select *, cast(coalesce(phm_flight_start_time,ffd_flight_date_exact,fa_estimated_depart_utc) as date) as depart_date,"
                      +" cast(coalesce(phm_flight_end_time,ffd_flight_date_exact,fa_estimated_dest_utc) as date) as dest_date "
                      +" from  phm_ffd_fa_cm_GCD_vw "
                      )

    phm_ffd_fa_cm_GCD1.createOrReplaceTempView("phm_ffd_fa_cm_GCD1_vw")
# Reading env  staging table

    env = spark.sql("select * from "+v_env )
    env.createOrReplaceTempView("env")
    env_distinct = spark.sql("select * from "+v_envver  )
    env_distinct.createOrReplaceTempView("env_distinct")


############### DEPART LOGIC ########################
#joining phm_ffd_fa_denorm with env
    phm_ffd_fa_cm_GCD_env_depart = spark.sql("select A.*,env_6x_id as airport_depart_env_6x_id1, env_5x_id as airport_depart_env_5x_id1,"
                      +" env_4x_id as airport_depart_env_4x_id1, "
                      +" env_airport_id as environment_id_depart1"
                      +" from phm_ffd_fa_cm_GCD1_vw A "
                      +" Left Join env  "
                      +" on     best_depart_icao == env.icao_code "
                      +" and  depart_date  == env.nrl_date_year"
                      +" and    length(best_depart_icao) == 4 ")
    phm_ffd_fa_cm_GCD_env_depart.createOrReplaceTempView( "phm_ffd_fa_cm_GCD_env_depart_vw")

########### GET RECORDS WHERE ENV_AIRPORT_ID IS NOT NULL, IMPLIES V6 ID'S ARE FOUND ################
    phm_ffd_fa_cm_GCD_env_depart_v6 = spark.sql("select * from phm_ffd_fa_cm_GCD_env_depart_vw where environment_id_depart1 is not null")
    phm_ffd_fa_cm_GCD_env_depart_v6.createOrReplaceTempView("phm_ffd_fa_cm_GCD_env_depart_v6_vw")

### GET RECORDS WHERE ENV_AIRPORT_ID IS NULL, THEN WE NEED TO GET V4 AND V5 ENV ID'S ####################

    phm_ffd_fa_cm_GCD_env_depart_null = spark.sql(" select * from phm_ffd_fa_cm_GCD_env_depart_vw where environment_id_depart1 is null")
    phm_ffd_fa_cm_GCD_env_depart_null.createOrReplaceTempView( "phm_ffd_fa_cm_GCD_env_depart_null_vw")

    phm_ffd_fa_cm_GCD_env_depart_v4v5 = spark.sql(" select A.*,B.env_6x_id as airport_depart_env_6x_id2, B.env_5x_id as airport_depart_env_5x_id2,"
                      +"B.env_4x_id as airport_depart_env_4x_id2,B.env_airport_id as environment_id_depart2"
                                +" from phm_ffd_fa_cm_GCD_env_depart_null_vw A "
                                +" Left Join env_distinct B"
                                +" on  best_depart_icao == B.icao_code "
                                )
    phm_ffd_fa_cm_GCD_env_depart_v4v5.createOrReplaceTempView( "phm_ffd_fa_cm_GCD_env_depart_v4v5_vw")
## NOW CLEAN COLUMNS FROM ENV ###

    phm_ffd_fa_cm_GCD_env_depart_pre = spark.sql("select phm_flight_def_id,has_duplicates,is_overlapping,  customer_icao_code,  engine_serial_number,  aircraft_type,  engine_type,  engine_position,"
                           +"tail_number,  installation_date,  removal_date,  phm_flight_start_time,  phm_flight_end_time,  cleansed_airport_depart_icao,"
                           +"cleansed_airport_destination_icao,  cleansed_airport_depart_iata,  cleansed_airport_destination_iata,  phm_engine_family,"
                     +"ffd_engine_family, fa_engine_family, "
                     +"best_date_time,  phm_st_id,  phm_st_date_time,no_of_engines, aircraft_family,  flight_key_no,  adi_flight_record_number,"
                     +"  ffd_flight_date_exact,ffd_depart_icao,"
      +"ffd_arrival_icao,  matched_adi_flight_rec_nos,  faware_id,  fa_planned_time_utc,  fa_estimated_depart_utc,  fa_estimated_dest_utc,"
      +"faware_depart,  faware_destination,  timeline_begin_date,  timeline_end_date,  engine_family_mdm,  engine_model_mdm,  engine_series_mdm,"
      +"esn_sister_array,  esn_sister_comment, eng_pos_1_esn, eng_pos_2_esn, eng_pos_3_esn, eng_pos_4_esn,"
      +" matched_adi_flight_rec_count,  best_depart_icao,  Depart_src,  best_arrival_icao,  destination_src,"
       +"best_flight_start_time,  best_datetime,  best_flight_end_time,  best_id, "
       +"depart_str,dest_str,metar_id_depart, metar_id_destination,great_circle_distance,"
       +" depart_date,  dest_date,"
       +"coalesce (airport_depart_env_6x_id1,airport_depart_env_6x_id2) as airport_depart_env_6x_id, "
       +"coalesce(airport_depart_env_5x_id1,airport_depart_env_5x_id2) as airport_depart_env_5x_id, "
       +"coalesce(airport_depart_env_4x_id1, airport_depart_env_4x_id2) as airport_depart_env_4x_id, "
       +"coalesce(environment_id_depart1, environment_id_depart2) as environment_id_depart"
       +" from phm_ffd_fa_cm_GCD_env_depart_v4v5_vw ")

    phm_ffd_fa_cm_GCD_env_depart_pre.createOrReplaceTempView( "phm_ffd_fa_cm_GCD_env_depart_pre_vw")
#


### NOW COMBINE V6, V5 AND V4 RECORDS ##################
    phm_ffd_fa_cm_GCD_env_depart_final = phm_ffd_fa_cm_GCD_env_depart_pre.union(phm_ffd_fa_cm_GCD_env_depart_v6)
    phm_ffd_fa_cm_GCD_env_depart_final.createOrReplaceTempView( "phm_ffd_fa_cm_GCD_env_depart_final_vw")



####################### DESTINATION LOGIC ###########################

    phm_ffd_fa_cm_GCD_env_dest = spark.sql("select A.*,env_6x_id as airport_destination_env_6x_id1, env_5x_id as airport_destination_env_5x_id1,"
                      +" env_4x_id as airport_destination_env_4x_id1, "
                      +" env_airport_id as environment_id_destination1"
                      +" from phm_ffd_fa_cm_GCD_env_depart_final_vw A "
                      +" Left Join env  "
                      +" on     best_arrival_icao == env.icao_code "
                      +" and  dest_date  == env.nrl_date_year"
                      +" and    length(best_arrival_icao) == 4 ")
    phm_ffd_fa_cm_GCD_env_dest.createOrReplaceTempView( "phm_ffd_fa_cm_GCD_env_dest_vw")
##
########### GET RECORDS WHERE ENV_AIRPORT_ID IS NOT NULL, IMPLIES V6 ID'S ARE FOUND ################
    phm_ffd_fa_cm_GCD_env_dest_v6 = spark.sql("select * from phm_ffd_fa_cm_GCD_env_dest_vw where environment_id_destination1 is not null")
    phm_ffd_fa_cm_GCD_env_dest_v6.createOrReplaceTempView("phm_ffd_fa_cm_GCD_env_dest_v6_vw")
###
### GET RECORDS WHERE ENV_AIRPORT_ID IS NULL, THEN WE NEED TO GET V4 AND V5 ENV ID'S ####################

    phm_ffd_fa_cm_GCD_env_dest_null = spark.sql(" select * from phm_ffd_fa_cm_GCD_env_dest_vw where environment_id_destination1 is null")
    phm_ffd_fa_cm_GCD_env_dest_null.createOrReplaceTempView( "phm_ffd_fa_cm_GCD_env_dest_null_vw")

    phm_ffd_fa_cm_GCD_env_dest_v4v5 = spark.sql(" select A.*,B.env_6x_id as airport_destination_env_6x_id2, B.env_5x_id as airport_destination_env_5x_id2,"
                      +"B.env_4x_id as airport_destination_env_4x_id2,B.env_airport_id as environment_id_destination2"
                                +" from phm_ffd_fa_cm_GCD_env_dest_null_vw A "
                                +" Left Join env_distinct B"
                                +" on  best_arrival_icao == B.icao_code "
                               )
    phm_ffd_fa_cm_GCD_env_dest_v4v5.createOrReplaceTempView( "phm_ffd_fa_cm_GCD_env_dest_v4v5_vw")
## NOW CLEAN COLUMNS FROM ENV ###

    phm_ffd_fa_cm_GCD_env_dest_pre = spark.sql("select phm_flight_def_id, has_duplicates,is_overlapping, customer_icao_code,  engine_serial_number,  aircraft_type,  engine_type,  engine_position,"
      +"tail_number,  installation_date,  removal_date,  phm_flight_start_time,  phm_flight_end_time,  cleansed_airport_depart_icao,"
      +"cleansed_airport_destination_icao,  cleansed_airport_depart_iata,  cleansed_airport_destination_iata,  phm_engine_family,ffd_engine_family,fa_engine_family,"
      +"best_date_time,  phm_st_id,  phm_st_date_time,no_of_engines, aircraft_family,  flight_key_no,  adi_flight_record_number,  ffd_flight_date_exact,ffd_depart_icao,"
      +"ffd_arrival_icao,  matched_adi_flight_rec_nos,  faware_id,  fa_planned_time_utc,  fa_estimated_depart_utc,  fa_estimated_dest_utc,"
      +"faware_depart,  faware_destination,  timeline_begin_date,  timeline_end_date,  engine_family_mdm,  engine_model_mdm,  engine_series_mdm,"
      +"esn_sister_array,  esn_sister_comment,  eng_pos_1_esn, eng_pos_2_esn, eng_pos_3_esn, eng_pos_4_esn,"
      +"matched_adi_flight_rec_count,  best_depart_icao,  Depart_src,  best_arrival_icao,  destination_src,"
      +"best_flight_start_time,  best_datetime,  best_flight_end_time,  best_id,  "
      +"depart_str,dest_str,metar_id_depart, metar_id_destination,great_circle_distance,"
      +"depart_date,  dest_date,"
      +" airport_depart_env_6x_id, airport_depart_env_5x_id, airport_depart_env_4x_id,environment_id_depart, "
      +"coalesce (airport_destination_env_6x_id1,airport_destination_env_6x_id2) as airport_destination_env_6x_id, "
      +"coalesce(airport_destination_env_5x_id1,airport_destination_env_5x_id2) as airport_destination_env_5x_id, "
      +"coalesce(airport_destination_env_4x_id1, airport_destination_env_4x_id2) as airport_destination_env_4x_id, "
      +"coalesce(environment_id_destination1, environment_id_destination2) as environment_id_destination"
      +" from phm_ffd_fa_cm_GCD_env_dest_v4v5_vw ")

    phm_ffd_fa_cm_GCD_env_dest_pre.createOrReplaceTempView( "phm_ffd_fa_cm_GCD_env_dest_pre_vw")
    
#### NOW COMBINE V6, V5 AND V4 RECORDS ##################
    phm_ffd_fa_cm_GCD_env_dest_final = phm_ffd_fa_cm_GCD_env_dest_pre.union(phm_ffd_fa_cm_GCD_env_dest_v6)
    phm_ffd_fa_cm_GCD_env_dest_final.createOrReplaceTempView( "phm_ffd_fa_cm_GCD_env_dest_final_vw")

    print("PHM FFD FA METAR GCD ENV JOINED")

###################### END OF PHM FFD FA METAR GCD ENV LOGIC ########################################################################################
####################### STEP 6: BEGIN OF JOINING WITH DENORM ###################################################################
# READING WITH DENORM

    denorm = spark.sql(" select esn, denorm_tail_number, timeline_begin_date, timeline_end_date,"
               +" engine_family_mdm, engine_model_mdm, engine_series_mdm,eng_pos_1_esn, eng_pos_2_esn,eng_pos_3_esn, eng_pos_4_esn, "
               +" esn_sister_array, esn_sister_comment,engine_family_denorm "
               +" from "+v_denorm+" where engine_family_denorm = '"+v_enginefamily+"'"
              )

    denorm.createOrReplaceTempView("denorm_vw")


####################### JOINGING PHM FFA FA ENV  WITH DENORM ###########################
    phm_ffd_fa_cm_GCD_env_denorm1 = spark.sql(" select A.*,B.engine_model_mdm as engine_model_denorm,  B.engine_family_denorm,"
                  +" B.engine_series_mdm as engine_series_denorm, B.esn_sister_array as esn_sister_array_denorm, "
                  +" B.esn_sister_comment as esn_sister_comment_denorm,  B.eng_pos_1_esn as eng_pos_1_esn_denorm, B.eng_pos_2_esn as eng_pos_2_esn_denorm,"
                  +" B.eng_pos_3_esn as eng_pos_3_esn_denorm, B.eng_pos_4_esn as eng_pos_4_esn_denorm "
                  +" from phm_ffd_fa_cm_GCD_env_dest_final_vw A"
                  +"  left join denorm_vw B on"
                   +" A.tail_number = B.denorm_tail_number"
                  +" and A.best_date_time between B.timeline_begin_date and B.timeline_end_date"
                  +" and a.engine_serial_number = b.esn"
                  )
    phm_ffd_fa_cm_GCD_env_denorm1.createOrReplaceTempView("phm_ffd_fa_cm_GCD_env_denorm1_vw")




################ CLEANING EXTRA COLUMNS ###########################
    phm_ffd_fa_cm_GCD_env_denorm = spark.sql("select phm_flight_def_id ,has_duplicates, is_overlapping,customer_icao_code ,engine_serial_number,aircraft_type,engine_type,engine_position,"
                              +"tail_number,"
                              +"installation_date ,removal_date ,phm_flight_start_time ,phm_flight_end_time,cleansed_airport_depart_icao,"
                              +"cleansed_airport_destination_icao,cleansed_airport_depart_iata,cleansed_airport_destination_iata,phm_engine_family,"
                              +"ffd_engine_family,fa_engine_family,"
                              +"phm_st_id,phm_st_date_time,no_of_engines,aircraft_family,flight_key_no,adi_flight_record_number,ffd_flight_date_exact,"
                              +"ffd_depart_icao,"
                              +"ffd_arrival_icao,matched_adi_flight_rec_nos,faware_id,fa_planned_time_utc,fa_estimated_depart_utc,"
                              +"fa_estimated_dest_utc,faware_depart,faware_destination,"
                              +"matched_adi_flight_rec_count ,best_depart_icao,depart_src , best_arrival_icao ,destination_src ,best_flight_start_time ,"
                              +" best_datetime , best_flight_end_time ,best_id ,environment_id_depart,airport_depart_env_6x_id,airport_depart_env_5x_id,"
                              +"airport_depart_env_4x_id ,environment_id_destination,metar_id_depart, metar_id_destination,great_circle_distance,"
                              +" airport_destination_env_6x_id, airport_destination_env_5x_id ,airport_destination_env_4x_id, "
                              +" engine_family_mdm,engine_family_denorm,"
                              +" coalesce(engine_model_mdm,engine_model_denorm) as engine_model_mdm,"
                              +" coalesce(engine_series_mdm ,engine_series_denorm) as engine_series_mdm,"
                              +" coalesce(esn_sister_array,esn_sister_array_denorm) as esn_sister_array,"
                              +" coalesce(esn_sister_comment, esn_sister_comment_denorm) as esn_sister_comment,"
                              +" coalesce(eng_pos_1_esn,eng_pos_1_esn_denorm) as eng_pos_1_esn, "
                              +" coalesce(eng_pos_2_esn, eng_pos_2_esn_denorm) as eng_pos_2_esn, "
                              +" coalesce(eng_pos_3_esn, eng_pos_3_esn_denorm) as eng_pos_3_esn, "
                              +" coalesce(eng_pos_4_esn, eng_pos_4_esn_denorm) as eng_pos_4_esn "
                              +" from phm_ffd_fa_cm_GCD_env_denorm1_vw"
                              )

    phm_ffd_fa_cm_GCD_env_denorm.createOrReplaceTempView("phm_ffd_fa_cm_GCD_env_denorm_vw")

    print("PHM FFD FA METAR GCD ENV DENORM JOINED !!!!!!!!!") 

############################### END OF PHM FFD FA METAR GCD ENV DENORM LOGIC #########################################################################
################# STEP 7   SISTER ENGINE LOGIC  ########################################################################################################



    sis_eng_prep= spark.sql("select * from  phm_ffd_fa_cm_GCD_env_denorm_vw "
              +" order by tail_number, best_datetime, engine_position"
               )
    sis_eng_prep.createOrReplaceTempView("sis_eng_prep")
    sis_eng_lag = spark.sql("select *, "
                +"lag(best_datetime) over(PARTITION BY tail_number order by tail_number , best_datetime, engine_position ) as prev_best, "
                +"  lag(best_flight_start_time) over(PARTITION BY tail_number order by tail_number , best_datetime,engine_position ) as prev_start, "
                +"  lag(best_flight_end_time) over(PARTITION BY tail_number order by tail_number , best_datetime,engine_position ) as prev_end "
                +" from sis_eng_prep "
               +" order by tail_number, best_datetime, engine_position "
               )
    sis_eng_lag.createOrReplaceTempView("sis_eng_lag")
    sis_eng_pc = spark.sql("select *, case when"
                +"      best_datetime between coalesce(prev_start,prev_best) and coalesce(prev_end,prev_best) "
                +" or best_datetime = prev_best"
                 +" then ' ' else '#' "
                            +" end as parent_child "
                            +" from sis_eng_lag ")
    sis_eng_pc.createOrReplaceTempView("sis_eng_pc")
    sis_eng_p = spark.sql("select *, concat(tail_number, best_datetime) as key"
                 +" from sis_eng_pc"
                 +" where parent_child = '#' "
               )
    sis_eng_p.createOrReplaceTempView("sis_eng_p")

    sis_eng_p_new = sis_eng_p.toDF(*(c.replace('_', '__') for c in sis_eng_p.columns))

    sis_eng_p_new.createOrReplaceTempView("sis_eng_p_new")


    sis_eng_c = spark.sql(" select * from sis_eng_pc where parent_child != '#' "
               )
    sis_eng_c.createOrReplaceTempView("sis_eng_c")

    sis_eng_pc_join = spark.sql(" select b.*, a.key "
               +" from sis_eng_c b "
               +" left join sis_eng_p_new a"
               +" on b.tail_number = a.tail__number"
               +" and b.best_flight_start_time between a.best__flight__start__time and a.best__flight__end__time"
               )
    sis_eng_pc_join.createOrReplaceTempView("sis_eng_pc_join")

    sis_eng_p_new1 = sis_eng_pc_join.toDF(*(c.replace('__', '_') for c in sis_eng_pc_join.columns))

    sis_eng_p_new1.createOrReplaceTempView("sis_eng_p_new1")
    sis_eng_pc_union = spark.sql(" select * from sis_eng_p_new1 union select * from sis_eng_pc_join"
                +" order by tail_number, best_datetime, engine_position ")
    sis_eng_pc_union.createOrReplaceTempView("sis_eng_pc_union")
    sis_eng_defid = spark.sql(" select tail_number, key as parent_key , collect_list(phm_flight_def_id) as def_id_array"
                +" from sis_eng_pc_union "
                +" group by tail_number, key "
                  )
    sis_eng_defid.createOrReplaceTempView("sis_eng_defid")
    sis_eng_defid_new = sis_eng_defid.toDF(*(c.replace('_', '__') for c in sis_eng_defid.columns))
    sis_eng_defid_new.createOrReplaceTempView("sis_eng_defid_new")
    sis_eng_defid_arr  = spark.sql(" select A.*, B.def__id__array as phm_def_id_array from "
               +" sis_eng_pc_union  A "
               +" left join sis_eng_defid_new  B "
               +" on  A.tail_number = B.tail__number "
               +" and A.key = B.parent__key "
               +"  order by tail_number, best_datetime, engine_position "
               )
    sis_eng_defid_arr.createOrReplaceTempView("sis_eng_defid_arr")
    sis_eng_defid_splt = sis_eng_defid_arr.withColumn("def_id_1", sis_eng_defid_arr["phm_def_id_array"].getItem(0)).withColumn("def_id_2", sis_eng_defid_arr["phm_def_id_array"].getItem(1)).withColumn("def_id_3", sis_eng_defid_arr["phm_def_id_array"].getItem(2)).withColumn("def_id_4", sis_eng_defid_arr["phm_def_id_array"].getItem(3))
    sis_eng_defid_splt.createOrReplaceTempView("sis_eng_defid_splt")
    sis_eng = spark.sql(" select *, "
               +" case when eng_pos_1_esn is null then  'no_def_id' "
               +" else "
               +"     def_id_1 "
              +" end as phm_def_id_eng_pos_1, "
               +" case when eng_pos_2_esn is null then 'no_def_id' "
               +"     when  eng_pos_2_esn is not null and eng_pos_1_esn is not null then  def_id_2 "
               +"     when eng_pos_1_esn is null and eng_pos_2_esn is not null then def_id_1 "
               +" end as phm_def_id_eng_pos_2, "
               +" case when eng_pos_3_esn is null then 'no_def_id' "
               +"      when eng_pos_3_esn is not null and eng_pos_1_esn is not null and eng_pos_2_esn is not null then def_id_3 "
               +"      when eng_pos_1_esn is null and eng_pos_2_esn is not null then def_id_2 "
               +"      when eng_pos_1_esn is not null and eng_pos_2_esn is null then def_id_2 "
               +"      when eng_pos_1_esn is null and eng_pos_2_esn is null then def_id_1"
               +"      when eng_pos_1_esn is not null and eng_pos_2_esn is not null and eng_pos_3_esn is not null and eng_pos_4_esn is null then def_id_3"
               +"      when eng_pos_1_esn is not null and eng_pos_2_esn is null and eng_pos_3_esn is not null and eng_pos_4_esn is not null then def_id_2"
               +"      when eng_pos_1_esn is null and eng_pos_2_esn is not null and eng_pos_3_esn is not null and eng_pos_4_esn is not null then def_id_2"
               +" end as phm_def_id_eng_pos_3, "
               +" case when eng_pos_4_esn is null then 'no_def_id' "
               +"      when eng_pos_4_esn is not null and eng_pos_1_esn is not null and eng_pos_2_esn is not null and eng_pos_3_esn is not null then  def_id_4 "
               +"      when eng_pos_1_esn is null and eng_pos_2_esn is not null and eng_pos_3_esn is null then def_id_2"
               +"      when eng_pos_1_esn is not null and eng_pos_2_esn is null and eng_pos_3_esn is null then def_id_2"
               +"      when eng_pos_1_esn is null and eng_pos_2_esn is null and eng_pos_3_esn is not null then def_id_2"
               +"      when eng_pos_1_esn is not null and eng_pos_2_esn is not null and eng_pos_3_esn is null then def_id_3"
               +"      when eng_pos_1_esn is not null and eng_pos_2_esn is null and eng_pos_3_esn is not null then def_id_3"
               +"      when eng_pos_1_esn is null and eng_pos_2_esn is not null and eng_pos_3_esn is not null then def_id_3"
               +" end as phm_def_id_eng_pos_4 "
               +" from sis_eng_defid_splt"
          )
    sis_eng.createOrReplaceTempView("sis_eng")

#################################### SISTER ENGINE LOGIC END #############################################################

####################### STEP 8: BEGIN  OF FDM LOGIC #####################################
    phm_df_temp = spark.sql("select * from phm_ffd_fa_cm_GCD_env_dest_final_vw ")
    phm_df_temp.createOrReplaceTempView("phm_df_temp_vw")
##print('PHM FFD FA ENV TEMP TABLE READ SUCCESSFULLY')
#
## READING FDM STAGING TABLE
    fdm_df = spark.sql("select *  from "+v_fdm  )
    fdm_df.createOrReplaceTempView("fdm_df_vw")


    phm_fdm = spark.sql(" Select phm.best_id,phm.engine_serial_number,phm.best_flight_start_time, "
                        +" max(event_date) fdm_event_date,max(current_removal_count) fdm_removal_count, max(current_shop_visit_count) fdm_shop_visit_count "
                        +" from phm_df_temp_vw phm "
                        +" Left Join fdm_df_vw fdm "
                        +" on     phm.engine_serial_number = fdm.serialized_engine_id"
                        +" and    phm.best_flight_start_time > fdm.event_date "
                        +" and    phm.best_flight_start_time IS NOT NULL"
                        +" group by best_id,phm.engine_serial_number,best_flight_start_time ")



    phm_fdm.createOrReplaceTempView("phm_fdm_vw")

    phm_ffd_fa_cm_GCD_env_denorm_fdm_pre = spark.sql(" select sis.*, PF.fdm_event_date,fdm_removal_count, fdm_shop_visit_count"
                  +"  from sis_eng sis "
                  +"  left join phm_fdm_vw PF  on"
                   +" sis.engine_serial_number = PF.engine_serial_number"
                   +"  and sis.best_id =  PF.best_id"
                   +" and sis.best_flight_start_time = PF.best_flight_start_time" )

    phm_ffd_fa_cm_GCD_env_denorm_fdm_pre.createOrReplaceTempView("phm_ffd_fa_cm_GCD_env_denorm_fdm_pre_vw")

    print(" PHM FFD FA METAR GCD ENV DENORM FDM JOINED ::::;")


################## CLEANING COLUMNS ###########################
    phm_ffd_fa_cm_GCD_env_denorm_fdm = spark.sql(" select "
                     +"phm_flight_def_id ,has_duplicates ,is_overlapping,customer_icao_code,engine_serial_number,aircraft_type,engine_type,"
                     +"engine_position,tail_number,installation_date,removal_date,phm_flight_start_time,phm_flight_end_time,"
                     +"cleansed_airport_depart_icao,cleansed_airport_destination_icao,cleansed_airport_depart_iata,cleansed_airport_destination_iata,"
                     +"phm_engine_family,ffd_engine_family,fa_engine_family,phm_st_id,phm_st_date_time,no_of_engines,aircraft_family,flight_key_no,"
                     +"adi_flight_record_number,ffd_flight_date_exact,ffd_depart_icao,ffd_arrival_icao,matched_adi_flight_rec_nos,faware_id,"
                     +"fa_planned_time_utc,fa_estimated_depart_utc,fa_estimated_dest_utc,faware_depart,faware_destination,matched_adi_flight_rec_count,best_depart_icao,"
                     +"depart_src,best_arrival_icao,destination_src,best_flight_start_time,best_datetime,best_flight_end_time,best_id,"
                     +"environment_id_depart,airport_depart_env_6x_id,airport_depart_env_5x_id,airport_depart_env_4x_id,"
                     +"environment_id_destination,airport_destination_env_6x_id,airport_destination_env_5x_id,airport_destination_env_4x_id,"
                     +"metar_id_depart,metar_id_destination,great_circle_distance,"
                     +"engine_family_mdm,engine_model_mdm,engine_series_mdm,esn_sister_array,esn_sister_comment,engine_family_denorm,"
                     +"eng_pos_1_esn,eng_pos_2_esn,eng_pos_3_esn,eng_pos_4_esn,"
                     +"phm_def_id_eng_pos_1,phm_def_id_eng_pos_2,phm_def_id_eng_pos_3,phm_def_id_eng_pos_4,"
                     +"concat_ws(',',phm_def_id_eng_pos_1,phm_def_id_eng_pos_2,phm_def_id_eng_pos_3,phm_def_id_eng_pos_4) as phm_def_id_eng_pos_array,"
                     +"fdm_event_date,fdm_removal_count,fdm_shop_visit_count"
                     +" from phm_ffd_fa_cm_GCD_env_denorm_fdm_pre_vw "
                     )

    phm_ffd_fa_cm_GCD_env_denorm_fdm.createOrReplaceTempView("phm_ffd_fa_cm_GCD_env_denorm_fdm_vw")


    print(" PHM FFD FA CM GCD ENV DENORM FDM JOINED ::::;")
############################ END OF FDM LOGIC ##########################################################################################################
###################### STEP 9 BEGIN OF RMD LOGIC  #####################

######### joinging phm ffd fa env denorm fdm with rmd( have to run phm_rmd.py script to read phm_rmd_stg_cf)  ##########################################


# reading phm rmd staging table ####
    phm_rmd = spark.sql("select phm_flight_def_id  as phm_def_id ,sedi_etsn ,sedi_ecsn, sedi_etsv ,sedi_ecsv, sedi_etsi, sedi_ecsi,"
                +"es_rprt_id_eng_pos_1,es_rprt_id_eng_pos_2,es_rprt_id_eng_pos_3,es_rprt_id_eng_pos_4,es_rprt_id_eng_pos_array,"
                +" to_rprt_id_eng_pos_1 ,to_rprt_id_eng_pos_2,to_rprt_id_eng_pos_3,to_rprt_id_eng_pos_4,to_rprt_id_eng_pos_array,"
                +" cl_rprt_id_eng_pos_1,cl_rprt_id_eng_pos_2,cl_rprt_id_eng_pos_3,cl_rprt_id_eng_pos_4 ,cl_rprt_id_eng_pos_array,"
                +"cr1_rprt_id_eng_pos_1 ,cr1_rprt_id_eng_pos_2 ,cr1_rprt_id_eng_pos_3 ,cr1_rprt_id_eng_pos_4 ,cr1_rprt_id_eng_pos_array, "
                +"pf_rprt_id_eng_pos_1,pf_rprt_id_eng_pos_2,pf_rprt_id_eng_pos_3,pf_rprt_id_eng_pos_4 ,pf_rprt_id_eng_pos_array,"
                +" sp_rprt_id_eng_pos_1,sp_rprt_id_eng_pos_2, sp_rprt_id_eng_pos_3 ,sp_rprt_id_eng_pos_4,sp_rprt_id_eng_pos_array,"
                +" rmd_flight_def_id,airport_depart as airport_depart_rmd, airport_dest, dlgs_es_ct_code, dlgs_to_ct_code from "+v_phmrmd   )

    phm_rmd.createOrReplaceTempView("phm_rmd_vw")


#JOINING WITH FDM
    phm_ffd_fa_cm_GCD_env_denorm_fdm_rmd = spark.sql(" select *, coalesce(best_depart_icao, airport_depart_rmd) as airport_depart,"
                                         +" coalesce(best_arrival_icao, airport_dest) as airport_destination "
                                         +" from phm_ffd_fa_cm_GCD_env_denorm_fdm_vw A  left join phm_rmd_vw B  on "
                                         +" A.phm_flight_def_id = B.phm_def_id ").drop("phm_def_id","airport_depart_rmd","airport_dest")

    phm_ffd_fa_cm_GCD_env_denorm_fdm_rmd.createOrReplaceTempView("phm_ffd_fa_cm_GCD_env_denorm_fdm_rmd_vw")
#print("PHM FFD FA ENV DENORM FDM RMD JOINED :")

################ CLEANING COLUMNS ####################
    phm_ffd_fa_cm_GCD_env_denorm_fdm_rmd1 = spark.sql("select * "
                                         +" from phm_ffd_fa_cm_GCD_env_denorm_fdm_rmd_vw").drop('cf_start_time','start_minus_5','start_plus_5','cf_end_time','end_minus_5','end_plus_5','start_minus_5m','start_plus_5m','end_minus_5m','end_plus_5m','interm_key')

#phm_ffd_fa_env_denorm_fdm_rmd_cm1.repartition('tail_number')
    phm_ffd_fa_cm_GCD_env_denorm_fdm_rmd1.createOrReplaceTempView("phm_ffd_fa_cm_GCD_env_denorm_fdm_rmd1_vw")
#
    final_temp = spark.sql("select *, coalesce(phm_engine_family, ffd_engine_family, fa_engine_family, engine_family_denorm) as engine_family,"
                    +" coalesce(fa_estimated_depart_UTC, fa_estimated_dest_UTC, fa_planned_time_UTC) as fa_date "
                    +" from   phm_ffd_fa_cm_GCD_env_denorm_fdm_rmd1_vw"
                    ).drop('phm_engine_family','ffd_engine_family','fa_engine_family','engine_family_denorm')
    final_temp.createOrReplaceTempView("final_temp_vw")

######################## END OF RMD LOGIC ######################################################
##### getting columns engine_position_description, invalid and missing columns #####
    final_cf = spark.sql("select * ,"
       +" concat_ws('-',coalesce(phm_flight_def_id,000),coalesce(engine_serial_number,'xxx')) as ID,"
       +" concat_ws('_',engine_serial_number,date_format(installation_date,'YYYYMMdd')) as enginefitstring, "
       +" case when no_of_engines = 2 then (case when engine_position = 1 then 'LI'        "
       +"                                    when engine_position = 2 then 'RI' end)   "
       +"     when no_of_engines = 3 then (case when engine_position = 1 then 'LI'        "
       +"                                    when engine_position = 2 then 'C'         "
       +"                                    when engine_position = 3 then 'RI' end)   "
       +"     when no_of_engines = 4 then (case when engine_position = 1 then 'LO'        "
       +"                                    when engine_position = 2 then 'LI'        "
       +"                                    when engine_position = 3 then 'RI'        "
       +"                                    when engine_position = 4 then 'RO' end)   "
       +"end as engine_position_description,"
           +"   CASE when aircraft_type        is null then 't' else 'f' end as missing_aircraft_type, "
           +"   CASE when engine_type          is null then 't' else 'f' end as missing_engine_type, "
           +"   CASE when engine_family        is null then 't' else 'f' end as missing_engine_family, "
           +"   CASE when best_depart_icao     is null then 't' else 'f' end as invalid_airport_depart_cd,"
           +"   CASE when best_arrival_icao    is null then 't' else 'f' end as invalid_airport_dest_cd,"
           +"   CASE when flight_key_no        is null then 't' else 'f' end as missing_flight_key_no,"
           +"   CASE when tail_number          is null then 't' else 'f' end as missing_tail_number,"
           +"   CASE when engine_serial_number is null then 't' else 'f' end as missing_engine_serial_number,"
           +"   CASE when installation_date    is null then 't' else 'f' end as missing_installation_date,"
           +"   CASE when removal_date         is null then 't' else 'f' end as missing_removal_date, "
          +"   CASE when customer_icao_code   is null then 't' else 'f' end as missing_customer_icao_code, "
          +"   CASE when engine_family_mdm    is null then 't' else 'f' end as missing_engine_family_cd,"
          +"   CASE when engine_model_mdm     is null then 't' else 'f' end as missing_engine_model_cd, "
           +"   CASE when engine_series_mdm    is null then 't' else 'f' end as missing_engine_series_cd, "
           +"   CASE WHEN aircraft_family      is null then 't' else 'f' end as missing_aircraft_family, "
           +" now() as when_created "
        +" from final_temp_vw")


    final_cf.createOrReplaceTempView("final_cf")
    COMMON_FLIGHT = spark.sql("select ID, flight_key_no, tail_number,engine_serial_number, engine_position,"
              +"engine_position_description,enginefitstring,no_of_engines,"
              +"removal_date,airport_depart, airport_destination,faware_depart, faware_destination, great_circle_distance,cleansed_airport_depart_iata,"
              +" cleansed_airport_depart_icao, dlgs_es_ct_code, dlgs_to_ct_code,depart_src,cleansed_airport_destination_iata, "
              +"cleansed_airport_destination_icao, destination_src, environment_id_depart, airport_depart_env_6x_id, airport_depart_env_5x_id, "
             +"airport_depart_env_4x_id, environment_id_destination, airport_destination_env_6x_id, "
             +"airport_destination_env_5x_id, airport_destination_env_4x_id,metar_id_depart,metar_id_destination,faware_id, fa_planned_time_utc,"
             +"fa_estimated_dest_utc,ffd_flight_date_exact,phm_flight_start_time,phm_flight_end_time,adi_flight_record_number,matched_adi_flight_rec_nos,"
             +" phm_flight_def_id, rmd_flight_def_id, sedi_etsn, sedi_ecsn, sedi_etsv, sedi_ecsv,sedi_etsi,sedi_ecsi, invalid_airport_depart_cd,"
             +" invalid_airport_dest_cd, missing_flight_key_no, missing_tail_number,missing_engine_serial_number,missing_aircraft_family,"
            +"missing_aircraft_type,missing_engine_family,missing_engine_type,missing_engine_family_cd,missing_engine_model_cd,missing_engine_series_cd,"
            +"missing_installation_date,missing_removal_date,missing_customer_icao_code,is_overlapping,has_duplicates,fdm_event_date,fdm_removal_count,"
            +"fdm_shop_visit_count,phm_st_id,phm_st_date_time,eng_pos_1_esn,eng_pos_2_esn,eng_pos_3_esn,eng_pos_4_esn,esn_sister_array, esn_sister_comment,"
            +"es_rprt_id_eng_pos_1,es_rprt_id_eng_pos_2,es_rprt_id_eng_pos_3,es_rprt_id_eng_pos_4,es_rprt_id_eng_pos_array,"
            +"to_rprt_id_eng_pos_1,to_rprt_id_eng_pos_2,to_rprt_id_eng_pos_3,to_rprt_id_eng_pos_4,to_rprt_id_eng_pos_array,cl_rprt_id_eng_pos_1,"
             +"cl_rprt_id_eng_pos_2,cl_rprt_id_eng_pos_3,cl_rprt_id_eng_pos_4,cl_rprt_id_eng_pos_array,cr1_rprt_id_eng_pos_1,cr1_rprt_id_eng_pos_2,"
             +"cr1_rprt_id_eng_pos_3,cr1_rprt_id_eng_pos_4,cr1_rprt_id_eng_pos_array,pf_rprt_id_eng_pos_1,pf_rprt_id_eng_pos_2,pf_rprt_id_eng_pos_3,"
             +"pf_rprt_id_eng_pos_4,pf_rprt_id_eng_pos_array, sp_rprt_id_eng_pos_1,sp_rprt_id_eng_pos_2,sp_rprt_id_eng_pos_3,sp_rprt_id_eng_pos_4,"
             +"sp_rprt_id_eng_pos_array,"
             +"phm_def_id_eng_pos_1,phm_def_id_eng_pos_2,phm_def_id_eng_pos_3,phm_def_id_eng_pos_4,phm_def_id_eng_pos_array,"
             +" best_datetime,best_flight_start_time,best_flight_end_time, when_created, "
             +" date_format(best_datetime,'yyyyMM') as best_yearmonth "
             +" from final_cf "
             +" where date_format(best_datetime,'yyyyMM') >= "+v_yearmonth 
            )


    COMMON_FLIGHT.createOrReplaceTempView("COMMON_FLIGHT")
############ Backing i year of data from full run of common flight #################################################################
    common_flight_base = spark.sql("select *  from rdf_etl."+v_enginefamily+"_common_flight"
                                  )
    common_flight_base.createOrReplaceTempView("common_flight_base")
    common_flight_lastyear = spark.sql("select * from common_flight_base where best_yearmonth >= "+v_yearmonth)
    common_flight_lastyear.createOrReplaceTempView("common_flight_lastyear")
    lsStrPartsToDrop1  = common_flight_lastyear.select("best_yearmonth").distinct().rdd.flatMap(lambda x: x).collect()
    lsStrPartsToDrop= map(unicode,lsStrPartsToDrop1)
    print     lsStrPartsToDrop 
############# set final table name and esxternal path
    v_finalCFstgTableName                          = v_enginefamily+'_Common_Flight'
    v_finalCFstgTableFullName = (v_finalHiveTableSchemaName.lower() +'.'+ v_finalCFstgTableName.lower())
    v_finalCFTablefullpath    = (v_extHiveFinalTablePath + v_finalCFstgTableName)
    print v_finalCFTablefullpath
 ########### Data getting dropped ##################################################################################################   
    for i in range(0, len(lsStrPartsToDrop)):
        droplocation = v_finalCFTablefullpath + "/best_yearmonth=" + lsStrPartsToDrop[i]
        v_sp_cmd = ['hdfs', 'dfs', '-rm', '-r','-skipTrash',' ', droplocation]
        print v_sp_cmd
        v_sp_fx_return_code = fx_run_subprocess_cmd( v_sp_cmd )
        print 'partition dropped',lsStrPartsToDrop[i]



################# PREPARING FINAL DATA TO WRITE BASED ON ENGINE FAMILY #################

    print ('[{}]: Creating/Overwiting final Hive table "{}" ...'.format( datetime.now(), v_finalCFstgTableFullName ))
#`save COMMON FLIGHT as external file
    COMMON_FLIGHT.write.mode("append").partitionBy("best_yearmonth").parquet( v_finalCFTablefullpath )

## get column name and data type in string varibale
    v_dfColWithTypesString = ''
    for v_colName, v_colType in COMMON_FLIGHT.dtypes:
        if v_colName != 'best_yearmonth':
           v_dfColWithTypesString = ( v_dfColWithTypesString + ', ' + v_colName + ' ' + v_colType ).lstrip(',')

## Drop Hive table if it exists

    spark.sql( "DROP TABLE IF EXISTS {}".format( v_finalCFstgTableFullName ))


#Create final  table

    spark.sql(
                 ("""CREATE EXTERNAL TABLE {}
                        ( {} )
                        PARTITIONED BY ({})
                        STORED AS PARQUET
                        LOCATION '{}'
                """).format(
                                                v_finalCFstgTableFullName,
                                                v_dfColWithTypesString,
                                                v_partitioncol,
                                                v_finalCFTablefullpath
                                        ))
    


    print ('[{}]: Created table "{}": OK'.format( datetime.now(), v_finalCFstgTableName ))
    v_endDatetime = datetime.now()
### Print total run-time
    print ('[{}]:  Common Flight Completed Successfully - Total Run-time: {} '.format( datetime.now(), ( v_endDatetime - v_startDatetime ) ))
    spark.sql( "MSCK REPAIR TABLE {}".format( v_finalCFstgTableFullName ))
    #spark.sql( "MSCK REPAIR TABLE {}".format( v_finalCFstgTableFullName1 ))
    print ('[{}]: **** END **** \n\n'.format( datetime.now() ))


################## EXCEPTIONS #####################
except getopt.GetoptError:
        print (v_errUsageMsg)
        sys.exit(2)

except InvalidNumOfArgsError:
        print ('\n[{}]: {} Invalid Number of Arguments'.format( datetime.now(), v_errMsgPrefix ))
        print (v_errUsageMsg)
        sys.exit(2)

except UnknownInputArgsError:
        print ('\n[{}]: {} Unknown Argument(s) {}'.format( datetime.now(), v_errMsgPrefix, unparsed_args ))
        print (v_errUsageMsg)
        sys.exit(2)

except UnknownEngineFamilyError:
        print ('\n[{}]: {} "{}" is not a valid engine family'.format( datetime.now(), v_errMsgPrefix, v_enginefamily ))
        sys.exit(2)

except InvalidShowCountsOptError:
        print ('\n[{}]: {} "{}" is not a valid showcounts argument option'.format( datetime.now(), v_errMsgPrefix, v_showDFCountsOpt ))
        sys.exit(2)



package com.fis.ecomm.reporting

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

object rptextorders {

  def main(args: Array[String]) {

    var Exitcode=0


    val spark = SparkSession
      .builder()
      .appName("rpt_ext_orders")
      .config("spark.sql.warehouse.dir", "/tmp")
      //.config("spark.hadoop.validateOutputSpecs", "false")
      .config("spark.debug.maxToStringFields", 100)
      .config("spark.scheduler.mode", "FAIR")
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("INFO")

    println("started loading rpt-ext-orders")

    var current_date = java.time.LocalDate.now.toString
    var target_path = "/lake/ecomm/rpt_ext_orders/"
    if (args.length == 0) {
      println("completed deriving the dates")
    }
    else if (args.length == 1) {
      current_date = args(0)
    }
    else if(args.length == 2){
      current_date = args(0)
      target_path = args(1)
    }
    else {
      println("Usage: <YYYY-MM-DD> <TargetPath> OR Usage: No Arguments")
      System.exit(1)
    }

    val current_date_formatted = LocalDate.parse(current_date, DateTimeFormatter.ofPattern("yyyy-MM-dd"))
    val from_ext_date_30 = current_date_formatted.minusMonths(1)
    val from_ext_date_120 = current_date_formatted.minusMonths(3)

    try {
      val ext = spark.sql("select * from ecomm.rpt_ext_payments where created_date between'" + from_ext_date_30 + "' and '" + current_date_formatted + "'")
      ext.createOrReplaceTempView("ext")

      val query_1 = spark.sql("SELECT tmp2.*,FIRST_VALUE(ot_start_ts) OVER w1 AS initial_decline_time,FIRST_VALUE(payment_id) OVER w1 AS order_group_id,FIRST_VALUE(CASE WHEN rrc_advice_category = 'HARD_DECLINES' AND amount > 0 THEN 1 ELSE 0 END) OVER w1 AS hard_decline,ROW_NUMBER() OVER (PARTITION BY tmp2.merchant_id,tmp2.card_id, tmp2.amount, tmp2.session_id ORDER BY tmp2.ot_start_ts, tmp2.payment_id) AS row_num,FIRST_VALUE(phoenix_response_reason_code) OVER w1 AS initial_phoenix_response_reason_code,FIRST_VALUE(phoenix_response_reason_message) OVER w1 AS initial_phoenix_response_reason_message FROM ( select tmp.*, COUNT(CASE WHEN lag_1 = 1 THEN 1 END)    OVER(PARTITION BY tmp.merchant_id,tmp.card_id, tmp.amount ORDER BY tmp.ot_start_ts, tmp.payment_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) session_id from ( SELECT p0.*, LAG(transaction_status,1) OVER(PARTITION BY p0.merchant_id,p0.card_id, p0.amount ORDER BY p0.ot_start_ts, p0.payment_id) lag_1 FROM ext p0  where  p0.sha_account_number_id > 0 and p0.action_type = 'A' ) tmp )tmp2 WINDOW w1 AS (PARTITION BY tmp2.merchant_id,tmp2.card_id, tmp2.amount, tmp2.session_id ORDER BY tmp2.ot_start_ts, tmp2.payment_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)")

      query_1.createOrReplaceTempView("query_1")

      val cbk = spark.sql("select * from ecomm.rpt_cbk_cases where open_Date between '" + from_ext_date_120 + "' and '" + current_date_formatted + "'")
      cbk.createOrReplaceTempView("cbk")

      val query_2 = spark.sql("select recycling_seq.payment_id ,recycling_seq.method_of_payment_code ,recycling_seq.ot_start_ts ,recycling_seq.card_id ,recycling_seq.action_type ,recycling_seq.transaction_status ,recycling_seq.parent_id ,recycling_seq.merchant_id ,recycling_seq.transaction_reason ,recycling_seq.transaction_type_code ,recycling_seq.amount ,recycling_seq.response_reason_code ,recycling_seq.avs_response_code ,recycling_seq.fraud_checksum_response ,recycling_seq.custom_billing_descriptor ,recycling_seq.merchant_classification ,recycling_seq.merchant_reference_string ,recycling_seq.settlement_amount ,recycling_seq.funding_source_type ,recycling_seq.affluence ,recycling_seq.card_product_type ,recycling_seq.prepaid_card_type ,recycling_seq.card_usage ,recycling_seq.card_expiration_date ,case when recycling_seq.method_of_payment_code='DI' then recycling_seq.product_id else recycling_seq.card_product_code end as card_product_code ,recycling_seq.sha_account_number_id ,recycling_seq.grouping_id ,recycling_seq.purchase_currency_code ,recycling_seq.settlement_currency_code ,recycling_seq.amount_usd ,recycling_seq.settlement_amount_usd ,recycling_seq.merchant_name ,recycling_seq.merchant_category_code ,recycling_seq.merchant_legal_name ,recycling_seq.organization_id ,recycling_seq.organization_name ,recycling_seq.bin ,recycling_seq.purchase_currency_desc ,recycling_seq.settlement_currency_desc ,recycling_seq.purchase_rpt_precision ,recycling_seq.settlement_rpt_precision ,recycling_seq.response_desc ,recycling_seq.response_reason_type ,recycling_seq.mcc_grouping ,recycling_seq.mcc_desc ,recycling_seq.litle_exchange_rate_adjustment ,recycling_seq.phoenix_response_reason_code ,recycling_seq.phoenix_response_reason_message ,recycling_seq.rrc_advice_category ,recycling_seq.prod_id_code ,recycling_seq.prod_id_description ,recycling_seq.bin_country_code ,recycling_seq.iso_country_name ,recycling_seq.iso_country_code_alpha2 ,recycling_seq.iso_country_code_alpha3 ,recycling_seq.product_code ,recycling_seq.product_code_type ,recycling_seq.product_funding_source ,recycling_seq.product_description ,recycling_seq.method_of_payment_description ,recycling_seq.batch_post_day ,recycling_seq.reloadable ,recycling_seq.complete_date ,recycling_seq.organization_type_code ,recycling_seq.currency_display_name ,recycling_seq.settlement_display_name ,recycling_seq.account_suffix ,recycling_seq.ot_start_ts_ts ,recycling_seq.created_date_day ,recycling_seq.created_date_tss ,recycling_seq.created_date_ts ,recycling_seq.batch_post_day_ts ,recycling_seq.complete_date_ts ,recycling_seq.initial_decline_time ,recycling_seq.order_group_id ,recycling_seq.session_id ,recycling_seq.hard_decline ,recycling_seq.row_num as row_number ,recycling_seq.initial_phoenix_response_reason_code ,recycling_seq.initial_phoenix_response_reason_message ,CAST(substr(recycling_seq.ot_start_ts, 1, 7) AS STRING) AS year_month ,CAST(CONCAT(substr(recycling_seq.ot_start_ts, 1, 7),'-01') AS TIMESTAMP) AS year_month_day ,deposit.payment_id AS deposit_payment_id ,CAST(substr(deposit.ot_start_ts, 1, 7) AS STRING) AS deposit_month ,sum(case when cbk.deposit_id is not NULL then 1 else 0 end) cbk_chargebacks_count ,recycling_seq.created_date from query_1 recycling_seq LEFT OUTER JOIN ext deposit     ON  recycling_seq.payment_id = deposit.parent_id and deposit.action_type = 'D' and deposit.transaction_status = '1' LEFT OUTER JOIN  cbk ON  deposit.payment_id = cbk.deposit_id and cbk.type = 'D' and cbk.current_cycle in ('FIRST_CHARGEBACK', 'REPRESENTMENT') GROUP BY recycling_seq.payment_id ,recycling_seq.method_of_payment_code ,recycling_seq.ot_start_ts ,recycling_seq.card_id ,recycling_seq.action_type ,recycling_seq.transaction_status ,recycling_seq.parent_id ,recycling_seq.merchant_id ,recycling_seq.transaction_reason ,recycling_seq.transaction_type_code ,recycling_seq.amount ,recycling_seq.response_reason_code ,recycling_seq.avs_response_code ,recycling_seq.fraud_checksum_response ,recycling_seq.custom_billing_descriptor ,recycling_seq.merchant_classification ,recycling_seq.merchant_reference_string ,recycling_seq.settlement_amount ,recycling_seq.funding_source_type ,recycling_seq.affluence ,recycling_seq.card_product_type ,recycling_seq.prepaid_card_type ,recycling_seq.card_usage ,recycling_seq.card_expiration_date ,case when recycling_seq.method_of_payment_code='DI' then recycling_seq.product_id else recycling_seq.card_product_code end ,recycling_seq.sha_account_number_id ,recycling_seq.grouping_id ,recycling_seq.purchase_currency_code ,recycling_seq.settlement_currency_code ,recycling_seq.amount_usd ,recycling_seq.settlement_amount_usd ,recycling_seq.merchant_name ,recycling_seq.merchant_category_code ,recycling_seq.merchant_legal_name ,recycling_seq.organization_id ,recycling_seq.organization_name ,recycling_seq.bin ,recycling_seq.purchase_currency_desc ,recycling_seq.settlement_currency_desc ,recycling_seq.purchase_rpt_precision ,recycling_seq.settlement_rpt_precision ,recycling_seq.response_desc ,recycling_seq.response_reason_type ,recycling_seq.mcc_grouping ,recycling_seq.mcc_desc ,recycling_seq.litle_exchange_rate_adjustment ,recycling_seq.phoenix_response_reason_code ,recycling_seq.phoenix_response_reason_message ,recycling_seq.rrc_advice_category ,recycling_seq.prod_id_code ,recycling_seq.prod_id_description ,recycling_seq.bin_country_code ,recycling_seq.iso_country_name ,recycling_seq.iso_country_code_alpha2 ,recycling_seq.iso_country_code_alpha3 ,recycling_seq.product_code ,recycling_seq.product_code_type ,recycling_seq.product_funding_source ,recycling_seq.product_description ,recycling_seq.method_of_payment_description ,recycling_seq.batch_post_day ,recycling_seq.reloadable ,recycling_seq.complete_date ,recycling_seq.organization_type_code ,recycling_seq.currency_display_name ,recycling_seq.settlement_display_name ,recycling_seq.account_suffix ,recycling_seq.ot_start_ts_ts ,recycling_seq.created_date_day ,recycling_seq.created_date_tss ,recycling_seq.created_date_ts ,recycling_seq.batch_post_day_ts ,recycling_seq.complete_date_ts ,recycling_seq.initial_decline_time ,recycling_seq.order_group_id ,recycling_seq.session_id ,recycling_seq.hard_decline ,recycling_seq.row_num ,recycling_seq.initial_phoenix_response_reason_code ,recycling_seq.initial_phoenix_response_reason_message ,CAST(substr(recycling_seq.ot_start_ts, 1, 7) AS STRING) ,CAST(CONCAT(substr(recycling_seq.ot_start_ts, 1, 7),'-01') AS TIMESTAMP) ,deposit.payment_id ,CAST(substr(deposit.ot_start_ts, 1, 7) AS STRING) ,recycling_seq.created_date")

      
import scala.sys.process._


val remove_Dates=spark.sql("select distinct created_Date from ext where created_DAte>='"+from_ext_date_30+"'").collect

def remove_hdfs(remove_Dates: Array[org.apache.spark.sql.Row]) = {
remove_Dates.foreach {
columns =>{
var extract_date=columns.toString.replace("[","").replace("]","")
println ("hdfs dfs -rm -r  /lake/ecomm/rpt_ext_orders1/created_date="+extract_date)
"hdfs dfs -rm -r  /lake/ecomm/rpt_ext_orders1/created_date="+extract_date! }
}
}

      
      query_2.write.mode(SaveMode.Overwrite).partitionBy("created_date").parquet(target_path)

      println("completed loading rpt-ext-orders")

    } catch {
      case e: Throwable =>
        println(e)
        Exitcode = 1
    } finally {
      spark.stop()
      System.exit(Exitcode)
    }
  }
}

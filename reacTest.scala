import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit
import scala.collection.mutable.{ListBuffer, WrappedArray}

/**
  * @author ${user.name}
  */
object reacTest {

  def main(args: Array[String]) {

    val spark = SparkSession.builder
      .master(args(0))
      .appName("reacTest")
      .getOrCreate()

    //val path = "hdfs://c7-master:9100/user/long/"
    val path = args(1)
    val N = args(2).toInt
    val filePath = path + "Generated_" + N + "/REAC" + "14Q3-18Q2/"

    val FDA_obl_conclusions = Seq(
      "obl_report_on_ICSRs_to_FDA",
      "obl_report_Patient_information_to_FDA",
      "obl_report_Patient_age_to_FDA",
      "obl_report_Patient_date_of_birth_to_FDA",
      "obl_report_Patient_gender_to_FDA",
      "obl_report_Patient_weight_to_FDA",
      "obl_report_Adverse_drug_experience_to_FDA",
      "obl_report_outcome_attributed_to_adverse_drug_experience_to_FDA",
      "obl_report_Date_of_adverse_drug_experience_to_FDA",
      "obl_report_Date_of_ICSR_submission_to_FDA",
      "obl_report_description_of_adverse_drug_experience_to_FDA",
      "obl_report_concise_medical_narrative_of_adverse_drug_experience_to_FDA",
      "obl_report_Adverse_drug_experience_term_to_FDA",
      "obl_report_description_of_relevant_test_of_Adverse_drug_experience_to_FDA",
      "obl_report_date_of_relevant_test_of_Adverse_drug_experience_to_FDA",
      "obl_report_laboratory_data_of_relevant_test_of_Adverse_drug_experience_to_FDA",
      "obl_report_other_relevant_patient_history_of_Adverse_drug_experience_to_FDA",
      "obl_report_preexisting_medical_conditions_of_Adverse_drug_experience_to_FDA",
      "obl_report_Suspect_medical_product_information_to_FDA",
      "obl_report_Suspect_medical_product_name_to_FDA",
      "obl_report_suspect_medical_product__Dose_frequency_to_FDA",
      "obl_report_Suspect_medical_product_route_of_administration_used_to_FDA",
      "obl_report_suspect_medical_product_therapy_date_to_FDA",
      "obl_report_Suspect_medical_product_diagnosis_for_use_indication_to_FDA",
      "obl_report__to_FDA_Suspect_medical_product_whether_prescription_product",
      "obl_report_to_FDA_suspect_medical_product_whether_nonprescription_product",
      "obl_report_to_FDA_whether_adverse_drug_experience_abated_after_drug_use_stopped",
      "obl_report_to_FDA_whether_adverse_drug_experience_abated_after_drug_reduced",
      "obl_report_to_FDA_whether_adverse_drug_experience_reappeared_after_reintroduction_of_drug",
      "obl_report_Suspect_medical_product_to_FDA",
      "obl_report_Suspect_medical_Lot_number_to_FDA",
      "obl_report_Suspect_medical_product_Expiration_date_to_FDA",
      "obl_report_Suspect_medical_product_NDC_number_to_FDA",
      "obl_report_Suspect_concomitant_medical_products_to_FDA",
      "obl_report_Suspect_medical_product_concomitant_therapy_dates_to_FDA",
      "obl_report_initial_reporter_information_to_FDA",
      "obl_report_initial_reporter_name_to_FDA",
      "obl_report_initial_reporter_address_to_FDA",
      "obl_report_initial_reporter_telephone_number_to_FDA",
      "obl_report_Whether_the_initial_reporter_is_a_health_care_professional_to_FDA",
      "obl_report_initial_reporter_is_a_health_care_professional_to_FDA",
      "obl_report_MPD_information_to_FDA",
      "obl_report_MPD_name_to_FDA",
      "obl_report_MPD_contact_office_address_to_FDA",
      "obl_report_MPD_Telephone_number_to_FDA",
      "obl_report_MPD_Report_source_to_FDA",
      "obl_report_MPD_Report_received_Date_to_FDA",
      "obl_report_Whether_15_day_Alert_report_to_FDA",
      "obl_report_whether_initial_report_to_FDA",
      "obl_report_Whether_initial_report_to_FDA",
      "obl_report_Whether_followup_report_to_FDA",
      "obl_report_Unique_case_identification_number_to_FDA"
    )

    val broadCast_Des = spark.sparkContext.broadcast(FDA_obl_conclusions)

    val reacDF = spark
      .read
      .format("csv")
      .option("header", "true")
      .option("delimiter", "$")
      .load(filePath + "*.csv")

    import spark.implicits._

    ///////////////////////////
    // SQL Queries (FDA Data)//
    ///////////////////////////

    // start time
    val startTimeMillis = System.currentTimeMillis()

    // Empty Dataframe for following queries
    var factsDF = reacDF.select(reacDF.col("primaryid").as("argument_X"))
      .withColumn("predicate", lit(""))
      .limit(0)

    // REAC
    /**/
    //report_Adverse_drug_experience_term_to_FDA
    factsDF = factsDF.union(reacDF
      .where(reacDF.col("pt").isNotNull && reacDF.col("pt") =!= "")
      .select(reacDF.col("primaryid").as("argument_X"))
      .withColumn("predicate",lit("report_Adverse_drug_experience_term_to_FDA")))
    /**/

    // Remove duplicates
    factsDF = factsDF.distinct()

    /////////////////////////////////////////
    // Defeasible Reasoning (FDA Rule Set) //
    /////////////////////////////////////////

    // Set up table name
    factsDF.createOrReplaceTempView("facts")

    // Group facts with common 'primaryid'
    factsDF = factsDF.sqlContext.sql("SELECT argument_X, collect_list(predicate) as facts " +
      "FROM facts GROUP BY argument_X")

    // Perform reasoning
    val reasoning = factsDF.mapPartitions(iter => {
      val loc_FDA_obl_conclusions = broadCast_Des.value
      var sumProovedLiterals = ListBuffer[String]()

      for (row <- iter) {
        val facts = row(1).asInstanceOf[WrappedArray[String]]
        for (i <- facts) sumProovedLiterals += "+D " + i + "(" + row(0) + ")"
        for (i <- loc_FDA_obl_conclusions) sumProovedLiterals += "+d " + i + "(" + row(0) + ")"
      }

      sumProovedLiterals.iterator
    }).toDF()

    // Number of final conclusions
    println("Number of final conclusions: " + reasoning.count())

    /*
    // Store final conclusions
    factsDF.repartition(numOfPartitions)
      .write
      .format("com.databricks.spark.csv")
      .option("header", "false")
      .option("delimiter","$")
      .save(ReasoningFilesDataDir + dataTimePeriod + "/OUTPUT" + dataTimePeriod)
    */

    // end time
    val endTimeMillis = System.currentTimeMillis()
    val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
    println("REAC Total execution time = " + durationSeconds + " seconds")

    ///////////////////////
    // End of Processing //
    ///////////////////////

    spark.stop()

  }
}

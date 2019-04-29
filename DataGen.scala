import org.apache.spark.sql.SparkSession

object DataGen {

  def main(args: Array[String]) {

    val spark = SparkSession.builder
      .master(args(0))
      .appName("DataGen")
      .getOrCreate()

    //val path = "hdfs://c7-master:9100/user/long/"
    val path = args(1)

    val csvParser = spark
      .read
      .format("csv")
      .option("header", "true")
      .option("delimiter", "$")

    import spark.implicits._

    // !!! EXPERIMENTAL CONFIGURATIONS !!!

    // Directory where initial FDA files are located
    val FDAFilesDataDir = path
    // Time period covered within the given FDA files
    val dataTimePeriod = "14Q3-18Q2" // "17Q2" //
    // Number of copies to be generated based on each initial file
    val numOfCopies = args(2).toInt
    // Directory where generated FDA files are located
    val GeneratedFDAFilesDataDir = path + "Generated_"+args(2)

    //the number of cores
    val N = args(3).toInt

    //=============================================================================
    /**/
    //////////////////////////////////
    // Beggining of Data Generation //
    //////////////////////////////////

    // start time
    val startTimeMillis = System.currentTimeMillis()

    // Load each file
    val demoDF = csvParser.load(FDAFilesDataDir  + "/DEMO" + dataTimePeriod + ".csv")
    val drugDF = csvParser.load(FDAFilesDataDir  + "/DRUG" + dataTimePeriod + ".csv")
    val outcDF = csvParser.load(FDAFilesDataDir  + "/OUTC" + dataTimePeriod + ".csv")
    val reacDF = csvParser.load(FDAFilesDataDir  + "/REAC" + dataTimePeriod + ".csv")
    val rpsrDF = csvParser.load(FDAFilesDataDir + "/RPSR" + dataTimePeriod + ".csv")

    // Set up table names
    demoDF.createOrReplaceTempView("demo")
    drugDF.createOrReplaceTempView("drug")
    outcDF.createOrReplaceTempView("outc")
    reacDF.createOrReplaceTempView("reac")
    rpsrDF.createOrReplaceTempView("rpsr")

    // Sub queries specific to each table
    val demoSubQuery = "caseid, caseversion, i_f_code, event_dt, mfr_dt, init_fda_dt, fda_dt, rept_cod, " +
      "auth_num, mfr_num, mfr_sndr, lit_ref, age, age_cod, age_grp, sex, e_sub, wt, " +
      "wt_cod, rept_dt, to_mfr, occp_cod, reporter_country, occr_country FROM demo"
    val drugSubQuery = "caseid, drug_seq, role_cod, drugname, prod_ai, val_vbm, route, dose_vbm, cum_dose_chr, " +
      "cum_dose_unit, dechal, rechal, lot_num, exp_dt, nda_num, dose_amt, dose_unit, dose_form, dose_freq FROM drug"
    val outcSubQuery = "caseid, outc_cod FROM outc"
    val reacSubQuery = "caseid, pt, drug_rec_act FROM reac"
    val rpsrSubQuery = "caseid, rpsr_cod FROM rpsr"

    // Initialize Dataframes
    var tmpDemoDF = demoDF.sqlContext.sql("SELECT * FROM demo LIMIT 0")
    var tmpDrugDF = drugDF.sqlContext.sql("SELECT * FROM drug LIMIT 0")
    var tmpOutcDF = outcDF.sqlContext.sql("SELECT * FROM outc LIMIT 0")
    var tmpReacDF = reacDF.sqlContext.sql("SELECT * FROM reac LIMIT 0")
    var tmpRpsrDF = rpsrDF.sqlContext.sql("SELECT * FROM rpsr LIMIT 0")

    // Generate copies for each file
    for (i <- 1 to numOfCopies) {
      tmpDemoDF = tmpDemoDF.union(demoDF.sqlContext.sql("SELECT CONCAT(primaryid, '" + i + "') as primaryid, " + demoSubQuery))
      tmpDrugDF = tmpDrugDF.union(drugDF.sqlContext.sql("SELECT CONCAT(primaryid, '" + i + "') as primaryid, " + drugSubQuery))
      tmpOutcDF = tmpOutcDF.union(outcDF.sqlContext.sql("SELECT CONCAT(primaryid, '" + i + "') as primaryid, " + outcSubQuery))
      tmpReacDF = tmpReacDF.union(reacDF.sqlContext.sql("SELECT CONCAT(primaryid, '" + i + "') as primaryid, " + reacSubQuery))
      tmpRpsrDF = tmpRpsrDF.union(rpsrDF.sqlContext.sql("SELECT CONCAT(primaryid, '" + i + "') as primaryid, " + rpsrSubQuery))
    }

    // Store copies
    tmpDemoDF.repartition(N).write.format("com.databricks.spark.csv").option("header", "true")
      .option("delimiter", "$").save(GeneratedFDAFilesDataDir + "/DEMO" + dataTimePeriod)
    tmpDrugDF.repartition(N).write.format("com.databricks.spark.csv").option("header", "true")
      .option("delimiter", "$").save(GeneratedFDAFilesDataDir + "/DRUG" + dataTimePeriod)
    tmpOutcDF.repartition(N).write.format("com.databricks.spark.csv").option("header", "true")
      .option("delimiter", "$").save(GeneratedFDAFilesDataDir+ "/OUTC" + dataTimePeriod)
    tmpReacDF.repartition(N).write.format("com.databricks.spark.csv").option("header", "true")
      .option("delimiter", "$").save(GeneratedFDAFilesDataDir + "/REAC" + dataTimePeriod)
    tmpRpsrDF.repartition(N).write.format("com.databricks.spark.csv").option("header", "true")
      .option("delimiter", "$").save(GeneratedFDAFilesDataDir + "/RPSR" + dataTimePeriod)

    // end time
    val endTimeMillis = System.currentTimeMillis()
    val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
    println("Data Gen Total execution time = " + durationSeconds + " seconds")

    spark.stop()

  }
}
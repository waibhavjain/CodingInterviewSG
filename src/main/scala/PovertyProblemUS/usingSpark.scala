package PovertyProblemUS

import org.apache.spark.sql.{SparkSession,DataFrame}

object usingSpark extends App {

  val spark = SparkSession
    .builder()
    .appName("povertyProblemUsingSpark")
    .master("local[3]") // 1 master and 2 core
    .getOrCreate()

  val data = spark.read.format("csv").option("header",true).option("inferSchema",true).load("data/PovertyEstimates.csv")

  val mapping = spark.read.format("csv").option("header",true).option("inferSchema",true).load("data/StateMappingUS.csv")

  data.createOrReplaceTempView("PovertyData")

  mapping.createOrReplaceTempView("StateMapping")

  val finaldata = spark.sql("""select
      | s.State,
      | p.Area_name || " " || p.Stabr as Area_name,
      | p.Urban_Influence_Code_2003,
      | p.`Rural-urban_Continuum_Code_2013`,
      | round((100*(1-(p.POV017_2018/p.POVALL_2018))),2) as POV_elder_than17_2018
      | from PovertyData p
      | join StateMapping s
      | on p.Stabr = s.StateCode
      | where p.Urban_Influence_Code_2003 % 2 != 0
      | and p.`Rural-urban_Continuum_Code_2013` % 2 == 0
      |""".stripMargin)

    finaldata.coalesce(1).write.format("csv")
      .option("header",true)
        .mode("overwrite")
        .save("output/finalDataUsingSpark.csv")


  spark.stop()

}

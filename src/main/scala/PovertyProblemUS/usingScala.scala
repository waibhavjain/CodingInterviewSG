package PovertyProblemUS

import scala.io._
import java.io._

object usingScala extends App {

  val data = Source.fromFile("data/PovertyEstimates.csv").getLines().toList

  val mapping = Source.fromFile("data/StateMappingUS.csv").getLines().toList.map(x=>(x.split(",")(1),x.split(",")(0))).toMap

  val header = data(0)

  def toInt(s: String): Int = {
    try {
      Integer.parseInt(s.trim)
    } catch {
      case e: Exception => 0
    }
  }

 val filteredData = data
                    .filter(x=>x!=header)
                    .map(x=>x.split(",").toList)
                    .filter(x => toInt(x(4)) % 2 != 0)
                    .filter(x => toInt(x(5)) % 2 == 0)
//                    .map(x => (mapping.get(x(1)).getOrElse("Nothing")
//                                      ,x(2)+" "+x(1)
//                                      ,x(4)
//                                      ,x(5)
//                                      ,"%05.2f".format(100*(1-(x(13).toFloat/x(7).toFloat)))))
                   .map(x => mapping.get(x(1)).getOrElse("Nothing")
                     +","+x(2)+" "+x(1)
                     +","+x(4)
                     +","+x(5)
                     +","+"%05.2f".format(100*(1-(x(13).toFloat/x(7).toFloat))))

  val file = new File("output/finalData.csv")
  val bw = new BufferedWriter(new FileWriter(file))
  for (line <- filteredData) {
    bw.write(line+ '\n')
  }
  bw.close()

}

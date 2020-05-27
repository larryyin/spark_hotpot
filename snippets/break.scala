import scala.util.control.Breaks._

var iLoop = 0
breakable {
  for ((v,i) <- A.zipWithIndex) {
    try {
      spark.sql(f"select * from Table_$i%d limit 2")
      break
    } catch {
      case e: Exception => {
      }
    }
    iLoop = iLoop + 1
  }
}
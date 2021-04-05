package example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/*Create DataSet*/
object SpendAmtbyKeyUser{

/* Create DataSet
    case class UserOrders(CustId:Int, ItemId:Int,totalamt:Double)

    def main(args:Array[String]) {
        
        val spark = SparkSession
                    .builder()
                    .appName("SpendAmtbyUserDataSet")
                    .master("local[*]")
                    .getOrCreate()
        
        val UserOrderSchema = new StructType()
                            .add("CustId", IntegerType,nullable=true)
                            .add("ItemId",IntegerType,nullable=true)
                            .add("totalamt",DoubleType,nullable =true)

        import spark.implicits._
    
        val UserDS = spark.read
                    .schema(UserOrderSchema)
                    .csv("data/UserOrder.csv")
*/
        
        def main(args:Array[String]) {
        //Load DataFrame
            val OrderbyUser = spark.read.format("csv")
                        .option("header","true")
                        .option("inferSchema","true")
                        .csv("C:/Users/A201909072/Desktop/data/UserOrders.csv")
                        .select("CustId","ItemId","TotalAmt") // select only three columns
                        .coalesce(5) 

            val CreateidexOrderbyUser=OrderbykeyUser.withColumn("id",monotonically_increasing_id())
            //Create Keyuser
            val OrderbykeyUser = OrderbyUser.withColumn("Key_User",when($"CustId" > 50,"X"))
            val filterKeyUser = OrderbykeyUser.filter(OrderbykeyUser("Key_User") === "X")
            
            OrderbykeyUser.createOrReplaceTempview("OrderbykeyUserview")
            //SparkSql         
            val totalamtbyKeyUserSorted = filterKeyUser.groupBy("CustId")
                                        .agg(
                                            round(sum("TotalAmt"),2).alias("sum_total")
                                            ,expr("round(avg(TotalAmt),2)").alias("avg_sum_total"))
                                            .orderBy(desc("CustId"))
            totalamtbyKeyUserSorted.show()



    }

}

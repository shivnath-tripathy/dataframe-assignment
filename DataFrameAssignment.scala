package sparkassignment45
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._
import scala.io.Source

object SparkAssignment45 {
   def main(args: Array[String]): Unit = {
    val conf:SparkConf = new SparkConf().setAppName("Simple Application").setMaster("local[2]").set("spark.executor.memory","1g") //Configuration
    val sc=new SparkContext(conf) // Create Spark Context with Configuration
    val spark = SparkSession //Create Spark Session
    .builder()
    .appName("Spark SQL basic example")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()
    // READ ALL DATA
    val RDD1 = sc.textFile("Dataset_Holidays.txt")
    val RDD2 = sc.textFile("Dataset_Transport.txt")
    val RDD3 = sc.textFile("Dataset_User_details.txt")
    //MEMORY EFFICIENT    
    RDD1.persist(StorageLevel.MEMORY_ONLY)
    RDD2.persist(StorageLevel.MEMORY_ONLY)
    RDD3.persist(StorageLevel.MEMORY_ONLY)
    // SPLIT DATA AND SAVE ALL IN RESPECTIVE RDD AS holiday, transport and user -- ALL NUMERIC ARE DEFINED AS INT USING toInt   
    val holiday = RDD1.map(x=> (x.split(",")(0).toInt,x.split(",")(1),x.split(",")(2),x.split(",")(3),x.split(",")(4).toInt,x.split(",")(5).toInt))
    holiday.foreach(println)
    val transport = RDD2.map(x=> (x.split(",")(0),x.split(",")(1).toInt))
    transport.foreach(println)
    val user = RDD3.map(x=> (x.split(",")(0).toInt,x.split(",")(1),x.split(",")(2).toInt))
    user.foreach(println)
    
    val holidayMap = holiday.map(x=> x._4->(x._3,x._5,x._6)) // Create a Key Value pair with transport Mode and (dest,distance,year)
    holidayMap.foreach(println)
    val transportMap = transport.map(x=> x._1->x._2) // Create a Key Value pair transport mode and cost
    transportMap.foreach(println)
   
    val joins =  holidayMap.join(transportMap) //Join based on the Transport Mode
    joins.foreach(println)
    val route = joins.map(x=>(x._2._1._1 ->x._2._1._3)->(x._2._1._2 * x._2._2)) //Identify, for each travel what is the cost by multiply distance * cost 
    route.foreach(println)                                                 // and Key Value pair it with Destination and Year
    val revenues = route.groupByKey().map(x=> x._2.sum->x._1) // Group by  Destination and year and sum all the travel cost based on - revenue
    revenues.foreach(println)
    val mostrevenue = revenues.sortByKey(true)// Get the highest revenue from the grouped data
    
    println("Most Revenue Destination and Year is in Below Order:")
    mostrevenue.foreach(println)
    
    // TO IDENTIFY THE AMOUNT SPEND BY EACH USER PER YEAR
    
    val holidayUserMap = holiday.map(x=> x._4->(x._1,x._5,x._6)) // Get Holiday Details with User id, with transport mode as key
    val transportRate = holidayUserMap.join(transportMap) // Get all rate for each transport mode using Join
    //Create a Tuple rdd with User/Year and the  amount for each trip by multiplying Dist * rate
    val amount = transportRate.map(x=> (x._2._1._1, x._2._1._3)->(x._2._1._2 * x._2._2)) 
    val UserExpense = amount.groupByKey().map(x=>x._1->x._2.sum) // Group by User/Year and Sum the amount
    println("*** Expense of each User per Year ***")
    UserExpense.foreach(println)
    
    //Considering age groups of < 20 , 20-35, 35 > ,Which age group is traveling the most every year.
    
    val groupAgeMap = user.map(x=> x._1-> 
                  {if(x._3<20) "20" else if(x._3>35) "35" else "20-35"}) //Categorize the age group using age in User data
    val UserIDMap = holiday.map(x=> x._1->1) // Get User ID from the Holiday Data
    val UserIDGroup = groupAgeMap.join(UserIDMap) // Join to get the Age Group Data
    val AgeGrouped = UserIDGroup.map(x=> x._2._1 -> x._2._2) // Remove User ID and have only Age Group
    val TotalAgeGroup = AgeGrouped.groupByKey.map(x=> x._1 -> x._2.sum) // Sum all the value to get the count for each group
    println("List of Age group, with number of time they have travelled")
    TotalAgeGroup.foreach(println)
    
}
}

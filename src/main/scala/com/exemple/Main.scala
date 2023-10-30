package com.exemple

import org.apache.spark.sql.functions.{array_contains, avg, col, concat_ws, count, countDistinct, date_add, date_format, date_sub, datediff, dayofmonth, dayofyear, explode, hour, initcap, last_day, length, lit, lower, lpad, ltrim, max, min, minute, months_between, next_day, quarter, regexp_extract, regexp_replace, rpad, rtrim, second, sort_array, sum, sum_distinct, to_date, to_timestamp, translate, udf, upper, weekofyear, year}
import org.apache.spark.sql.types.{BooleanType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession, functions}

object Main {
  def build(): SparkSession = {
    val spark = SparkSession.builder().appName("test").master("local[*]").getOrCreate()
    return spark
  }

  def createDF_fromSeq(spark: SparkSession): DataFrame = {
    import spark.implicits._
    val sc = spark.sparkContext
    val data = Seq(("1", "Jad", "20"), ("2", "Tim", "19"), ("3", "Nicola", "18"))
    val etudRDD = sc.parallelize(data)
    return etudRDD.toDF()
  }
  def createDF_fromSeq_V2(spark: SparkSession) : DataFrame = {
    import spark.implicits._
    val sc = spark.sparkContext
    val data = Seq((1L,"Jad",20L),(2L,"Tim",19L),(3L,"Nicola",18L))
    val etudRDD = sc.parallelize(data)
    return etudRDD.toDF("ID","Name","Age")
  }
  def createDF_byRow(spark: SparkSession) : DataFrame = {
    val sc = spark.sparkContext
    val etudRDD = sc.parallelize(List(Row(1L, "Jad", 20L), Row(2L, "Tim", 19L), Row(3L, "Nicola", 18L)))
    val schema = StructType(Array(StructField("ID", LongType,true), StructField("Name", StringType, true),StructField("Age", LongType, true)))
    val etudDF = spark.createDataFrame(etudRDD,schema)
    return etudDF
  }

 /* def createDF_CSV(spark: SparkSession, path: String): DataFrame = {
    val movies = spark.read.option("header", "true").csv(path)
    return movies
  }*/

 /* def createDF_CSV(spark: SparkSession, path: String): DataFrame = {
    //val movies = spark.read.option("header","true").csv(path)
    val movies = spark.read.options(Map("header" -> "true", "delimiter" -> ","
      , "inferschema" -> "true")).csv(path)
    return movies
  }*/
 def createDF_CSV(spark: SparkSession, path : String) : DataFrame = {
   //val movies = spark.read.option("header","true").csv(path)
   val movieSchema = StructType(Array(StructField("actor_name", StringType, true),StructField("movie_title", StringType, true),StructField("produced_year", LongType, true)))

   val moviesDF = spark.read.options(Map("header" ->"true","delimiter" -> ","
     ,"inferschema"->"true")).schema(movieSchema).csv(path)
   return moviesDF
 }
  def createDF_JSON(spark: SparkSession, path: String): DataFrame = {
    val moviesDF = spark.read.json(path)
    return moviesDF
  }
 /* def createDF_JSON(spark: SparkSession, path: String): DataFrame = {
    val movieSchema = StructType(Array(StructField("actor_name", StringType, true), StructField("movie_title", StringType, true), StructField("produced_year",
      BooleanType, true)))
    val moviesDF = spark.read.schema(movieSchema).option("inferSchema","true")
      .option("mode","failFast").json(path)
    return moviesDF
  }*/

  def createDF_DB(spark: SparkSession): DataFrame = {
    val mysqlURL = "jdbc:mysql://192.168.232.3:3306/movies"
    val filmDF = spark.read.format("jdbc").option("driver", "com.mysql.cj.jdbc.Driver")
        .option("url", mysqlURL).option("dbtable", "movie").option("user", "root")
        .option("password", "123Tim456").load()
    return filmDF
  }


  def add_column(df: DataFrame): DataFrame = {
    //Ajouter une nouvelle colonne avec une valeur constante
    /*val df_new = df.withColumn("ID", lit(10))*/
    //Ajouter une nouvelle colonne à partir d’une autre colonne
  /*  val df_new = df.withColumn("ID", df.col("produced_year") >= 2000)*/
    /*val df_new = df.withColumn("produced_decade", (df.col("produced_year") - df.col("produced_year") % 10))*/
    //Ajouter une nouvelle colonne en changeant le type de colonne
    val df_new = df.withColumn("PY", df.col("produced_year").cast("Long"))
    return df_new
  }
  def renommer_column(df : DataFrame) : DataFrame = {
    val df_rename =
    df.withColumnRenamed("actor_name","actor").withColumnRenamed("movie_title","title")
      .withColumnRenamed("produced_year", "year")

    return df_rename
  }

  def drop_column(df : DataFrame) : DataFrame = {
    return df.drop("actor_name", "column_x")
  }

  def sample_Trans(df: DataFrame): DataFrame = {
    // échantillon avec un ratio et sans remplacement
    val sample1 = df.sample(false, 0.0004)
    // échantillon avec remplacement, un ratio et une graine (seed)
    val sample2 = df.sample(true, 0.0003, 123)

    /*return sample1*/
    return sample2
  }
  def randomSplit_Trans(df: DataFrame): Array[DataFrame] = {
    // les poids doivent être sous forme d'un tableau
    val petitsDFs = df.randomSplit(Array(0.5, 0.3, 0.2))
    return petitsDFs
  }
  def select_Version(spark : SparkSession, df : DataFrame) : DataFrame = {
    import spark.implicits._
    val df1 = df.select('movie_title, ('produced_year - ('produced_year % 10)).as("produced_decade"))
    val df2 = df.select('movie_title)
    val df3 = df.select('movie_title, (length(df.col("movie_title")) >= 10).as("new_column"))
    val df4 = df.select(df.columns(0))
    val df5 = df.select(df.columns.slice(0,2).map(m => col(m)):_*)
    return df5
    // return df1 // return df2 // return df4 // return df5
  }
  def selectExpr_Version(df : DataFrame) : DataFrame = {
    val df1 = df.selectExpr("*", "(produced_year - (produced_year % 10)) as decade")
    return df1
  }
  def selectExpr_Version2(df: DataFrame): DataFrame = {
    val df1 = df.selectExpr("count(distinct(movie_title)) as movies_nbr"," count (distinct(actor_name)) as actors_nbr")
    return df1
  }
  def filtrage(spark : SparkSession, df : DataFrame) : DataFrame = {
    import spark.implicits._
    val df1 = df.filter('produced_year === 1990)
    val df2 = df.select("movie_title",
      "produced_year").where('produced_year =!= 2000)
    val df3 = df.filter('produced_year >= 1998 && length('movie_title) < 7)
    val df4 = df.filter('produced_year >= 2000).filter(length('movie_title)
      > 10 )
    return df4
    //return df3 // return df2 // return df1
  }
  def drop_fct(df : DataFrame) : DataFrame = {
    val df1 = df.select("movie_title").distinct.selectExpr("count(movie_title) as movies")
    val df2 = df.dropDuplicates("movie_title").selectExpr("count(movie_title) as movies")
    return df2
  }

  def tri_fcts(spark : SparkSession, df : DataFrame) : DataFrame = {
    import spark.implicits._
    val df_base = df.dropDuplicates("movie_title").selectExpr("movie_title"
      ,"length(movie_title) as title_length", "produced_year")
    val df1 = df_base.sort('title_length)
    val df2 = df_base.orderBy('title_length.desc)
    //Tri par deux colonnes
    val df3 = df_base.orderBy('title_length.desc, 'produced_year)
    return df3 //return df2 // return df1
  }
  def limit_fct(spark : SparkSession, df : DataFrame) : DataFrame = {
    import spark.implicits._
    // Creation du DataFrame avec nom acteur et sa longueur associée
    val df_actname = df.select("actor_name").distinct.selectExpr("*","length(actor_name) as length")
    // classer les noms par longueur et récupère le top 10
    val df_limit = df_actname.orderBy('length.desc).limit(10)
    return df_limit
  }

  def union_fct(spark: SparkSession, df: DataFrame): DataFrame = {
    import spark.implicits._
    // Selectionner les films que nous voulons ajouter l'acteur manquant ("12")
    val movie_selected = df.where('movie_title === "12")

    // creer un DataFrame avec une seule ligne
    val actor_data = Seq(Row("Brychta, Edita", "12", 2007L))
    val addedActor_RDD = spark.sparkContext.parallelize(actor_data)
    val addedActor_DF = spark.createDataFrame(addedActor_RDD, movie_selected.schema)

    // union
    val union_DF = movie_selected.union(addedActor_DF)
    return union_DF
  }
  def drop_nullEntries(spark : SparkSession, df : DataFrame) : DataFrame = {
    // créer d'abord un DataFrame avec des valeurs manquantes
    val badMovies = Seq(Row(null,null,null),Row(null,null,2018L),Row("John Doe", "Awesome Movie", null),Row(null, "Awesome Movie", 2018L),Row("Mary Jane", null, 2018L))
    val badMRDD = spark.sparkContext.parallelize(badMovies)
    val badMDF = spark.createDataFrame(badMRDD, df.schema)
    // suppression des lignes contenant des données manquantes dans n'importe quelle
    //colonne (deux variantes)
    val df1 = badMDF.na.drop()
    val df2 = badMDF.na.drop("any")
    // suppression des lignes contenant des données manquantes dans chaque colonne
    val df3 = badMDF.na.drop("all")
    // suppression des lignes dans lesquelles la colonne actor_name contenant des données
    //manquantes
    val df4 = badMDF.na.drop(Array("actor_name"))
    //remplir les valeurs manquantes par une valeur définie par l'utilisateur
    val df5 = badMDF.na.fill("EMPTY",Array("actor_name"))
    return df4
  }
  def describe_fct(df : DataFrame) : DataFrame = {
    val desc = df.describe("produced_year")
    return desc
  }
  case class Movie(actor_name:String, movie_title:String, produced_year:Long)
  def convert_DFtoDS (spark : SparkSession, df : DataFrame) : Dataset[Movie] = {
    import spark.implicits._
    val MovieDS = df.as[Movie]
    return MovieDS
  }
  def creer_Dataset1(spark : SparkSession) : Dataset[Movie] = {
    import spark.implicits._
    val data = Seq(Movie("John Doe", "Awesome Movie", 2018L),Movie("Mary Jane", "Awesome Movie",2018L))

    //1ère Option --> Passer la séquence comme argument de la fonction createDataset
    val MoviesDS1 = spark.createDataset(data)
    //2ème Option --> Créer un RDD et le mettre comme argument de la fonction createDataset
    //val RDD_Data = spark.sparkContext.parallelize(data)
    //val MoviesDS2 = spark.createDataset(RDD_Data)
    return MoviesDS1 //return MoviesDS2
  }
  def creer_Dataset_2(spark: SparkSession): Dataset[Movie] = {
    import spark.implicits._
    val data = Seq(Movie("John Doe", "Awesome Movie", 2018L), Movie("Mary Jane", "Awesome Movie", 2018L))
    val MoviesDS = data.toDS()
    return MoviesDS
  }

  def DS_filter(spark: SparkSession, df: DataFrame): Dataset[Movie] = {
    import spark.implicits._
    val moviesDS = df.as[Movie]
    // filtrer les films produits en 2000
    val DS_filter = moviesDS.filter(movie => movie.produced_year == 2000)

    return DS_filter
  }
/*  def DS_Manipulation(spark: SparkSession, df: DataFrame): Dataset[Movie] = {
    import spark.implicits._
    val moviesDS = df.as[Movie]
    // Effectuer une projection en utilisant la transformation map
    val titleYearPairDS = moviesDS.map(m => (m.movie_title, m.produced_year))
    //Tapez titleYearDS.printSchema pour consulter le schema du Dataset créé
    return titleYearPairDS
  }*/

 /* def DS_Compile_Error(spark : SparkSession , df : DataFrame) : Dataset[Movie] =
  {
    import spark.implicits._
    // 1er Cas --> le problème n'est pas détecté lors de la manipulation d'un DataFrame (jusqu'à l'exécution)
    val DF1 = df.select('movie_title - 'movie_title)
    // 2ème Cas --> le problème est détecté lors de la compilation
    val DS2 = df.as[Movie].map(m => m.movie_title - m.movie_title)
    //error: value - is not a member of String
    return DS2
    // return DF1
  }*/

 def SQL_Use_Case(spark: SparkSession, df: DataFrame): DataFrame = {
   import spark.implicits._
   // Enregistrer le DataFrame en tant que vue temporaire
   df.createOrReplaceTempView("Movies")
   //df.createOrReplaceGlobalTempView("movies_global")
   // Afficher la vue des films dans le catalogue
    println(spark.catalog.listTables().show())
   spark.catalog.listColumns("Movies").show()

   // Simple exécution d'une instruction SQL sans enregistrer la vue
   val selected_DF1 = spark.sql("select current_date() as today , 1 + 100 as value")

   //Sélection à partir de la vue
   val selected_DF2 = spark.sql("select * from Movies where actor_name like '%Jolie%' and produced_year > 2009")

   //mélanger des instructions SQL et des transformations sur les DataFrame
   val selected_DF3 = spark.sql("select actor_name, count(*) as count from movies group by actor_name").where('count > 20).orderBy('count.desc)

   // Utiliser une sous-requête pour déterminer le nombre de films produits chaque année.
   // Exploiter """ pour formater une instruction SQL multiligne
   val selected_DF4 = spark.sql("""select produced_year, count(*) as count from (select distinct movie_title, produced_year from movies) group by produced_year""".stripMargin).orderBy('count.desc)

   return selected_DF4 // selected_DF3 //selected_DF2 //selected_DF1
 }

  def sauvegarde_df(spark: SparkSession, df: DataFrame): Unit = {
    // Ecrire les données dans le format CSV, en utilisant # comme délimiteur
    df.write.format("csv").option("sep", "#").save("C:/Users/Lenovo/Desktop/big data analyst/big data 2/tp/sql_spark/CSV_OUTPUT")
    // Ecrire les données dans le format CSV, en utilisant ; comme délimiteur avec overwrite comme mode de sauvegarde
    df.write.format("csv").mode("overwrite").option("sep", ";").save("C:/Users/Lenovo/Desktop/big data analyst/big data 2/tp/sql_spark/CSV_OUTPUT_2")

    //Écrire les données en utilisant la partition par la colonne produced_year
    df.write.format("csv").mode("overwrite").option("sep",";").partitionBy("produced_year").save("C:/Users/Lenovo/Desktop/big data analyst/big data 2/tp/sql_spark/CSV_OUTPUT_3")
  }
  def createDF_CSV_Flight(spark: SparkSession, path: String): DataFrame = {
    //val movies = spark.read.option("header","true").csv(path)
    val flightsDF = spark.read.format("csv").option("header","true")
      .option("inferSchema", "true").load(path)
       flightsDF.cache()
    println(flightsDF.count())
    flightsDF.printSchema()
    return flightsDF
  }
  def count_agg(spark : SparkSession, df : DataFrame) : DataFrame = {
    val countDF = df.select(count("ORIGIN_AIRPORT"), count("DESTINATION_AIRPORT")
      .as("Dest_count"))
    return countDF
  }
  def countDistinct_agg(spark: SparkSession, df: DataFrame): DataFrame = {
    val countDisDF = df.select(countDistinct("ORIGIN_AIRPORT"),
    countDistinct("DESTINATION_AIRPORT").as("Dest_count"),count("*"))
    return countDisDF
  }
  def minMax_agg(spark: SparkSession, df: DataFrame): DataFrame = {
    val minmaxDF = df.select(min("TAXI_IN"), max("TAXI_IN"))
    return minmaxDF
  }
  def sum_agg(spark: SparkSession, df: DataFrame): DataFrame = {
    val sumDF = df.select(sum("TAXI_IN"))
    return sumDF
  }
  def sumDistinct_agg(spark: SparkSession, df: DataFrame): DataFrame = {
    val sumDDF = df.select(sum_distinct(col("TAXI_IN")))
    return sumDDF
  }
  def avg_agg(spark: SparkSession, df: DataFrame): DataFrame = {
    val avgDF = df.select(avg("TAXI_IN"))
    return avgDF
  }

  def agg_byGroup(spark: SparkSession, df: DataFrame): DataFrame = {
    val aggGrDF = df.groupBy("origin_airport").count()
    return aggGrDF
  }
  def agg_byGroup_2(spark: SparkSession, df: DataFrame): DataFrame = {
    val aggGrDF2 = df.groupBy("state","city").count()
    .where(col("state") === "CA").orderBy(col("count").desc)
    return aggGrDF2
  }

  def byGroup(spark: SparkSession, df: DataFrame): DataFrame = {
    val aggGrDF2 = df.groupBy("state", "city").count()
      .orderBy(col("count").desc)
    return aggGrDF2
  }
  def agg_multiple(spark: SparkSession, df: DataFrame): DataFrame = {
    val aggmultiDF = df.groupBy("origin_airport").agg(count("TAXI_IN").as("nbr"), min("TAXI_IN"), max("TAXI_IN"), sum("TAXI_IN"))
    return aggmultiDF
  }
  case class Student(name: String, gender: String, weight: Int, graduation_year: Int)
  def pivot_fct(spark : SparkSession) : DataFrame = {
    import spark.implicits._
    val studDF = Seq(Student("John", "M", 180, 2015),Student("Mary", "F", 110, 2015),Student("Derek", "M", 200, 2015), Student("Julie", "F", 109, 2015)
      ,Student("Allison", "F", 105, 2015),Student("kirby", "F", 115, 2016), Student("Jeff", "M", 195, 2016)).toDF()
    //calculer le poids moyen selon le sexe par année d'obtention du diplôme
    val resultDF = studDF.groupBy("graduation_year").pivot("gender").avg("weight")
    val resultDF_2 = studDF.groupBy("graduation_year").pivot("gender").agg(min("weight").as("min"), max("weight").as("max"), avg("weight").as("avg"))
    val resultDF_3 = studDF.groupBy("graduation_year").pivot("gender", Seq("M")).agg( min("weight").as("min"),max("weight").as("max"),avg("weight").as("avg"))
    return resultDF
  }
  case class Employee(first_name: String, dept_no: Long)
  case class Dept(id: Long, name: String)

  def jointures_fct(spark : SparkSession) : DataFrame = { import spark.implicits._
    val empDF = Seq(Employee("John", 31),Employee("Jeff", 33),Employee("Mary", 33), Employee("Mandy", 34),Employee("Julie", 34),Employee("Kurt",
      null.asInstanceOf[Long])).toDF()
    val deptDF = Seq(Dept(31L, "Sales"),Dept(33L, "Engineering"), Dept(34L, "Finance"),Dept(35L, "Marketing")).toDF()

    // Effectuer une jointure interne par l'ID de service
    // Définir l'expression de jointure de la comparaison d'égalité
    val deptJoinExpression = empDF.col("dept_no") === deptDF.col("id")
    val joinDF_1 = empDF.join(deptDF, deptJoinExpression, "inner")
    // using SQL
    // Enregistrer les en tant que vues afin que nous puissions utiliser SQL pour effectuer des jointures
    empDF.createOrReplaceTempView("employees")
    deptDF.createOrReplaceTempView("departments")
    val joinDF_2 = spark.sql("select * from employees JOIN departments on dept_no == id")
    // une version plus courte de l'expression de jointure
    val DF1 = empDF.join(deptDF, 'dept_no === 'id)
    // Spécifie l'expression de jointure dans la transformation de jointure
    val DF2 = empDF.join(deptDF, empDF.col("dept_no") === deptDF.col("id"))
    // Spécifie l'expression de jointure en utilisant la transformation Where
    val DF3 = empDF.join(deptDF).where('dept_no === 'id)

    return joinDF_1 //return joinDF_2
  }
  def left_outer_join(spark: SparkSession): DataFrame = {
    import spark.implicits._
    val empDF = Seq(Employee("John", 31), Employee("Jeff", 33), Employee("Mary", 33),
      Employee("Mandy", 34), Employee("Julie", 34), Employee("Kurt", null.asInstanceOf[Long])).toDF()
    val deptDF = Seq(Dept(31L, "Sales"), Dept(33L, "Engineering"), Dept(34L, "Finance"), Dept(35L, "Marketing")).toDF()

    val joinDF_1 = empDF.join(deptDF, 'dept_no === 'id, "left_outer")
    // using SQL empDF.createOrReplaceTempView("employees") deptDF.createOrReplaceTempView("departments")
    val joinDF_2 = spark.sql("select * from employees LEFT OUTER JOIN departments on dept_no == id")

    return joinDF_1 //return joinDF_2
  }
  def right_outer_join(spark: SparkSession): DataFrame = {
    import spark.implicits._
    val empDF = Seq(Employee("John", 31), Employee("Jeff", 33), Employee("Mary", 33),
      Employee("Mandy", 34), Employee("Julie", 34), Employee("Kurt", null.asInstanceOf[Long])).toDF()
    val deptDF = Seq(Dept(31L, "Sales"), Dept(33L, "Engineering"), Dept(34L, "Finance"), Dept(35L, "Marketing")).toDF()
    val joinDF_1 = empDF.join(deptDF, 'dept_no === 'id, "right_outer")
    // using SQL empDF.createOrReplaceTempView("employees") deptDF.createOrReplaceTempView("departments")
    val joinDF_2 = spark.sql("select * from employees RIGHT OUTER JOIN departments on dept_no == id")
    return joinDF_1 //return joinDF_2
  }
  def full_outer_join(spark: SparkSession): DataFrame = {
    import spark.implicits._
    val empDF = Seq(Employee("John", 31), Employee("Jeff", 33), Employee("Mary", 33),
      Employee("Mandy", 34), Employee("Julie", 34), Employee("Kurt", null.asInstanceOf[Long])).toDF()
    val deptDF = Seq(Dept(31L, "Sales"), Dept(33L, "Engineering"), Dept(34L, "Finance"), Dept(35L, "Marketing")).toDF()
    val joinDF_1 = empDF.join(deptDF, 'dept_no === 'id, "outer")
    // using SQL empDF.createOrReplaceTempView("employees") deptDF.createOrReplaceTempView("departments")
    val joinDF_2 = spark.sql("select * from employees RIGHT OUTER JOIN departments on dept_no == id")
    return joinDF_1 //return joinDF_2
  }
  def left_anti_join(spark: SparkSession): DataFrame = {
    import spark.implicits._
    val empDF = Seq(Employee("John", 31), Employee("Jeff", 33), Employee("Mary", 33),
      Employee("Mandy", 34), Employee("Julie", 34), Employee("Kurt", null.asInstanceOf[Long])).toDF()
    val deptDF = Seq(Dept(31L, "Sales"), Dept(33L, "Engineering"), Dept(34L, "Finance"), Dept(35L, "Marketing")).toDF()

    val joinDF_1 = empDF.join(deptDF, 'dept_no === 'id, "left_anti")
    // using SQL empDF.createOrReplaceTempView("employees") deptDF.createOrReplaceTempView("departments")
    val joinDF_2 = spark.sql("select * from employees LEFT ANTI JOIN departments on dept_no == id")
    return joinDF_1 //return joinDF_2
  }
  def left_semi_join(spark: SparkSession): DataFrame = {
    import spark.implicits._
    val empDF = Seq(Employee("John", 31), Employee("Jeff", 33), Employee("Mary", 33),
      Employee("Mandy", 34), Employee("Julie", 34), Employee("Kurt", null.asInstanceOf[Long])).toDF()
    val deptDF = Seq(Dept(31L, "Sales"), Dept(33L, "Engineering"), Dept(34L, "Finance"), Dept(35L, "Marketing")).toDF()
    val joinDF_1 = empDF.join(deptDF, 'dept_no === 'id, "left_semi")
    // using SQL empDF.createOrReplaceTempView("employees") deptDF.createOrReplaceTempView("departments")
    val joinDF_2 = spark.sql("select * from employees LEFT SEMI JOIN departments on dept_no == id")
    return joinDF_1 //return joinDF_2
  }
  def cross_join(spark: SparkSession): DataFrame = {
    import spark.implicits._
    val empDF = Seq(Employee("John", 31), Employee("Jeff", 33), Employee("Mary", 33),
      Employee("Mandy", 34), Employee("Julie", 34), Employee("Kurt", null.asInstanceOf[Long])).toDF()
    val deptDF = Seq(Dept(31L, "Sales"), Dept(33L, "Engineering"), Dept(34L, "Finance"), Dept(35L, "Marketing")).toDF()
    val joinDF_1 = empDF.crossJoin(deptDF)
    empDF.createOrReplaceTempView("employees")
    deptDF.createOrReplaceTempView("departments")
    val joinDF_2 = spark.sql("select * from employees CROSS JOIN departments")
    return joinDF_1 //return joinDF_2
  }



  def date_time_conversion(spark : SparkSession) : DataFrame = {
    import spark.implicits._
    // les deux dernières colonnes ne suivent pas le format de date par défaut
    val DF1 = Seq((1, "2018-01-01", "2018-01-01 15:04:58:865","01-01-2018", "12-05-2017 45:50")).toDF("id", "date", "timestamp", "date_str", "ts_str")
    // convertir ces chaînes en date et horodatage et spécifiez un format de date et d'horodatage personnalisé
    val ResultDF1 = DF1.select(to_date('date).as("date1"), to_timestamp('timestamp).as("ts1"), to_date('date_str, "MM-dd- yyyy").as("date2"), to_timestamp('ts_str, "MM-dd-yyyy mm:ss").as("ts2"))
    val ResultDF2 = ResultDF1.select(date_format('date1, "dd-MM-YYYY").as("date_str"), date_format ('ts1, "dd-MM-YYYY HH:mm:ss").as("ts_str"))
    val empData = Seq(("John", "2016-01-01", "2017-10-15"), ("May", "2017-02-06", "2017-12 - 25")).toDF("name", "join_date", "leave_date")
    // effectuer des calculs de date et de mois
    val ResultDF3 = empData.select('name, datediff('leave_date, 'join_date).as("days"), months_between('leave_date, 'join_date).as("months"), last_day('leave_date).as("last_day_of_mon"))
    val dateData = Seq(("2023-01-01")).toDF("new_year")
    val ResultDF4 = dateData.select(date_add('new_year, 14).as("mid_month"), date_sub('new_year, 1).as("new_year_eve"), next_day('new_year, "Fri").as("next_mon"))
    val IndependenceDateDF = Seq(("2023-11-18 10:35:55")).toDF("date")
    val ResultDF5 = IndependenceDateDF.select(year('date).as("year"),
      quarter('date).as("quarter"), functions.month('date).as("month"), weekofyear('date).as("woy"),
      dayofmonth('date).as("dom"), dayofyear('date).as("doy"), hour('date).as("hour"), minute('date).as("minute"), second('date).as("second"))

    ResultDF1.printSchema()
    return ResultDF1
  }
  def string_manipulation(spark: SparkSession): DataFrame = {
    import spark.implicits._
    // Différentes façons de transformer une chaîne avec des fonctions de chaîne intégrées
    val DF = Seq((" Spark ")).toDF("name")
    // découpage - suppression des espaces sur le côté gauche, le côté droit d'une chaîne ou les deux
    val result1 = DF.select(functions.trim('name).as("trim"),ltrim('name).as("ltrim"), rtrim('name).as("rtrim"))

    //remplissage d'une chaîne à une longueur spécifiée avec une chaîne de remplissage donnée
    val result2 = DF.select(functions.trim('name).as("trim"))
      .select(lpad('trim,8,"-").as("lpad"),rpad('trim, 8, "=").as("rpad"))

    // transformez une chaîne avec concaténation, majuscule, minuscule et inverse
    val sparkDF = Seq(("Spark", "is", "awesome")).toDF("subject", "verb", "adj")
    val result3 = sparkDF.select(concat_ws(" ",'subject,'verb,'adj).as("sentence"))
      .select(lower('sentence).as("lower"),upper('sentence).as("upper"), initcap('sentence).as("initcap"),functions.reverse('sentence).as("reverse"))

    // traduire un caractère à un autre
    val result4 = sparkDF.select('subject, translate('subject,"ar", "oc")
      .as("translate"))

    val DF2 = Seq(("A fox saw a crow sitting on a tree singing")).toDF("sentence")
    val result5 = DF2.select(regexp_extract('sentence, "[a-z]*o[xw]", 0)
      .as("substring")) //return fox

    //Remplacez « fox » et « crow » par « animal »
    val result6 = DF2.select(regexp_replace('sentence, "fox|crow", "animal").as("new_sentence"))


    return result4 //return result3 // return result2 // return result1
  }

  def collection_manipulation(spark : SparkSession) : DataFrame = {
    import spark.implicits._
    val tasksDF = Seq(("Monday", Array("Pick Up John", "Buy Milk", "Pay Bill"))).toDF("day", "tasks")
    tasksDF.printSchema()
    // récupèrer la taille du tableau, le trier et vérifier si une valeur particulière existe dans le tableau
    val resultDF1 = tasksDF.select('day, functions.size('tasks).as("size"), sort_array('tasks).as("sorted_tasks"),
      array_contains('tasks, "Pay Bill").as("payBill"))

    // Appliquer explode pour créer une nouvelle ligne pour chaque élément du tableau
    val resultDF2 = tasksDF.select('day, explode('tasks))

    return resultDF2 //return resultDF1
  }
  case class StudentGrade(name:String, score:Int)

  // créer une fonction pour convertir la note en note en format lettre
  def letterGrade(score: Int): String = {
    score match {
    case score if score > 100 => "Cheating" case score if score >= 90 => "A"
    case score if score >= 80 => "B" case score if score >= 70 => "C" case _ => "F"
  }
  }

  def udf_manipulation(spark : SparkSession) : DataFrame = {
    import spark.implicits._
    val studentDF = Seq(StudentGrade("Joe", 85), StudentGrade("Jane",90), StudentGrade("Mary", 55)).toDF()
    studentDF.createOrReplaceTempView("students")
    // s'enregistrer en tant qu'UDF
    val letterGradeUDF = udf(letterGrade(_:Int):String)
    // utiliser l'UDF pour convertir les scores en notes alphabétiques
    val result = studentDF.select('name, 'score, letterGradeUDF('score).as("grade"))

    // s'enregistrer en tant qu'UDF à utiliser dans SQL
    spark.sqlContext.udf.register("letterGrade", letterGrade(_: Int): String)
    val resultSQL = spark.sql("select name, score, letterGrade(score) as grade from students")

    return result //return resultSQL
  }
  def rollup_analysis(spark : SparkSession, df : DataFrame) : DataFrame = {
    import spark.implicits._
    // filtrer les données vers une taille plus petite pour faciliter la visualisation du résultat des cumuls
    val twoStatesSummary = df.select('state, 'city, 'count)
      .where('state === "CA" || 'state === "NY")
      .where('count > 1 && 'count < 20)
      .where('city =!= "White Plains")
      .where('city =!= "Newburgh")
      .where('city =!= "Mammoth Lakes")
      .where('city =!= "Ontario")

    // voyons à quoi ressemblent les données
    twoStatesSummary.orderBy('state).show()

    // Effectuer le rollup par état, ville, puis calcule la somme du nombre
    val rollup_result = twoStatesSummary.rollup('state, 'city).agg(sum("count") as "total")
      .orderBy('state.asc_nulls_last, 'city.asc_nulls_last)

    return rollup_result
  }

  def cube_analysis(spark: SparkSession, df: DataFrame): DataFrame = {
    import spark.implicits._
    // filtrer les données vers une taille plus petite pour faciliter la visualisation du résultat des cumuls
    val twoStatesSummary = df.select('state, 'city, 'count)
      .where('state === "CA" || 'state === "NY")
      .where('count > 1 && 'count < 20)
      .where('city =!= "White Plains")
      .where('city =!= "Newburgh")
      .where('city =!= "Mammoth Lakes")
      .where('city =!= "Ontario")

    // voyons à quoi ressemblent les données
    twoStatesSummary.orderBy('state).show()

    // Effectuer le rollup par état, ville, puis calcule la somme du nombre
    val cube_result = twoStatesSummary.cube('state, 'city).agg(sum("count") as "total")
      .orderBy('state.asc_nulls_last, 'city.asc_nulls_last)

    return cube_result
  }

  def main(args: Array[String]): Unit = {
val spark=build()
    /*createDF_fromSeq(spark).printSchema()*/
   /* createDF_fromSeq_V2(spark).printSchema()*/
   /* createDF_byRow(spark).printSchema()*/
    /*createDF_CSV(spark,"C:/Users/Lenovo/Desktop/big data analyst/big data 2/tp/Movies.csv").printSchema()*/
   /* createDF_JSON(spark,"C:/Users/Lenovo/Desktop/big data analyst/big data 2/tp/movies.json").printSchema()*/
    createDF_DB(spark).printSchema()
   val df=createDF_CSV(spark,"C:/Users/Lenovo/Desktop/big data analyst/big data 2/tp/Movies.csv")
    /*add_column(df).show(5)*/
     /*renommer_column(df).show(5)*/
     /*drop_column(df).printSchema()*/
    /*sample_Trans(df).show()*/
    /* println(randomSplit_Trans(df)(0).count())
     println(randomSplit_Trans(df)(1).count())
     println(randomSplit_Trans(df)(2).count())
     println(df.count())*/
    /*select_Version(spark, df).show(5)*/
    /*selectExpr_Version(df).show(5)*/
    /*selectExpr_Version2(df).show(5)*/
    /*filtrage(spark, df).show(5)*/
    /*drop_fct(df).show()*/
    /*tri_fcts(spark, df).show(5)*/

    /*limit_fct(spark, df).show()*/
    /*union_fct(spark,df).show(5)*/
    /*drop_nullEntries(spark,df).show(5)*/
    /*describe_fct(df).show()*/
   /* convert_DFtoDS(spark,df).show(5)*/
    /*creer_Dataset1(spark).show(5)*/
    /*creer_Dataset_2(spark).show(5)*/
    /*println(DS_filter(spark, df).first().actor_name)*/
    /*DS_Manipulation(spark,df).printSchema()*/
    /*SQL_Use_Case(spark,df).show(5)*/
    /*DS_Compile_Error(spark,df).show()*/
    /*sauvegarde_df(spark,df)*/
    /*val df1=createDF_CSV_Flight(spark,"C:/Users/Lenovo/Desktop/big data analyst/big data 2/tp/flights.csv")
    val df2=createDF_CSV_Flight(spark,"C:/Users/Lenovo/Desktop/big data analyst/big data 2/tp/airports.csv")*/

    /*count_agg(spark, df1).show()*/
    /*countDistinct_agg(spark, df1).show()*/
    /*minMax_agg(spark, df1).show()*/
    /*sum_agg(spark, df1).show()*/
    /*sumDistinct_agg(spark, df1).show()*/
    /*avg_agg(spark, df1).show()*/
    /*agg_byGroup(spark, df1).show()*/
    /*agg_byGroup_2(spark,df2).show()*/
    /*agg_multiple(spark, df1).show()*/
    /*pivot_fct(spark).show()*/
    /*jointures_fct(spark).show()*/
    /*left_outer_join(spark).show()*/
    /*right_outer_join(spark).show()*/
    /*full_outer_join(spark).show()*/
   /* left_anti_join(spark).show()*/
    /*left_semi_join(spark).show()*/
    /*cross_join(spark).show()*/
    /*date_time_conversion(spark).show()*/
    /*string_manipulation(spark).show()*/
    /*collection_manipulation(spark).show()*/
    /*udf_manipulation(spark).show()*/
    /*byGroup(spark,df2).show()*/
    /*val df3=byGroup(spark,df2)*/
   /* rollup_analysis(spark,df3).show()
    cube_analysis(spark,df3).show()*/
  }
}

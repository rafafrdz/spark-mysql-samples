import java.util.Properties
import org.apache.spark.sql.SparkSession

object sparkMySQL {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("KillGuillermo")
      .getOrCreate()

    // Omitimos parte de la información del Log en Intellij
    spark.sparkContext.setLogLevel("WARN")

// EJERCICIO 1
    def archivo(anno:Int)={
      spark
        .read
        .option("header",true)
        .option("inferScheme",true)
        .option("sep",",")
        .csv(s"file:///opt/datasets/csv/lapd_calls_for_service_$anno.csv")
    }

    val incidente =
      spark
      .read
      .option("header",true)
      .option("inferScheme",true)
      .option("sep",",")
      .csv(s"file:///opt/datasets/csv/sf_pd_incident_reports.csv").limit(10)

    val (archivo15, archivo16, archivo17) =
      ( archivo(2015).limit(10),
        archivo(2016).limit(10),
        archivo(2017).limit(10))

    val propiedades = new Properties()
    propiedades.put("user","root")
    propiedades.put("password","root")

   archivo15.write.jdbc("jdbc:mysql://localhost:3306/lapd","data2015",propiedades)
   archivo16.write.jdbc("jdbc:mysql://localhost:3306/lapd","data2016",propiedades)
   archivo17.write.jdbc("jdbc:mysql://localhost:3306/lapd","data2017",propiedades)
   incidente.write.jdbc("jdbc:mysql://localhost:3306/lapd","incidenteReports",propiedades)

  // EJERCICIO2
    var union = archivo15.union(archivo16.union(archivo17)).limit(10)

    // Tenemos que renombrar las columnas del dataset para guardarlo
    // en los formatos que no permitan espacios en blanco
    for (i <- 0 to union.columns.length-1){
      union = union.withColumnRenamed(union.columns(i),union.columns(i).replaceAll(" ",""))
    }
    //union.limit(10).show()
    // Conexión con el hdfs y guardamos en los distintos formatos
    // Previamente debemos haber creado el directorio en el hdfs
    union.write.csv("hdfs://192.168.8.100:7232/datasets/jdbc/fechasAnnion.csv")
    union.write.parquet("hdfs://192.168.8.100:7232/datasets/jdbc/fechasAnnion.parquet")
    union.write.json("hdfs://192.168.8.100:7232/datasets/jdbc/fechasAnnion.json")
    union.write.orc("hdfs://192.168.8.100:7232/datasets/jdbc/fechasAnnion.orc")
  }

}

package rs.raf.pds.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class air_quality_bg {

    public static void main(String[] args) {

        // === Kreiranje Spark sesije ===
    	
        SparkSession spark = SparkSession.builder()
                .appName("air_quality")
                .master("local[*]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("OFF");
        
        // === Učitavanje dataset-a ===
        
        Dataset<Row> aqDF = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("dataset/air_quality_belgrade.csv");

        Dataset<Row> aq = aqDF
                .withColumn("value", col("value").cast("double"))
                .withColumn("datetime", col("datetimeUtc"));

        // === Izdvajanje temperature i vlažnosti ===
        Dataset<Row> tempDF = aq
                .filter(col("parameter").equalTo("temperature"))
                .select(col("datetime"), col("value").alias("temperature"));

        Dataset<Row> rhDF = aq
                .filter(col("parameter").equalTo("relativehumidity"))
                .select(col("datetime"), col("value").alias("humidity"));

        // === Filtriranje samo PM2.5 čestica ===
        Dataset<Row> pmDF = aq
                .filter(col("parameter").equalTo("pm25"))
                .select(col("datetime"), col("parameter"), col("value"));

        // === Spajanje PM2.5 sa temperaturom i vlažnošću ===
        Dataset<Row> joined = pmDF
                .join(tempDF, "datetime")
                .join(rhDF, "datetime");

        
        // === Dodavanje vremenskih kolona (sat, datum, mesec, godišnje doba) ===
        
        Dataset<Row> withHour = joined.withColumn("hour", hour(to_timestamp(col("datetime"))));

        Dataset<Row> withDate = joined.withColumn("date", to_date(to_timestamp(col("datetime"))));

        Dataset<Row> withMonth = joined.withColumn("month", month(to_timestamp(col("datetime"))));

        
        // === Kategorizacija temperatura po zonama ===
        
        Dataset<Row> withZones = joined.withColumn(
                "temp_zone",
                when(col("temperature").leq(15), lit("do 15°C"))
                        .when(col("temperature").gt(15).and(col("temperature").leq(30)), lit("15–30°C"))
                        .otherwise(lit("preko 30°C"))
        );

        Dataset<Row> avgByZone = withZones
                .groupBy("temp_zone")
                .agg(avg("value").alias("avg_pm25"))
                .orderBy(
                        when(col("temp_zone").equalTo("do 15°C"), 1)
                                .when(col("temp_zone").equalTo("15–30°C"), 2)
                                .otherwise(3)
                );

        System.out.println("\nProsečne koncentracije PM2.5 čestica po temperaturnim zonama:");
        avgByZone.show(false);

        
        // === Prosečne vrednosti PM2.5 po mesecima ===
        
        Dataset<Row> monthlyAvgPM = withMonth
                .groupBy("month")
                .agg(avg("value").alias("avg_pm25"))
                .orderBy("month");

        System.out.println("\nProsečne koncentracije PM2.5 čestica po mesecima:");
        monthlyAvgPM.show(12, false);

        
        // === Prosečne vrednosti PM2.5 po godišnjim dobima ===
        
        Dataset<Row> withSeason = withMonth.withColumn("season",
                when(col("month").isin(3, 4, 5), "Proleće")
                        .when(col("month").isin(6, 7, 8), "Leto")
                        .when(col("month").isin(9, 10, 11), "Jesen")
                        .otherwise("Zima")
        );
        
        Dataset<Row> avgBySeason = withSeason
                .groupBy("season")
                .agg(avg("value").alias("avg_pm25"))
                .orderBy(
                        when(col("season").equalTo("Proleće"), 1)
                                .when(col("season").equalTo("Leto"), 2)
                                .when(col("season").equalTo("Jesen"), 3)
                                .otherwise(4)
                );

        System.out.println("\nProsečne vrednosti PM2.5 po godišnjim dobima:");
        avgBySeason.show(false);

       
        // === Prosečne vrednosti PM2.5 po satima u danu ===
        
        Dataset<Row> hourlyAvgPM = withHour
                .groupBy("hour")
                .agg(avg("value").alias("avg_pm25"))
                .orderBy("hour");

        System.out.println("\nProsečne koncentracije PM2.5 čestica po satima tokom dana:");
        hourlyAvgPM.show(24, false);

        
        // === Grupisanje PM2.5 po opsezima vlažnosti ===
        
        Dataset<Row> withHumidityRange = joined
                .withColumn("humidity_range",
                        concat(
                                (floor(col("humidity").divide(10)).multiply(10)).cast("int"),
                                lit("–"),
                                (floor(col("humidity").divide(10)).multiply(10).plus(10)).cast("int"),
                                lit("%")
                        )
                );

        Dataset<Row> avgPMByHumidityRange = withHumidityRange
                .groupBy("humidity_range")
                .agg(avg("value").alias("avg_pm25"))
                .orderBy("humidity_range");

        System.out.println("\nProsečne koncentracije PM2.5 čestica po opsezima relativne vlažnosti:");
        avgPMByHumidityRange.show(false);
        
        
        // === Najzagađeniji dan po prosečnom PM2.5 + prosečna temperatura i vlažnost ===
        
        Dataset<Row> dailyPmTempRh = withDate
                .groupBy("date")
                .agg(
                        avg("value").alias("avg_pm25"),
                        max("value").alias("max_pm25"),
                        avg("temperature").alias("avg_temp"),
                        avg("humidity").alias("avg_humidity")
                );

        Row worst = dailyPmTempRh
                .orderBy(col("avg_pm25").desc())
                .limit(1)
                .collectAsList()
                .get(0);

        java.sql.Date worstDate = worst.getAs("date");
        Double worstPm25 = worst.getAs("avg_pm25");
        Double maxPm25 = worst.getAs("max_pm25");
        Double worstTemp = worst.getAs("avg_temp");
        Double worstHumidity = worst.getAs("avg_humidity");

        System.out.println("\nNajzagađeniji dan u posmatranom periodu: " + worstDate
                + " | avg_pm25=" + worstPm25
                + " | max_pm25=" + maxPm25
                + " | avg_temp=" + worstTemp
                + " | avg_humidity=" + worstHumidity);


        dailyPmTempRh.filter(col("date").equalTo(lit(worstDate))).show(false);
        
        
        // === Kategorizacija dana po prosečnoj vrednosti PM2.5 samo za zimu ===

        Dataset<Row> winterDays = withSeason
                .filter(col("season").equalTo("Zima"));

        Dataset<Row> dailyWinterAvgPM25 = winterDays
                .withColumn("date", to_date(to_timestamp(col("datetime"))))
                .groupBy("date")
                .agg(avg("value").alias("avg_pm25"))
                .withColumn(
                        "pm25_category",
                        when(col("avg_pm25").leq(12), "dobar")
                                .when(col("avg_pm25").gt(12).and(col("avg_pm25").leq(35.4)), "umeren")
                                .when(col("avg_pm25").gt(35.4).and(col("avg_pm25").leq(55.4)), "nezdrav za osetljive")
                                .when(col("avg_pm25").gt(55.4).and(col("avg_pm25").leq(150.4)), "nezdrav")
                                .when(col("avg_pm25").gt(150.4).and(col("avg_pm25").leq(250.4)), "vrlo nezdrav")
                                .otherwise("opasan")
                );

        Dataset<Row> winterDaysPerCategory = dailyWinterAvgPM25
                .groupBy("pm25_category")
                .count()
                .orderBy("pm25_category");

        System.out.println("\nBroj zimskih dana po kategorijama kvaliteta vazduhа:");
        winterDaysPerCategory.show(false);
        
            spark.stop();
    }
}
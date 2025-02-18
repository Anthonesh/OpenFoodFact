package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import java.io.File;

public class App {
    public static void main(String[] args) {
        try {
            // Configuration de Spark
            SparkSession spark = SparkSession.builder()
                    .appName("OpenFoodFacts Data Integration")
                    .master("local[*]")
                    .config("spark.sql.warehouse.dir", "file:///C:/temp/spark-warehouse")
                    .config("spark.hadoop.hadoop.tmp.dir", "file:///C:/temp/hadoop-tmp")
                    .config("spark.hadoop.hadoop.home.dir", "C:/hadoop")
                    .getOrCreate();

            String filePath = new File("src/main/resources/en.openfoodfacts.org.products.csv").getAbsolutePath();

            DataLoader loader = new DataLoader(spark);
            Dataset<Row> data = loader.loadCSV(filePath);
            loader.exploreData(data);

            DataCleaner cleaner = new DataCleaner();
            Dataset<Row> cleanedData = cleaner.cleanData(data);

            DataTransformer transformer = new DataTransformer();
            Dataset<Row> transformedData = transformer.transformData(cleanedData);

            // Agréger les données
            DataAggregator aggregator = new DataAggregator();
            Dataset<Row> topBrands = aggregator.getTopBrands(transformedData);
            Dataset<Row> avgSugarAndEnergy = aggregator.getAverageSugarAndEnergyByCountry(transformedData);
            Dataset<Row> labelDistribution = aggregator.getLabelDistribution(transformedData);

            DataExporter exporter = new DataExporter();
            exporter.exportToCSV(transformedData, "C:/temp/transformed_data.csv");
            exporter.exportToCSV(topBrands, "C:/temp/top_brands.csv");
            exporter.exportToCSV(avgSugarAndEnergy, "C:/temp/avg_sugar_energy_by_country.csv");
            exporter.exportToCSV(labelDistribution, "C:/temp/label_distribution.csv");


            spark.stop();
        } catch (Exception e) {
            System.err.println("Erreur générale du programme : " + e.getMessage());
        }
    }
}

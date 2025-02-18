package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import java.io.FileNotFoundException;

public class DataLoader {
    private final SparkSession spark;

    public DataLoader(SparkSession spark) {
        this.spark = spark;
    }

    public Dataset<Row> loadCSV(String filePath) {
        try {
            System.out.println("Chargement du fichier : " + filePath);
            return spark.read()
                    .option("header", "true")
                    .option("inferSchema", "true")
                    .option("delimiter", "\t")
                    .csv(filePath);
        } catch (Exception e) {
            System.err.println("Erreur lors du chargement du fichier CSV : " + e.getMessage());
            return spark.emptyDataFrame();
        }
    }

    public void exploreData(Dataset<Row> data) {
        try {
            System.out.println("Exploration des données...");
            data.printSchema();
            data.show(10);
        } catch (Exception e) {
            System.err.println("Erreur lors de l'exploration des données : " + e.getMessage());
        }
    }
}

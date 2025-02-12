package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DataLoader {
    private final SparkSession spark;

    public DataLoader(SparkSession spark) {
        this.spark = spark;
    }

    public Dataset<Row> loadCSV(String filePath) {
        return spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .option("delimiter", "\t")
                .csv(filePath);
    }

    public void exploreData(Dataset<Row> data) {
        System.out.println("Structure des données :");
        data.printSchema();
        System.out.println("Exemple de données :");
        data.show(10);
    }
}

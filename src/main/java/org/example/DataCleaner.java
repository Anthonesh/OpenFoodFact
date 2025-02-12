package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.*;

public class DataCleaner {
    public Dataset<Row> cleanData(Dataset<Row> data) {
        return data.filter(col("product_name").isNotNull()
                        .and(col("energy_100g").isNotNull())
                        .and(col("sugars_100g").isNotNull())
                        .and(col("fat_100g").isNotNull()))
                .na().fill(0, new String[]{"energy_100g", "sugars_100g", "fat_100g"})
                .na().fill("Unknown", new String[]{"brands", "countries"})
                .dropDuplicates()

                // 🟢 Uniformisation des noms de colonnes
                .withColumn("brands", lower(trim(col("brands"))))
                .withColumn("categories", lower(trim(col("categories"))))
                .withColumn("countries", lower(trim(col("countries"))));
    }
}


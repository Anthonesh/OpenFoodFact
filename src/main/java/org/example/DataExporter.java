package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class DataExporter {
    public void exportToCSV(Dataset<Row> data, String outputPath) {
        data.write()
                .option("header", "true")
                .option("delimiter", ";")
                .mode("overwrite")
                .csv(outputPath);
        System.out.println("✅ Exportation réussie : " + outputPath);
    }
}

package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class DataExporter {
    public void exportToCSV(Dataset<Row> data, String outputPath) {
        try {
            System.out.println("Exportation des données vers : " + outputPath);
            data.write()
                    .option("header", "true")
                    .option("delimiter", ";")
                    .mode("overwrite")
                    .csv(outputPath);
            System.out.println("Exportation réussie : " + outputPath);
        } catch (Exception e) {
            System.err.println("Erreur lors de l'exportation des données : " + e.getMessage());
        }
    }
}

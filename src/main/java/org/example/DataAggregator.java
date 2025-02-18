package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.*;

public class DataAggregator {
    public Dataset<Row> getTopBrands(Dataset<Row> data) {
        try {
            System.out.println("Calcul du top 10 des marques...");
            return data.filter(col("brands").isNotNull().and(col("brands").notEqual("")))
                    .groupBy("brands")
                    .count()
                    .orderBy(desc("count"))
                    .limit(10);
        } catch (Exception e) {
            System.err.println("Erreur lors du calcul des marques les plus représentées : " + e.getMessage());
            return data;
        }
    }

    public Dataset<Row> getAverageSugarAndEnergyByCountry(Dataset<Row> data) {
        try {
            System.out.println("Calcul de la teneur moyenne en sucre et énergie par pays...");
            return data.groupBy("countries")
                    .agg(avg("sugars_100g").alias("avg_sugars"),
                            avg("energy_100g").alias("avg_energy"));
        } catch (Exception e) {
            System.err.println("Erreur lors du calcul des moyennes nutritionnelles par pays : " + e.getMessage());
            return data;
        }
    }

    public Dataset<Row> getLabelDistribution(Dataset<Row> data) {
        try {
            System.out.println("Calcul de la distribution des produits par labels...");
            return data.groupBy("labels")
                    .count()
                    .orderBy(desc("count"));
        } catch (Exception e) {
            System.err.println("Erreur lors de l'analyse des labels : " + e.getMessage());
            return data;
        }
    }
}

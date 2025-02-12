package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.*;

public class DataAggregator {
    public Dataset<Row> getTopBrands(Dataset<Row> data) {
        return data.filter(col("brands").isNotNull().and(col("brands").notEqual(""))) // Filtre les valeurs vides
                .groupBy("brands")
                .count()
                .orderBy(desc("count"))
                .limit(10);
    }


    public Dataset<Row> getAverageSugarAndEnergyByCountry(Dataset<Row> data) {
        return data.groupBy("countries")
                .agg(avg("sugars_100g").alias("avg_sugars"),
                        avg("energy_100g").alias("avg_energy"));
    }

    public Dataset<Row> getLabelDistribution(Dataset<Row> data) {
        return data.groupBy("labels")
                .count()
                .orderBy(desc("count"));
    }
}

package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.*;

public class DataTransformer {

    // Méthode principale pour transformer les données
    public Dataset<Row> transformData(Dataset<Row> data) {
        data = data.withColumn("nutri_score",
                when(col("sugars_100g").leq(5).and(col("fat_100g").leq(3)).and(col("salt_100g").leq(0.3)), lit("A"))
                        .when(col("sugars_100g").between(5, 10).or(col("fat_100g").between(3, 10)), lit("B"))
                        .otherwise(lit("C")));

        data = data.withColumn("is_healthy", col("nutri_score").equalTo("A"));

        data = data.withColumn("ingredient_count", size(split(col("ingredients_text"), ",")));

        return filterByCountry(data, "france");
    }


    // Nouvelle méthode pour filtrer par pays (réutilisable)
    public Dataset<Row> filterByCountry(Dataset<Row> data, String country) {
        return data.filter(col("countries").isNotNull()
                .and(array_contains(split(lower(col("countries")), ","), country.toLowerCase())));
    }
}

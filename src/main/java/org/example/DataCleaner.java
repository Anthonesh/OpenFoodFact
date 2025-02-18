package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.*;

public class DataCleaner {
    public Dataset<Row> cleanData(Dataset<Row> data) {
        try {
            System.out.println("🧹 Début du nettoyage des données...");

            // ✅ Sélectionner uniquement les colonnes utiles
            data = selectUsefulColumns(data);

            // ✅ Suppression des lignes avec valeurs nulles essentielles
            data = data.filter(col("product_name").isNotNull()
                    .and(col("energy_100g").isNotNull())
                    .and(col("sugars_100g").isNotNull())
                    .and(col("fat_100g").isNotNull()));

            // ✅ Remplissage des valeurs manquantes
            data = data.na().fill(0, new String[]{"energy_100g", "sugars_100g", "fat_100g"})
                    .na().fill("Unknown", new String[]{"brands", "countries"});

            // ✅ Suppression des doublons
            data = data.dropDuplicates();

            // ✅ Standardisation des données
            data = data.withColumn("brands", lower(trim(col("brands"))))
                    .withColumn("categories", lower(trim(col("categories"))))
                    .withColumn("countries", lower(trim(col("countries"))));

            System.out.println("✅ Nettoyage terminé !");
            return data;
        } catch (Exception e) {
            System.err.println("❌ Erreur lors du nettoyage des données : " + e.getMessage());
            return data;
        }
    }

    private Dataset<Row> selectUsefulColumns(Dataset<Row> data) {
        try {
            return data.select(
                    "product_name",
                    "brands",
                    "categories",
                    "countries",
                    "ingredients_text",
                    "energy_100g",
                    "sugars_100g",
                    "fat_100g",
                    "salt_100g",
                    "labels",
                    "packaging"
            );
        } catch (Exception e) {
            System.err.println("❌ Erreur lors de la sélection des colonnes utiles : " + e.getMessage());
            return data;
        }
    }
}

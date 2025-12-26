# Databricks notebook source
# Databricks notebook source
import os

storage_account = "adlsgen2covid"

# ================================
# Azure AD Authentication (SAFE)
# ================================

spark.conf.set(
    f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net",
    "OAuth"
)

spark.conf.set(
    f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net",
    "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
)

# Client ID – read from environment variable
spark.conf.set(
    f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net",
    os.getenv("AZURE_CLIENT_ID")
)

# Client Secret – NEVER hardcode
spark.conf.set(
    f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net",
    os.getenv("AZURE_CLIENT_SECRET")
)

# Tenant endpoint – parameterized
spark.conf.set(
    f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net",
    f"https://login.microsoftonline.com/{os.getenv('AZURE_TENANT_ID')}/oauth2/token"
)


# COMMAND ----------

storage_account = "adlsgen2covid"


# COMMAND ----------

from pyspark.sql.functions import *

df_raw_population = (
    spark.read
         .option("header", "true")
         .option("sep", "\t")
         .csv(
             f"abfss://raw@{storage_account}.dfs.core.windows.net/Population/population_by_age_tsv"
         )
)


# COMMAND ----------

df_raw_population = (
    df_raw_population
        .withColumn(
            "age_group",
            regexp_replace(
                split(col("indic_de,geo\\time"), ",")[0],
                "PC_", ""
            )
        )
        .withColumn(
            "country_code",
            split(col("indic_de,geo\\time"), ",")[1]
        )
        .select(
            col("country_code"),
            col("age_group"),
            col("2019 ").alias("percentage_2019")
        )
)


# COMMAND ----------

df_raw_population = df_raw_population.withColumn(
    "percentage_2019",
    expr("try_cast(regexp_replace(percentage_2019, '[^0-9.]', '') as decimal(10,2))")
)


# COMMAND ----------

df_raw_population = df_raw_population.filter(length(col("country_code")) == 2)


# COMMAND ----------

df_population_pivot = (
    df_raw_population
        .groupBy("country_code")
        .pivot("age_group")
        .sum("percentage_2019")
        .orderBy("country_code")
)


# COMMAND ----------

df_dim_country = (
    spark.read
         .option("header", "true")
         .csv(
             f"abfss://transformation@{storage_account}.dfs.core.windows.net/lookup_files/country_lookup.csv"
         )
)


# COMMAND ----------

df_processed_population = (
    df_population_pivot.alias("p")
        .join(
            df_dim_country.alias("c"),
            col("p.country_code") == col("c.country_code_2_digit"),
            "inner"
        )
        .select(
            col("c.country").alias("country"),
            col("c.country_code_2_digit"),
            col("c.country_code_3_digit"),
            col("c.population"),
            col("p.Y0_14").alias("age_group_0_14"),
            col("p.Y15_24").alias("age_group_15_24"),
            col("p.Y25_49").alias("age_group_25_49"),
            col("p.Y50_64").alias("age_group_50_64"),
            col("p.Y65_79").alias("age_group_65_79"),
            col("p.Y80_MAX").alias("age_group_80_max")
        )
        .orderBy("country")
)

display(df_processed_population)


# COMMAND ----------

df_processed_population.write \
    .option("header", "true") \
    .mode("overwrite") \
    .csv(
        f"abfss://processed@{storage_account}.dfs.core.windows.net/clean/population/"
    )

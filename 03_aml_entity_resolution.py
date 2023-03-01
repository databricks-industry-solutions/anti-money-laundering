# Databricks notebook source
# MAGIC %md 
# MAGIC You may find this series of notebooks at https://github.com/databricks-industry-solutions/anti-money-laundering. For more information about this solution accelerator, visit https://www.databricks.com/blog/2021/07/16/aml-solutions-at-scale-using-databricks-lakehouse-platform.html.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Data Deduplication
# MAGIC 
# MAGIC The last category of problems in AML that we focus on is entity resolution. Many open-source libraries tackle this problem, so for some basic entity fuzzy matching, we chose to highlight a library that achieves the linkage at scale. 

# COMMAND ----------

# MAGIC %md
# MAGIC Developed by the ministry of Justice, [Splink](https://github.com/moj-analytical-services/splink) is an entity linkage framework that offers configurations to specify matching columns as well as blocking rules at enterprise scale. By combining field attributes (such as organization + mailing address), we could detect similarities and de-duplicate matching records. Splink works by assigning a match probability which can be used to identify transactions in which entity attributes are highly similar, indicating a potential alert in reported address, entity name, or transaction amounts. Given the fact that entity resolution can be highly manual for matching account information, having open-source libraries which automated this task and which save this information in Delta Lake can make investigators much more productive for case resolution. While there are several options available for entity matching like the one using Locality-Sensitive Hashing (LSH) as mentioned in this blog [post](https://databricks.com/blog/2021/05/24/machine-learning-based-item-matching-for-retailers-and-brands.html), we recommend our readers to identify the right algorithm for the job.

# COMMAND ----------

# MAGIC %run ./config/aml_config

# COMMAND ----------

raw_records = spark.read.table(config['db_dedupe'])
display(raw_records)

# COMMAND ----------

# MAGIC %md
# MAGIC As reported above, we can easily eye ball some similarities between "The Bank of New York Mellon Corp." and "BNY Mellon". Obvious for a human eye, this becomes a real challenge at scale when doing so programmatically. Splink works by assigning a match probability which can be used to identify transactions in which entity attributes are highly similar, indicating a potential alert in reported address, entity name, or transaction amounts. To reduce the required number of pairwise comparisons among descriptions, methods for entity resolution typically perform a preprocessing step, called blocking, which places similar entity descriptions into blocks and executes comparisons only between descriptions within the same block.

# COMMAND ----------

settings = {
    "link_type": "dedupe_only",
    "blocking_rules": [
        "l.amount = r.amount",
    ],
    "comparison_columns": [
        {
            "col_name": "org_name",
            "term_frequency_adjustments": True},
        {
            "col_name": "address",
            "term_frequency_adjustments": True
        },
        {
            "col_name": "country"
        },
              {
            "col_name": "amount"
        }
    ]
}

from splink import Splink
linker = Splink(settings, raw_records, spark)
raw_records_entities = linker.get_scored_comparisons()
display(raw_records_entities.take(1))

# COMMAND ----------

# MAGIC %md 
# MAGIC As reported above, we quickly found some inconsistencies for NY Mellon address, with "Canada Square, Canary Wharf, London, United Kingdom" similar to "Canada Square, Canary Wharf, London, UK". We can store our de-duplicated records back to a delta table that can be used for AML investigation.

# COMMAND ----------

raw_records_entities.write.mode("overwrite").format("delta").saveAsTable(config['db_dedupe_records'])

# COMMAND ----------

model = linker.model
model.probability_distribution_chart()
model.bayes_factor_chart()
model.all_charts_write_html_file(f"{temp_directory}/splink_charts.html", overwrite=True)

# COMMAND ----------

# MAGIC %md Let's dig deeper into some of statistics and plots offered by the Splink library. The following commands offer insights into how the match probability was calculated. Splink primarily uses a Expectation Maximization framework to maximize a likelihood function to generate match probabilities on record pairs as shown in the below graphs. Expectation Maximization is an iterative algorithm, so we can also see matches and non matches for different iterations as well. 

# COMMAND ----------

html_file_content = open(f"{temp_directory}/splink_charts.html", 'r').read()
displayHTML(html_file_content)

# COMMAND ----------

# MAGIC %md
# MAGIC As we continue our exploratory data analysis we can also look at individual matches and respective explanations. In the below example we are matching on Org name and Address and the respective match probabilities are shown for these levels. Another good example would be to show matches on a person's name, address and deposit amounts to detect high volume of suspicious transactions.

# COMMAND ----------

from splink.intuition import intuition_report
row_dict = raw_records_entities.toPandas().sample(1).to_dict(orient="records")[0]
print(intuition_report(row_dict, model))

# COMMAND ----------

# MAGIC %md
# MAGIC Finally, we can enrich our original dataset with a unique identifier that can be used to de-duplicate matching records. Once again, we do not wish to modify underlying data with AI, but rather provide our AML investigation team all the necessary context / utilities for them to take the necessary measures. 

# COMMAND ----------

df2 = spark.table(config['db_transactions'])
df2 = df2.withColumnRenamed("txn_id", "unique_id")
display(df2)

# COMMAND ----------

# MAGIC %md
# MAGIC Similar to our de-duplication strategy at an entity level, we can deduplicate multiple fields such as originator and recipient mailing addresses.

# COMMAND ----------

settings = {
    "link_type": "dedupe_only",
    "blocking_rules": [
        "l.txn_amount = r.txn_amount",
    ],
    "comparison_columns": [
        
        {
            "col_name": "rptd_originator_address",
        }, 
        { 
            "col_name": "rptd_originator_name",
        }
    ]
}

from splink import Splink
linker = Splink(settings, df2, spark)
df2_e = linker.get_scored_comparisons()

# COMMAND ----------

from pyspark.sql.functions import * 
display(df2_e.filter( (col("rptd_originator_address_l") != '')).filter((col("rptd_originator_address_r") != '')))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Closing thoughts
# MAGIC In this series of notebooks, we briefly touched on different technical concepts as they pertain to AML investigations. From network analysis, computer vision and entity resolution, we demonstrated the need for AI to complement decisioning and reduce investigation time. Although we kept the investigation aspect light, we covered why the Lakehouse architecture is the most scalable and versatile platform to enable analysts in their AML analytics. All of these capabilities will allow your organization to reduce TCO compared to proprietary AML solutions. Organizations can start embedding advanced analytics in their day to day process by bringing additional context to their investigations team rather than trying to automate what is often regarded as the most regulated activity. AI as **augmented intelligence**, we can easily package all necessary AI driven insights through the form of simple dashboarding capabilities for analysts to act upon.
# MAGIC 
# MAGIC <img src=https://brysmiwasb.blob.core.windows.net/demos/aml/aml_dashboard.png width=800>

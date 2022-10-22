# Databricks notebook source
# MAGIC %md 
# MAGIC You may find this series of notebooks at https://github.com/databricks-industry-solutions/anti-money-laundering. For more information about this solution accelerator, visit https://www.databricks.com/blog/2021/07/16/aml-solutions-at-scale-using-databricks-lakehouse-platform.html.

# COMMAND ----------

# MAGIC %md
# MAGIC <img src=https://d1r5llqwmkrl74.cloudfront.net/notebooks/fs-lakehouse-logo.png width="600px">
# MAGIC 
# MAGIC [![DBU](https://img.shields.io/badge/DBU-L-yellow)]()
# MAGIC [![COMPLEXITY](https://img.shields.io/badge/COMPLEXITY-201-yellow)]()
# MAGIC 
# MAGIC *Anti-Money Laundering (AML) compliance has been undoubtedly one of the top regulatory agenda items in the United States and across the globe to provide oversight of financial institutions. Given the shift to digital banking, Financial Institutions process billions of transactions every day and the scope for money laundering grows every day even with stricter payment monitoring and robust Know Your Customer (KYC) solutions. In this solution, we would like to share our experiences working with our customers on how FSI can build an Enterprise-scale AML solution on a Lakehouse platform that not only provides strong oversight but also provides innovative solutions to scale and adapt to the reality of modern ways of online money laundering threats. Through the concept of graph analytics, natural language processing (NLP) as well as computer vision, we will be uncovering multiple aspects of AML prevention in a world of Data and AI.*
# MAGIC 
# MAGIC ---
# MAGIC <anindita.mahapatra@databricks.com>, <ricardo.portilla@databricks.com>, <sri.ghattamaneni@databricks.com>

# COMMAND ----------

# MAGIC %md
# MAGIC <img src='https://databricks.com/wp-content/uploads/2021/07/aml-blog-img-1-a.png' width=800>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC &copy; 2021 Databricks, Inc. All rights reserved. The source in this notebook is provided subject to the Databricks License [https://databricks.com/db-license-source].  All included or referenced third party libraries are subject to the licenses set forth below.
# MAGIC 
# MAGIC | library                                | description             | license    | source                                              |
# MAGIC |----------------------------------------|-------------------------|------------|-----------------------------------------------------|
# MAGIC | graphframes:graphframes                | Graph library           | Apache2    | https://github.com/graphframes/graphframes          |
# MAGIC | torch                                  | Pytorch library         | BSD        | https://pytorch.org/                                |
# MAGIC | Pillow                                 | Image processing        | HPND       | https://python-pillow.org/                          |
# MAGIC | Splink                                 | Entity linkage          | MIT        | https://github.com/moj-analytical-services/splink   |

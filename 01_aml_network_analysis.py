# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Graph patterns 
# MAGIC 
# MAGIC One of the main data sources which AML analysts will use as part of a case is transaction data. Even though this data is tabular and easily accessible with SQL, it becomes cumbersome to track chains of transactions that are 3 or more layers deep with SQL queries. For this reason, it is important to have a flexible suite of languages and APIs to express simple concepts such as a connected network of suspicious individuals transacting illegally together. Luckily, this is simple to accomplish using [GraphFrames](https://graphframes.github.io/graphframes/docs/_site/index.html), a graph API pre-installed in the Databricks Runtime for Machine Learning. 
# MAGIC 
# MAGIC We are going to utilize a dataset consisting of transactions as well as entities derived from transactions to detect the presence of these patterns with Spark, GraphFrames, and Delta Lake. The persisted patterns are saved in Delta Lake so that Databricks SQL can be applied on the gold-level aggregated versions of these findings. The core value that the patterns lend is that analysts can consolidate investigations of connected individuals. In each of the scenarios, we will be using the connectivity of individuals using graph analytics - connecting identities in this simple manner means cases can be consolidated to reduce duplication of work and decrease time to resolve cases.

# COMMAND ----------

# MAGIC %run ./config/aml_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Entity Resolution/Synthetic Identity
# MAGIC 
# MAGIC The existence of synthetic identities can be a cause for alarm. Using graph analysis, all of the entities from our transactions can be analyzed in bulk to detect a risk level. Based on how many connections (i.e. common attributes) exist between entities, we can assign a lower or higher score and create an alert based on high-scoring groups. Below is a basic representation of this idea
# MAGIC 
# MAGIC <img src="https://databricks.com/wp-content/uploads/2021/07/AML-on-Lakehouse-Platform-blog-img-2.jpg" width=550/>
# MAGIC 
# MAGIC In our analysis, this is done in 3 phases: 
# MAGIC 
# MAGIC a) Based on the transaction data, extract the entities 
# MAGIC <br>
# MAGIC b) Create links between entities based on address, phone number, email 
# MAGIC <br>
# MAGIC c) Use GraphFrames connected components to determine whether multiple entities (identified by an ID and other attributes above) are connected via one or more links. 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Using SQL 
# MAGIC Before jumping straight into graph theory, we first want to get a glimpse at our synthetic data set using standard SQL. By joining our dataset by itself, we can easily extract entities sharing a same attribute such as email address. Although feasible for a 1st or 2nd degree connection, deeper insights can only be gained with more advanced network techniques as described later.

# COMMAND ----------

display(spark.read.table(config['db_transactions']))

# COMMAND ----------

display(
  sql("""
  select * 
  from {0}
  where email_addr in 
  (
    select A.email_addr 
    from 
      (
        select email_addr, count(*) as cnt 
        from {0}
        group by email_addr
      ) A
    where A.cnt > 1
  )
  order by email_addr
  """.format(config['db_entities']))
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Using GraphFrames
# MAGIC As we want to explore deeper relationships, our SQL statement will exponentially grow in size and complexity, requiring a graph library such as Graphframes. GraphFrames is a package for Apache Spark which provides DataFrame-based Graphs. It provides high-level APIs in Scala, Java, and Python. It aims to provide both the functionality of GraphX and extended functionality taking advantage of Spark DataFrames. This extended functionality includes motif finding, DataFrame-based serialization, and highly expressive graph queries.

# COMMAND ----------

from graphframes import *

# COMMAND ----------

# MAGIC %md
# MAGIC We build our graph structure where nodes will capture distinct transaction attributes (email/phone/address) and edges the relationships between those attributes. A transaction involving customer A and email address E would connect "node" A with "node" E. Our graph becomes undirected / unweighted network.

# COMMAND ----------

identity_edges = sql('''
select entity_id as src, address as dst from {0} where address is not null
union
select entity_id as src, email_addr as dst from {0} where email_addr is not null
union
select entity_id as src, phone_number as dst from {0} where phone_number is not null
'''.format(config['db_entities']))

identity_nodes = sql('''
select distinct(entity_id) as id, 'Person' as type from {0}
union 
select distinct(address) as id, 'Address' as type from {0}
union 
select distinct(email_addr) as id, 'Email' as type from {0}
union 
select distinct(phone_number) as id, 'Phone' as type from {0}
'''.format(config['db_entities']))

aml_identity_g = GraphFrame(identity_nodes, identity_edges)

# COMMAND ----------

# MAGIC %md
# MAGIC Graph built-in models such as a [connected components](https://graphframes.github.io/graphframes/docs/_site/user-guide.html#connected-components) drastically simplifies our overall investigation. Instead of recursively joining our dataset for connected entities, this simple API call will return groups of nodes having at least one entity in common. 

# COMMAND ----------

import uuid
sc.setCheckpointDir('{}/chk_{}'.format(temp_directory, uuid.uuid4().hex))
result = aml_identity_g.connectedComponents()
result.select("id", "component", 'type').createOrReplaceTempView("components")

# COMMAND ----------

# MAGIC %md
# MAGIC As we gain deeper insights of our graph structure, the results can be further analyzed through simple SQL. Used as a silver layer, this data asset can be used to find synthetic IDs at minimal cost.

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view ptntl_synthetic_ids
# MAGIC as
# MAGIC with dupes as
# MAGIC (
# MAGIC   select 
# MAGIC     component, 
# MAGIC     count(case when type = 'Person' then 1 end) person_ct 
# MAGIC   from components
# MAGIC   group by component
# MAGIC   having person_ct > 1
# MAGIC )
# MAGIC select * from components
# MAGIC where component in (select component from dupes);

# COMMAND ----------

# MAGIC %md
# MAGIC It would appear that 766 records in our set would be having at least one entity in common to a different account.

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ptntl_synthetic_ids

# COMMAND ----------

from pyspark.sql.functions import *

suspicious_component_id = (
  spark
    .sql("select id, component, type from ptntl_synthetic_ids")
    .filter(col('type') == 'Person')
    .drop('type')
)

ids = suspicious_component_id.join(spark.table("ptntl_synthetic_ids"), ['component', 'id'])
ids.createOrReplaceTempView("sus_ids")

# COMMAND ----------

# MAGIC %md
# MAGIC We can easily carry out our investigation by revealing those shared attributes

# COMMAND ----------

entity_synth_scores = sql("""
select
  component,
  min(min_id) first_entity_id, 
  max(max_id) last_entity_id,
  sum(
    case
      when dupe_ct = 1 then 1 else 0
    end
  ) entity_synth_score
from
  (
    select
      component,
      type,
      min(id) min_id,
      max(id) max_id,
      count(1) dupe_ct
    from
      ptntl_synthetic_ids
    group by
      component,
      type
  ) foo
group by component
""")

# COMMAND ----------

entity_synth_scores.write.format("delta").mode('overwrite').saveAsTable(config['db_synth_scores'])

# COMMAND ----------

# MAGIC %md
# MAGIC Based on the results of this query, we would expect a cohort consisting of only 1 matching attribute (such as address) isn’t too much cause for concern. However, the more attributes that match, we should expect to alert this scenario. As shown below, we can flag cases where all 3 attributes match, allowing SQL analysts to get results from graph analytics run on all entities daily.

# COMMAND ----------

display(spark.read.table(config['db_synth_scores']).orderBy(desc('entity_synth_score')))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Structuring/Smurfing
# MAGIC 
# MAGIC Another common pattern seen often is one called structuring in which multiple entities send collude and send smaller ‘under the radar’ payments to a set of banks, which subsequently route larger aggregate amounts to a final institution on the far right. In this scenario, all parties have stayed underneath the $10,000 amount which would typically flag authorities. Not only is this easily accomplished with graph analytics, but the motif finding technique used can be automated to extend to other permutations of networks to find other alerts in the same way. We represent this technique through the form of a simple graph below
# MAGIC 
# MAGIC <img src="https://databricks.com/wp-content/uploads/2021/07/AML-on-Lakehouse-Platform-blog-img-4.jpg" width="800"/>

# COMMAND ----------

# MAGIC %md
# MAGIC As previously introduced, we can easily build a network structure that aims at finding such a pattern.

# COMMAND ----------

entity_edges = spark.sql(
"""
select 
  originator_id as src, 
  beneficiary_id as dst, 
  txn_amount, txn_id as id 
from {0}
""".format(config['db_transactions'])
)

entity_nodes = spark.sql(
"""
select 
  distinct(A.id), 
  'entity' as type 
from
  (
    select 
      distinct(originator_id) as id 
    from {0}
    union 
    select 
      distinct(beneficiary_id) as id 
    from {0}
  ) A
""".format(config['db_transactions'])
)

aml_entity_g = GraphFrame(entity_nodes, entity_edges)
entity_edges.createOrReplaceTempView("entity_edges")
entity_nodes.createOrReplaceTempView("entity_nodes")

# COMMAND ----------

# MAGIC %md
# MAGIC Let’s write the basic motif-finding code to detect the scenario above. 

# COMMAND ----------

motif = "(a)-[e1]->(b); (b)-[e2]->(c); (c)-[e3]->(d); (e)-[e4]->(f); (f)-[e5]->(c); (c)-[e6]->(g)"
struct_scn_1 = aml_entity_g.find(motif)

joined_graphs = (
  struct_scn_1.alias("a")
  .join(struct_scn_1.alias("b"), col("a.g.id") == col("b.g.id"))
  .filter(col("a.e6.txn_amount") + col("b.e6.txn_amount") > 10000)
)

joined_graphs.selectExpr("a.*").write.option("overwriteSchema", "true").mode('overwrite').saveAsTable(config['db_structuring'])

# COMMAND ----------

# MAGIC %md
# MAGIC As parsed out from the motif patterns, we see the exact scenario above detected below when we join our graph metadata back to structured datasets.

# COMMAND ----------

display(
  sql(
    """
    select distinct b.entity_id top_entity_id, b.name first_entity, c.name second_entity, d.name third_entity, e.name fourth_entity
    from {0} a 
    join {1} b 
    on a.a.id = b.entity_id
    join {1} c 
    on a.b.id = c.entity_id
    join {1} d 
    on a.c.id = d.entity_id
    join {1} e
    on a.d.id = e.entity_id
    where e.entity_type = 'Company'
    order by (cast(top_entity_id as integer))
    """.format(config['db_structuring'], config['db_entities'])
  )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Round-tripping
# MAGIC 
# MAGIC There can be several variations of this pattern of money flow, but the basic premise is that the source and the destination are the same. Like the previous ‘structuring’ scenario, a simple motif search can help expose such patterns.
# MAGIC 
# MAGIC <img src="https://brysmiwasb.blob.core.windows.net/demos/aml/RoudTrip.png" width="650"/>

# COMMAND ----------

motif = "(a)-[e1]->(b); (b)-[e2]->(c); (c)-[e3]->(d); (d)-[e4]->(a)"
round_trip = aml_entity_g.find(motif)
round_trip.write.mode('overwrite').saveAsTable(config['db_roundtrips'])

# COMMAND ----------

# MAGIC %md
# MAGIC Once again, addressing this problem as a graph helps us record all parties involved in a roundtrip AML pattern together with the aggregated amount. 

# COMMAND ----------

display(
  sql(
    """
    select
      b.name original_entity,
      c.name intermediate_entity_1,
      d.name intermediate_entity_2,
      e.name final_entity,
      a.e1.txn_amount + a.e2.txn_amount + a.e3.txn_amount + a.e4.txn_amount agg_txn_amount
    from
      {0} a
      join {1} b on a.a.id = b.entity_id
      join {1} c on a.b.id = c.entity_id
      join {1} d on a.c.id = d.entity_id
      join {1} e on a.d.id = e.entity_id
    """.format(config['db_roundtrips'], config['db_entities'])
  )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Risk score Propagation
# MAGIC 
# MAGIC The 4th pattern we want to cover here is the perfect definition as to why this problem cannot be addressed through a simple SQL statement. Identified high risk entities (such as poltically exposed person) will have an influence (a network effect) on their circle. The risk score of all the entities that they interact with has to be adjusted to reflect the zone of influence. Using an iterative approach, we can follow the flow of transactions to any given depth and adjust the risk scores of others affected in the network. Luckily, [Pregel API](https://spark.apache.org/docs/latest/graphx-programming-guide.html#pregel-api) was built for that exact purpose. 
# MAGIC 
# MAGIC <img src="https://brysmiwasb.blob.core.windows.net/demos/aml/pregel.png" width="900"/>

# COMMAND ----------

entity_edges = spark.sql("""
select 
  originator_id as src, 
  beneficiary_id as dst, 
  txn_amount, 
  txn_id as id 
from {}
""".format(config['db_transactions']))

entity_nodes = spark.sql("""
select 
  distinct(A.id), risk 
from
  (
    select 
      distinct(entity_id) as id, 
      risk_score risk 
    from {}
  ) A
""".format(config['db_entities']))

entity_edges.createOrReplaceTempView("entity_edges")
entity_nodes.createOrReplaceTempView("entity_nodes")
aml_entity_g = GraphFrame(entity_nodes, entity_edges)

# COMMAND ----------

# MAGIC %md
# MAGIC Pregel operates against a set of functions and messages. Each node can propagate an information to their neighbours. Each neighbour can update its state and propagate a message downstream until no further messages can be sent or max iterations is reached. In the example below, we want to focus our analysis on maximum 3 layers depth, aggregating our risk score iteratively.

# COMMAND ----------

from graphframes import GraphFrame
from pyspark.sql.functions import coalesce, col, lit, sum, when, greatest
from graphframes.lib import Pregel

ranks = aml_entity_g.pregel \
     .setMaxIter(3) \
     .withVertexColumn("risk_score", col("risk"), coalesce(Pregel.msg()+ col("risk"), col("risk_score"))) \
     .sendMsgToDst(Pregel.src("risk_score")/2 )  \
     .aggMsgs(sum(Pregel.msg())) \
     .run()

ranks.write.mode('overwrite').saveAsTable(config['db_risk_propagation'])

# COMMAND ----------

display(
  sql(
    """
    select
      a.id,
      a.risk_score,
      a.risk original_risk_score,
      b.name
    from
      {0} a
      join {1} b on a.id = b.entity_id
    where
      id >= 10000001
    """.format(config['db_risk_propagation'], config['db_entities'])
  )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Closing thoughts
# MAGIC In this notebook, we gently introduced the concept of network analysis to gain further insights around AML activities. We demonstrated the need to acquire more context around transactions patterns rather than investigating individual transactions in isolation. Although we demonstrated the usefulness of graph theory, we decided to leave the investigation aspect to standard SQL processes in order to democratize the use of network analysis to investigation team with greater domain expertise and oftentimes less SW engineering experience. By building these data assets as new transactions are discovered, engineers and scientists can build simple dashboards for analysts to act upon.

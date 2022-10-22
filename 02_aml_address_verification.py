# Databricks notebook source
# MAGIC %md 
# MAGIC You may find this series of notebooks at https://github.com/databricks-industry-solutions/anti-money-laundering. For more information about this solution accelerator, visit https://www.databricks.com/blog/2021/07/16/aml-solutions-at-scale-using-databricks-lakehouse-platform.html.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Address Validation
# MAGIC A pattern we want to briefly touch upon is address matching of text to actual streetview images. Oftentimes, there is a need to validate the addresses which have been linked to entities on file. However, this can be a tedious, time-consuming, and manual process to both obtain a visualization of the address, clean, and validate. Luckily the Lakehouse architecture allows us to automate all of this using Python, machine learning runtimes with PyTorch, and pre-trained open-source models. Below is an example of a valid address to the human eye. 

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://brysmiwasb.blob.core.windows.net/demos/aml/aml_image_matching.png" width=600>

# COMMAND ----------

# MAGIC %run ./config/aml_config

# COMMAND ----------

from pyspark.sql.functions import * 

addresses = (
  spark
    .table(config['db_entities'])
    .filter(col("address").isNotNull())
    .withColumn("address", translate(translate(col("address"), ',', ''), ' ', '+'))
    .select('address')
    .toPandas()
)

addresses.head(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Using google maps API
# MAGIC Using street maps API, analysts can quickly access property pictures for investigation purpose. Although there are limitations in term of request rate (see Google maps API T&Cs), having access to some property data would be of great value for AML investigation. 
# MAGIC In order to safely pass credentials to notebook, we make use of the [secrets API](https://docs.databricks.com/security/secrets/index.html)

# COMMAND ----------

goog_api_key = dbutils.secrets.get(scope="solution-accelerator-cicd", key="google-api")

# COMMAND ----------

import json
import time
import requests
import urllib.error
import urllib.parse
import urllib.request

for index, row in addresses[0:100].iterrows():
    url = f"https://maps.googleapis.com/maps/api/streetview?parameters&size=640x640&fov=50&location={row['address']}&key={goog_api_key}"
    req = requests.get(url)
    with open(f'{temp_directory}/img_{index}.jpg', 'wb') as file:
       file.write(req.content)
    req.close()                                                      

# COMMAND ----------

# MAGIC %md
# MAGIC Using matplotlib, we can inspect a given address through the comfort of a Databricks notebook. It would appear that this specific address does not refer to any building / property. This additional context may have to be reported as part of an ongoing suspicious activity report (SAR).

# COMMAND ----------

import matplotlib.pyplot as plt
import matplotlib.image as mpimg
plt.figure(figsize=(10, 10))
img=mpimg.imread(f'{temp_directory}/img_0.jpg')
imgplot = plt.imshow(img)
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Augmented intelligence
# MAGIC In addition to the complexity of investigating some random pictures, the sheer amount of data analyst would have to manually inspect often hinders an efficient investigation process. Could we programmatically access pictures from google map and train a model to detect property / building automatically? Whilst AI is often seen as a automated black box decision engine, we consider AI in the context of AML as an augmented intelligence process that provides analysts with all necessary context to conduct AML investigation faster and more effectively.

# COMMAND ----------

# MAGIC %md
# MAGIC Instead of training our own computer vision model that will be expensive in term of infrastructure, skillset and manual efforts labeling data, we demonstrate how analysts can use a [pretrained](http://pytorch.org/docs/master/torchvision/models.html) model in PyTorch to make predictions. The model we'll use is a [VGG16](https://pytorch.org/hub/pytorch_vision_vgg/) convolutional network, trained on [ImageNet](http://www.image-net.org/) dataset.

# COMMAND ----------

import io
import numpy as np 

from PIL import Image
import requests
from matplotlib import cm

from torch.autograd import Variable
import torchvision.models as models
import torchvision.transforms as transforms

# Class labels used when training VGG as json, courtesy of the 'Example code' link above.
LABELS_URL = 'https://raw.githubusercontent.com/raghakot/keras-vis/master/resources/imagenet_class_index.json'
response = requests.get(LABELS_URL)  # Make an HTTP GET request and store the response.
labels = {int(key): value for key, value in response.json().items()}
img_and_labels = {}

for i in range(100):
  
  img=mpimg.imread(f'{temp_directory}/img_' + str(i) + '.jpg')
  img = Image.fromarray(img)
  # We can do all this preprocessing using a transform pipeline.
  min_img_size = 224  # The min size, as noted in the PyTorch pretrained models doc, is 224 px.
  transform_pipeline = transforms.Compose([transforms.Resize(min_img_size),
                                         transforms.ToTensor(),
                                         transforms.Normalize(mean=[0.485, 0.456, 0.406],
                                                              std=[0.229, 0.224, 0.225])])
  img = transform_pipeline(img)

  # PyTorch pretrained models expect the Tensor dims to be (num input imgs, num color channels, height, width).
  # Currently however, we have (num color channels, height, width); let's fix this by inserting a new axis.
  img = img.unsqueeze(0)  # Insert the new axis at index 0 i.e. in front of the other axes/dims. 

  # Now that we have preprocessed our img, we need to convert it into a 
  # Variable; PyTorch models expect inputs to be Variables. A PyTorch Variable is a  
  # wrapper around a PyTorch Tensor.
  img = Variable(img)

  # Now let's load our model and get a prediction!
  vgg = models.vgg16(pretrained=True)  # This may take a few minutes.
  prediction = vgg(img)  # Returns a Tensor of shape (batch, num class labels)
  prediction = prediction.data.numpy().argmax()  # Our prediction will be the index of the class label with the largest value.
  img_and_labels[i] = labels[prediction]

# COMMAND ----------

# MAGIC %md
# MAGIC As reported below, a pre-trained model already offers lots of information out of the box that could be really useful to understand how legit a given address is.

# COMMAND ----------

img_and_labels

# COMMAND ----------

# MAGIC %md
# MAGIC The power of Delta Lake allows us to store a reference to our unstructured data along with a label for simple querying in our classification breakdown below.

# COMMAND ----------

import pandas as pd 
pdf = pd.DataFrame.from_dict(img_and_labels, orient='index', columns=['image_number', 'label'])
spark.createDataFrame(pdf).filter(col("label") != 'envelope').write.mode('overwrite').saveAsTable(config['db_streetview'])

# COMMAND ----------

display(sql("select label, count(1) from {} group by label".format(config['db_streetview'])))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Closing thoughts
# MAGIC Although image classification is oftentimes a data science / data engineering exercise, storing results back to a delta table would allow AML analysts investigate those cases further through simple dashboarding or SQL capability. The power of Delta Lake allows us to store a reference to our unstructured data along with a label for simple querying in our classification breakdown below. The ability to query unstructured data with Delta Lake is an enormous time-saver for analysts and speeds up the validation process down to minutes instead of days or weeks.

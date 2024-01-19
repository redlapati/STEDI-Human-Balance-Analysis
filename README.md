# STEDI-Human-Balance-Analysis
## Overview

STEDI Step Trainer is a motion sensor device that records the distance of the object detected and its mobile app uses mobile phone accelerometer to detect motion in the X, Y, and Z directions. Several customers have purchased the device, installed mobile app, and begun using them together to test their balance. 

STEDI team wants to use the motion sensor data to train a machine learning model to detect steps accurately in real-time. *Privacy will be a primary consideration in deciding what data can be used*. Some of the early adopters have agreed to share their data for research purposes. Only these customers' step trainer and accelerometer data should be used in the training data for the machine learning model.

## Project Details

**STEDI Step Trainer** is a device that:
* trains the user to do a STEDI balance exercise;
* and has sensors on the device that collect data to train a machine-learning algorithm to detect steps;
* has a companion mobile app that collects customer data and interacts with the device sensors.

As a data engineer, I need to extract the data produced by the **STEDI Step Trainer** sensors and the mobile accelerometer app, and curate them into a lake house solution on AWS so that Data Scientists can train machine learning model. 

AWS Glue Catelog tables are created for the source data and AWS Glue jobs are created to read data from different sources, categorize the data, and curate it to be used for machine learning.

## Project Data

**Customer** -  data fulfillment and the STEDI website that contains following fields in JSON format
* customername
* email
* phone
* birthday
* serialnumber
* registrationdate
* lastupdatedate
* sharewithresearchasofdate
* sharewithpublicasofdate
* sharewithfriendsasofdate

**Accelerometer** - data from the mobile app that contains  following fields in JSON format
* user
* timestamp
* x
* y
* z

**Step Trainer** - data from the motion sensor that contains  following fields in JSON format
* sensorreadingtime
* serialnumber
* distancefromobject

This data is stored in AWS S3 and AWS Glue table are created on top these datasets.

## Implementation

**Landing Zone** 

The raw data of customer, accelerometer and stedi trainer data sets is stored here

**Trusted Zone**

AWS Glue jobs transforms raw data in landing zone and stores transformed trused data here

`customer_landing_to_trusted.py` - python script that filters customer data that contains data related to the customers who agreed to share their data

`accelerometer_landing_to_trusted.py` - python script that gets accelerometer data only for trusted customers

`step_trainer_landing_to_trusted.py` - python script that gets step trainer data only for trusted customers who have accelerometer data

**Curated Zone** 

AWS Glue jobs transforms data in trusted zone and stores final curated data here

`customer_trusted_to_curated.py` - python script that filters customer data that contains data related to the customers who agreed to share their data and has accelerometer data

`machine_learning_curated.py` - python script to get both trusted step trainer data and trusted accelerometer data for the curated customers to be used for machine learning model



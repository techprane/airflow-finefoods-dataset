import os
import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from datetime import datetime, timedelta


def on_failure_callback(**context):
    print(f"Task {context['task_instance_key_str']} failed.")


def extract(**kwargs):
    # Read data from finefoods.json
    # with open('./finefoods.json', 'r') as file:
    data = [
        {
            "productId": "B0027AWD02",
            "userId": "A2TZ10PVE5KWX8",
            "profileName": "KM",
            "helpfulness": "0/0",
            "score": "5.0",
            "time": "2016-04-28T10:26:28Z",
            "summary": "Nice Taffy",
            "text": "Lorem Ipsum Great taffy at a great price. There was a wide assortment of yummy taffy. Delivery was very quick."
        },
        {
            "productId": "B005WSEFI8",
            "userId": "AJIX01E6FHOFX",
            "profileName": "llaand12",
            "helpfulness": "0/1",
            "score": "4.0",
            "time": "2011-12-22T16:37:30Z",
            "summary": "Yay Barley",
            "text": "Lorem Ipsum Great taffy at a great price. There was a wide assortment of yummy taffy. Delivery was very quick."
        },
        {"productId": "B005611CNM",
         "userId": "A2866H7PEU6OKO",
         "profileName": "Eugenia Loli",
         "helpfulness": "0/0",
         "score": "2.0",
         "time": "2013-03-24T18:26:28Z",
         "summary": "Yay Barley",
         "text": "Right now I'm mostly just sprouting this so my cats can eat the grass."
         },
        {"productId": "B002B8AX7Y",
         "userId": "A14E40Q45XZXEQ",
         "profileName": "Amy Greer",
         "helpfulness": "0/0",
         "score": "5.0",
         "time": "2017-08-28T10:26:28Z",
         "summary": "Healthy Dog Food",
         "text": "Lorem Ipsum Great taffy at a great price. There was a wide assortment of yummy taffy. Delivery was very quick."
         },
        {
            "productId": "B0057OOLAO",
            "userId": "A38459WNXH0NZX",
            "profileName": "Robert L. Young \"Good Cook\"",
            "helpfulness": "0/1",
            "score": "4.0",
            "time": "2014-12-20T21:45:45Z",
            "summary": "Healthy Dog Food",
            "text": "This is a very healthy dog food. Good for their digestion. Also good for small puppies. My dog eats her required amount at every feeding."
        }
    ]
    return data


def transform(**kwargs):
    ti = kwargs['ti']
    # Pull the output from the extract task
    data = ti.xcom_pull(task_ids='extract')
    # (Optional) Perform any in-Python transformation if needed.
    return data


def load(ti, **kwargs):
    try:
        hook = MongoHook(mongo_conn_id='mongoid')
        client = hook.get_conn()
        db = client.airflow_db
        # Use a staging collection to insert raw data first
        staging_collection = db.finefoods_staging
        finefoods_collection = db.finefoods_collection

        print(f"Connected to MongoDB - {client.server_info()}")

        # Retrieve data from XCom
        data = ti.xcom_pull(task_ids='transform')

        # Insert raw data into staging collection
        if isinstance(data, list):
            staging_collection.insert_many(data)
        else:
            staging_collection.insert_one(data)

        # Define an aggregation pipeline to transform data types
        pipeline = [
            {
                "$addFields": {
                    "score": {"$toDouble": "$score"},
                    "time": {"$toDate": "$time"}
                }

            }
        ]

        # Run the aggregation pipeline on the staging collection
        transformed_docs = list(staging_collection.aggregate(pipeline))
        print("Transformed Documents:", transformed_docs)

        # Insert the transformed documents into the final collection
        if transformed_docs:
            finefoods_collection.insert_many(transformed_docs)

        # Clean up: Drop the staging collection
        staging_collection.drop()

    except Exception as e:
        print(f"Error connecting to MongoDB -- {e}")


with DAG(
    dag_id="load_finefoods_data",
    schedule_interval=None,
    description="ETL pipeline for finefoods dataset",
    catchup=False,
    tags=["finefoods"],
    default_args={
        "owner": "Saint Vandora",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": on_failure_callback,
    },
    start_date=datetime(2022, 10, 28)
) as dag:

    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract,
        dag=dag
    )

    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform,
        dag=dag
    )

    load_task = PythonOperator(
        task_id='load',
        python_callable=load,
        dag=dag
    )

extract_task >> transform_task >> load_task

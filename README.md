# data-in-the-cloud
Github du cours Cloud computing de l'Executive Master Statistiques et Big Data

➡ [Link to the course slides](https://docs.google.com/presentation/d/1Jh7VVYhXqhnqWHTBiRq8HjPoHb2FSyqY__Hx1GRa5Og/edit?usp=sharing) ⬅

## Airflow Lab Session
The idea is to wrap an already made model in a `PythonOperator` that takes the downloaded data file as a parameter, and the model output location as a parameter.

**Instructions**

Create an empty DAG file that will do the following:

- Download the dataset from S3 to a known path: https://iasd-data-in-the-cloud.s3.eu-west-3.amazonaws.com/petrol_consumption.csv.
- Then, triggers a PythonOperator to create a pickle of a trained regression model using the Random Forest algorithm: [here is the code to adapt](https://github.com/faouzelfassi/data-in-the-cloud/blob/master/model.py). You should be able to pass the filepath to the dataset as an argument to the PythonOperator in addition to the model pickle desired location.
- Then, uploads the model pickle to S3 in a timestamped folder (folder named after the execution date of the pipeline).
- Eventually, deletes the dataset and the model pickle from local storage.


To do so, you'll need:
1. To modify the Astronomer image by adding `pandas` and `scikit-learn` in your `requirements.txt` [instructions here](https://www.astronomer.io/docs/cloud/stable/develop/customize-image/#add-python-and-os-level-packages).
1. [S3Hook](https://airflow.apache.org/docs/stable/_modules/airflow/hooks/S3_hook.html), to communicate with S3 (download, upload).
1. Add a connection in Airflow to be able to store things in my personal S3 bucket, you will set the SECRET_KEY and ACCESS_KEY (I'll give you by DM) in the Airflow web interface in the tab Admin > Connections, to give the permissions to Airflow for managing AWS services on your behalf.
1. [PythonOperator](https://airflow.apache.org/docs/stable/howto/operator/python.html), that will contain the model generator.
1. [Airflow Macros](https://airflow.apache.org/docs/stable/macros-ref.html#macros), handy for getting some variables around the execution of the DAG. Useful for outputing in a folder prefixed by a date representing the execution date of the pipeline run.
1. Using `astro dev start` you'll be able to run Airflow locally to test your pipeline before deploying in production, [for more info](https://www.astronomer.io/docs/cloud/stable/develop/cli-quickstart/#start-airflow-locally).

![Add the connection in Airflow UI](https://github.com/faouzelfassi/data-in-the-cloud/blob/master/doc/airflow_add_connection.png?raw=true)

Here is an example of macros in use, in this example we delete a dynamically created file (containing the execution date of the pipeline in its name) using a BashOperator. 

```python
from airflow.operators.bash_operator import BashOperator

OUTPUT_CSV_FILEPATH = '/PATH/TO/MY_FILE.csv'

dag = DAG(
    "my_dag",
    default_args=default_args,
    max_active_runs=1,
    concurrency=10,
    schedule_interval="0 12 * * *",
)

delete_csv = BashOperator(
    task_id="delete_csv",
    bash_command="rm {}".format(OUTPUT_CSV_FILEPATH.replace(".csv", "{{ ds }}.csv")),
    dag=dag,
)
```

##### Note: pickle
Saving a model to disk is as simple as:

```python
import pickle

# save the model to disk
filename = 'finalized_model.sav'
pickle.dump(model, open(filename, 'wb'))
```

Symmetrically, loading a model from disk:

```python
# load the model from disk
filename = 'finalized_model.sav'
loaded_model = pickle.load(open(filename, 'rb'))
```

# Airflow Test

This project is intended for testing Airflow and DAGs and to create a suitable environment to execute airflow.

## Instalation
(NOTE: I tried with Python 3.6. Python 3.7 instalation was giving me problems with the "async" variable problem. Starting with 3.7, async is reserved by Python so the code cannot name a variable "async")

It is recommended to insall everything under a virtualenv.

First install all the requirements with pip:

    pip install requirements.txt

Note from the web:

> GPL Dependency: One of the dependencies of Apache Airflow by default pulls in a GPL library (‘unidecode’). In case this is a concern you can force a non GPL library by issuing `export  SLUGIFY_USES_TEXT_UNIDECODE=yes`  and then proceed with the normal installation. Please note that this needs to be specified at every upgrade. Also note that if  unidecode  is already present on the system the dependency will still be used.

The default home for Airflow is ~/airflow, which you can change with

    export AIRFLOW_HOME=<your path here>
 
Inside this folder you will find the ***airflow.cfg*** in which you can set parameters like which DB to point, authentication, the log and DAG folder, the number of tasks for the 

After the requirements have been installed, you will need to init Airflow's DB, so type in terminal:

    airflow initdb

If this does not break, then you are ready to go. Init the server with:

    airflow webserver -p 8080

And then open a separate terminal and start the scheduler. No tasks will be executed if the scheduler is not started:

    airflow scheduler

After this step, you can launch the workflows through terminal or in the web page at http://localhost:8080 in the dashboard, enabling the task and running it.

More information in Airflow's web page: https://airflow.apache.org

## About the code

An important thing to notice is that passwords are stored under config/secret.py so you will have to create your own to be able to run the code.
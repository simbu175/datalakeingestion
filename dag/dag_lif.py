from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime
import csv


def create_dag(dct):
    args = {
        'owner': 'airflow',
        'start_date': datetime(2021, 1, 1),
        'retries': 0
    }
    schedule = '0 8 * * *'

    with DAG(dag_id=dct['dag_name'], default_args=args, schedule_interval=schedule, catchup=False) as dag:
        # sshHook = SSHHook(ssh_conn_id='con_ssh_databiapp')

        start = DummyOperator(task_id='start')

        dag_dependency = False
        if dct['dependent_dag']:
            # create a sensor task that will wait for dependent dag completion
            dag_dependency = True
            sensor = ExternalTaskSensor(task_id='sensor', external_dag_id=dct['dependent_dag'])

        if dag_dependency:
            initial_task = sensor
            start >> sensor
        else:
            initial_task = start

        # {'dag_name': 'dag_extract_fact', 'tasks': {'1': ['stg_interest_accrual_drafty', 'stg_interest_accrual_ls',
        # 'stg_payment_calendar_drafty', 'stg_payment_calendar_ls'], '2': ['stg_fact_loan_transaction_drafty',
        # 'stg_fact_loan_transaction_ls']}, 'script_name': 'extract.py', 'dependent_dag': 'None'}

        for task_order in sorted(dct['tasks'].keys()):
            # create the task for each of the list for a task_order in sorted tasks dictionary

            task_list = dct['tasks'][task_order]

            dag_task_list = []

            sshHook = SSHHook(ssh_conn_id='lif')
            for task in task_list:
                # sh_task = SSHOperator(task_id='sh_task', ssh_hook=sshHook, command=command)
                dag_task_list.append(SSHOperator(
                    task_id='task_' + task,
                    ssh_hook=sshHook,
                    command='docker exec LIF python /lif/' + dct['script_name'] + ' ' + task
                    ))

            end = DummyOperator(task_id='end_' + str(task_order))

            initial_task >> dag_task_list >> end
            initial_task = end

    return dag


with open('/opt/airflow/dags/Airflow_Config.csv') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    line_count = 0
    dag_dict = {}
    for row in csv_reader:
        # pdb.set_trace()
        if line_count == 0:
            print("This is header line")
        else:
            """ need to add new elements as we go through parsing the excel/csv file so need to 
            check whether that element is existing in the data or not """
            if row[0] not in dag_dict:
                dag_dict[row[0]] = {"dag_name": row[0],
                                    "tasks": {row[4]: [row[2], ]},
                                    "script_name": row[3],
                                    "dependent_dag": row[6]
                                    }

            else:
                if row[4] not in dag_dict[row[0]]['tasks']:
                    dag_dict[row[0]]['tasks'][row[4]] = [row[2], ]
                else:
                    dag_dict[row[0]]['tasks'][row[4]].append(row[2])

        line_count += 1

        for dag in dag_dict:
            globals()[dag] = create_dag(dag_dict[dag])

    print(line_count)
    print(dag_dict)

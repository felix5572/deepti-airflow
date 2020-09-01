import json, os
from datetime import timedelta
from datetime import datetime

from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

from PBS_operator import PBSOperator



with open('/home/fengbo/airflow/dags/v100.m.json') as f:
    mdata=json.load(f)

class VAR(object):
    dag_work_dir="{{ task_instance.xcom_pull(task_ids='all_start_check', key='dag_work_dir') }}"
    tar_temp="{{ task_instance.xcom_pull(task_ids='all_start_check', key='tar_temp') }}"
    tar_press="{{ task_instance.xcom_pull(task_ids='all_start_check', key='tar_press') }}"
    structure="{{ task_instance.xcom_pull(task_ids='all_start_check', key='structure') }}"

    @classmethod
    def all_start_check(cls, *args, **kwargs):
        print("kwargs['dag_run'] is ")
        print("kwargs['dag_run']")
        tar_temp=int(kwargs['dag_run'].conf['tar_temp'])
        tar_press=int(kwargs['dag_run'].conf['tar_press'])
        structure=str(kwargs['dag_run'].conf['structure'])
        work_base_dir=kwargs['dag_run'].conf['work_base_dir']
        dag_work_folder=str(tar_temp)+'K-'+str(tar_press)+'bar-'+str(structure)
        dag_work_dir=os.path.join(work_base_dir,dag_work_folder)
        assert os.path.isdir(work_base_dir) is True,  f'work_base_dir {work_base_dir} must exist '
        assert os.path.isdir(dag_work_dir)is False,  f'dag_work_folder dir {dag_work_dir} already exist'
    
        kwargs['ti'].xcom_push(key='dag_work_dir', value=dag_work_dir)
        kwargs['ti'].xcom_push(key='tar_temp', value=tar_temp)
        kwargs['ti'].xcom_push(key='tar_press', value=tar_press)
        kwargs['ti'].xcom_push(key='structure', value=structure)

default_args = {
    'owner': 'fengbo',
    'start_date': datetime(2020, 1, 1, 8, 00)
}

dag = DAG(
    dag_id='FreeEnergy',
    schedule_interval=None,
    dagrun_timeout=timedelta(hours=8),
    default_args=default_args
)

all_start_check = PythonOperator(
    task_id='all_start_check',
    python_callable=VAR.all_start_check,
    provide_context=True,
    dag=dag,
)

NPT_start_script_command=(f"source activate deepmd"
f" && mkdir {VAR.dag_work_dir}"
f" && cd {VAR.dag_work_dir}"
f" && mkdir NPT_sim "
f" && cd NPT_sim " 
f" && ln -s ../../bcc.lmp ./conf.lmp " 
f" && ln -s ../../graph.pb ./graph.pb "
f" && cp ../../npt.json ./in.json"
f" && sed -i s'|tar_temp|{VAR.tar_temp}|g' ./in.json"
f" && sed -i s'|tar_press|{VAR.tar_press}|g' ./in.json"
f" && python ~/deepti-1-yfb-Sn/equi.py gen in.json")

NPT_start_script = BashOperator(
    task_id='NPT_start_script',
    bash_command=NPT_start_script_command,
    dag=dag,
)

NPT_sim = PBSOperator(
    task_id='NPT_sim',
    mdata=mdata,
    work_dir=os.path.join(f"{VAR.dag_work_dir}", 'NPT_sim/', 'new_job/'),
    execution_timeout=timedelta(minutes=90),
    dag=dag
)

NPT_end_script_command=(f"source activate deepmd"
f" && cd {VAR.dag_work_dir}"
f" && cd NPT_sim"
f" && python ~/deepti-1-yfb-Sn/equi.py compute ./new_job > result ")
   
NPT_end_script = BashOperator(
    task_id='NPT_end_script', 
    bash_command=NPT_end_script_command,
    dag=dag
)


NVT_start_script_command=(f"source activate deepmd"
f" && cd {VAR.dag_work_dir}"
f" && mkdir NVT_sim "
f" && cd NVT_sim " 
f" && ln -s ../../graph.pb ./graph.pb "
f" && cp ../../nvt.json ./in.json"
f" && sed -i s'|tar_temp|{VAR.tar_temp}|g' ./in.json"
f" && sed -i s'|tar_press|{VAR.tar_press}|g' ./in.json"
f" && python ~/deepti-1-yfb-Sn/equi.py gen in.json -c ../NPT_sim/new_job")

NVT_start_script = BashOperator(
    task_id='NVT_start_script',
    bash_command=NVT_start_script_command,
    dag=dag,
)

NVT_sim = PBSOperator(
    task_id='NVT_sim',
    mdata=mdata,
    work_dir=os.path.join(f"{VAR.dag_work_dir}", 'NVT_sim/', 'new_job/'),
    execution_timeout=timedelta(minutes=60),
    dag=dag,
)

NVT_end_script_command=(f"source activate deepmd"
f" && cd {VAR.dag_work_dir} "
f" && cd NVT_sim "
f" && python ~/deepti-1-yfb-Sn/equi.py compute ./new_job > result ")

NVT_end_script = BashOperator(
    task_id='NVT_end_script', 
    bash_command=NVT_end_script_command,
    dag=dag
)

TI_start_scipt_command=(f"source activate deepmd"
f" && cd {VAR.dag_work_dir}"
f" && mkdir TI_sim "
f" && cd TI_sim " 
f" && ln -s ../../graph.pb ./graph.pb "
f" && ln -s ../NVT_sim/new_job/out.lmp ./conf.lmp"
f" && cp ../../ti.p.json ./in.json"
f" && sed -i s'|tar_temp|{VAR.tar_temp}|g' ./in.json"
f" && sed -i s'|tar_press|{VAR.tar_press}|g' ./in.json"
f" && python ~/deepti-1-yfb-Sn/ti.py gen in.json ")


TI_start_script = BashOperator(
    task_id='TI_start_script',
    bash_command=TI_start_scipt_command,
    dag=dag,
)


TI_sim = PBSOperator(
    task_id='TI_sim',
    mdata=mdata,
    work_dir=os.path.join(f"{VAR.dag_work_dir}",'TI_sim/', 'new_job/'),
    dagrun_timeout=timedelta(hours=6),
    is_array_job=True,
    dag=dag,
)

HTI_start_script_command=(f"source activate deepmd"
f" && cd {VAR.dag_work_dir}"
f" && mkdir HTI_sim "
f" && cd HTI_sim " 
f" && ln -s ../../graph.pb ./graph.pb "
f" && ln -s ../NVT_sim/new_job/out.lmp ./conf.lmp"
f" && cp ../../hti.json ./in.json"
f" && sed -i s'|tar_temp|{VAR.tar_temp}|g' ./in.json"
f" && sed -i s'|tar_press|{VAR.tar_press}|g' ./in.json"
f" && python ~/deepti-1-yfb-Sn/hti.py gen in.json -s two-step")

HTI_start_script = BashOperator(
    task_id='HTI_start_script',
    bash_command=HTI_start_script_command,
    dag=dag,
)


HTI_sim = PBSOperator(
    task_id='HTI_sim',
    mdata=mdata,
    work_dir=os.path.join(f"{VAR.dag_work_dir}", 'HTI_sim/', 'new_job/'),
    dagrun_timeout=timedelta(hours=6),
    is_array_job=True,
    dag=dag,
)

HTI_end_script_command=(f"source activate deepmd"
f" && cd {VAR.dag_work_dir}"
f" && cd HTI_sim "
f" && python ~/deepti-1-yfb-Sn/hti.py compute ./new_job > result ")

HTI_end_script = BashOperator(
    task_id='HTI_end_script',
    bash_command=HTI_end_script_command,
    dag=dag
)

all_end_script_command=(f"source activate deepmd"
f" && cd {VAR.dag_work_dir}"
f" && cd TI_sim"
f" && G=$(awk 'END{{print $1}}' ../HTI_sim/result)"
f" && G_err=$(awk 'END{{print $2}}' ../HTI_sim/result)"
f" && python ~/deepti-1-yfb-Sn/ti.py compute ./new_job -e $G -E $G_err -t {VAR.tar_temp} > result"
f" && cat result")

all_end_script=BashOperator(
    task_id='all_end_script',
    bash_command=all_end_script_command,
    dag=dag
)


all_start_check >> NPT_start_script >> NPT_sim >> NPT_end_script >> NVT_start_script >> NVT_sim >> NVT_end_script
NVT_end_script >> TI_start_script >> TI_sim
NVT_end_script >> HTI_start_script >> HTI_sim >> HTI_end_script
[TI_sim, HTI_end_script] >> all_end_script


if __name__ == "__main__":
    dag.cli()

import os, time, re
from subprocess import PIPE, STDOUT, Popen
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
from PBS_hook import PBSHook
from PBS_hook import BashCommand

PBS_script_template="""#!/bin/bash
#PBS -q {queue}
#PBS -N {job_name}
#PBS -l select={select}:ncpus={ncpus}:ngpus={ngpus}:mem={mem}

cd $PBS_O_WORKDIR

if [ ! -f tag_finished ]; then
  {command}
  if test $? -ne 0; then exit 1; else touch tag_finished; fi
fi
"""

PBS_array_script_template="""#!/bin/bash
#PBS -q {queue}
#PBS -N {job_name}
#PBS -J 0-{array_job_max_index}
#PBS -l select={select_when_array}:ncpus={ncpus}:ngpus={ngpus}:mem={mem}

array_job_dir_tuple={array_job_dir_tuple}

cd $PBS_O_WORKDIR

if [ -f tag_finshed ]; then exit 0; fi

dir_name=$(echo ${{array_job_dir_tuple[$PBS_ARRAY_INDEX]}} | cut -d, -f1)

cd $dir_name 
if [ ! -f tag_finished ]; then
  {command}
  if test $? -ne 0; then exit 1; else touch tag_finished; fi
fi
"""


class PBSOperator(BaseOperator):
    template_fields = ('script_filename', 'work_dir', )

    @apply_defaults
    def __init__(
            self,
            work_dir,
            mdata,
            is_array_job=False,
            *args,
            **kwargs):
        super().__init__(*args, **kwargs)
        self.work_dir = work_dir
        self.script_filename="{{ task_instance_key_str }}.pbs"
        self.mdata=mdata
        self.is_array_job=is_array_job

    def pre_execute(self, context):
        if self.is_array_job:
            sub_task_pattern = re.compile(r'.*task\.\d{6}$')
            array_job_dir_tuple=tuple( t[0]  for t in os.walk(self.work_dir) if re.match(sub_task_pattern, t[0]) )
            array_job_num=len(array_job_dir_tuple)
            print(f"change to array job. array_job_num={array_job_num}")
            if array_job_num < 1:
                raise RuntimeError(f"array job number cannot be smaller than 1.sub_task must in dir like task.000032. Please check {self.work_dir}")
            PBS_script = PBS_array_script_template.format(array_job_max_index=array_job_num-1, array_job_dir_tuple=array_job_dir_tuple, **self.mdata)
        else:
            PBS_script = PBS_script_template.format(**self.mdata)
            
        print(self.work_dir, self.script_filename)
        script_dir= os.path.join(self.work_dir, self.script_filename)
        with open(script_dir, 'w') as f:
            f.write(PBS_script)

        bash_command=f'cd {self.work_dir} && qsub {self.script_filename}'
        sub_process = BashCommand(bash_command)
        output= sub_process.stdout.readlines()
        self.log.info('Output:')
        for line in output:
            self.log.info("%s", line)

        self.job_name=str(output[0].decode('utf-8').strip())
        print(self.job_name)

    def execute(self, context):
        
        # sub_process= BashCommand(bash_command)
        # output= sub_process.stdout.readlines()
        # self.log.info('PBS Job info:')
        output = PBSHook.single_query(job_name=self.job_name)
        self.log.info('PBS job detail:')
        for line in output:
            self.log.info("%s", line)
        
        job_state = PBSHook.get_status(job_name=self.job_name)
        while job_state == 'Q'  or  job_state == 'R' or job_state == 'B':
            self.log.info(f'PBS job  state : {self.job_name} == {job_state}')
            time.sleep(20)
            job_state = PBSHook.get_status(job_name=self.job_name)
        
        
        # message = "Hello PBS {}".format(self.name)
        # print(self.name)
        # return message

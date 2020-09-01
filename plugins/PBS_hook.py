import time
from subprocess import PIPE, STDOUT, Popen
from airflow.hooks.base_hook import BaseHook
import re

def BashCommand(bash_command):
    sub_process = Popen(  # pylint: disable=subprocess-popen-preexec-fn
                ['bash', '-c', bash_command],
                stdout=PIPE,
                stderr=STDOUT)
    sub_process.wait()
    if sub_process.returncode != 0:
        raise RuntimeError(f'query command failed. The command returned a non-zero exit code. {bash_command} ~~~~~~ {sub_process.stdout.readlines()}')
    return sub_process
     

class JobStateDict(object):
    def __init__(self):
        self.state_dict = dict()
        bash_command =r'''  qstat -x |awk 'BEGIN { printf "{" } NR>=6 { printf  "\"%s\": \"%s\",", $1 , $5 } END{ printf "}" }' '''
        sub_process = BashCommand(bash_command)
        sub_process.wait()
        raw_line = sub_process.stdout.readlines()[0]
        self.state_dict=eval(raw_line.decode('utf-8').rstrip())
      
    def __getitem__(self, key):
        return self.state_dict[key]
    
    def __setitem__(self, key, value):
        self.state_dict[key]=value
    
    def get_dict(self):
        return self.state_dict
    

class PBSHook(BaseHook):
    block_query_time_interval=30
    job_state_dict=JobStateDict()
    next_allow_block_query_time= time.time() + block_query_time_interval
    @classmethod
    def my_method(cls,*args, **kwargs):
        print("Hello World")

    @classmethod
    def single_query(cls,job_name , *args, **kwargs):
        bash_command = ''' qstat -x -f {job_name} '''.format(job_name=job_name)
        sub_process = BashCommand(bash_command)
          
        job_state_pattern=re.compile(r'job_state = (\w)')
        lines=[]
        job_state=None
        for raw_line in  iter(sub_process.stdout.readline,b''):            
            line = raw_line.decode('utf-8').strip()
            lines.append(line)
            
            m=job_state_pattern.match(line)
            if m : job_state=str(m.group(1))

        if not job_state:
            raise RuntimeError(f'Query failed: job_name {job_name} not get info; lines:{lines}')
        cls.job_state_dict[job_name]=job_state
        lines.append(f"PBS update {job_name} to {job_state}")
        return lines

    @classmethod
    def get_status(cls, job_name, *args, **kwargs):
        if time.time() > cls.next_allow_block_query_time:
            cls.job_state_dict=JobStateDict()
            cls.next_allow_block_query_time= time.time() + cls.block_query_time_interval
        return cls.job_state_dict[job_name]



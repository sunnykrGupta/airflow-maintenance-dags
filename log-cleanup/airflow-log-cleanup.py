
import os
from airflow.models import DAG, Variable
from airflow.operators.bash_operator import BashOperator
from airflow.configuration import conf
from datetime import datetime, timedelta


"""
A maintenance workflow that you can deploy into Airflow to periodically clean out the task logs to avoid those getting too big.

airflow trigger_dag --conf '{"maxLogAgeInDays":30}' airflow-log-cleanup

--conf options:
    maxLogAgeInDays:<INT> - Optional

"""
# airflow-log-cleanup
DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")

# Start time to schedule
START_DATE = datetime(2018, 11, 28, 18, 00)

# get airflow
BASE_LOG_FOLDER = conf.get("core", "BASE_LOG_FOLDER")

# How often to Run. @daily - Once a day at Midnight
SCHEDULE_INTERVAL = "@daily"

# Length of days to retain the instance logs in disk
max_retention_days = 1

# Who is listed as the owner of this DAG in the Airflow Web Server
DAG_OWNER_NAME = "operations"

# List of email address to send email alerts to if this job fails
ALERT_EMAIL_ADDRESSES = []

# The job will remove those logs that are `max_retention_days` days old or older.
DEFAULT_MAX_LOG_AGE_IN_DAYS = Variable.get("max_log_age_in_days", max_retention_days)

# Whether the job should delete the logs or not. Included if you want to temporarily avoid deleting the logs
ENABLE_DELETE = True

# The number of worker nodes you have in Airflow. Will attempt to run this process for however many workers there are so that each worker gets its logs cleared.
NUMBER_OF_WORKERS = 1

default_args = {
    'owner': DAG_OWNER_NAME,
    'email': ALERT_EMAIL_ADDRESSES,
    'email_on_failure': True,
    'email_on_retry': False,
    'start_date': START_DATE,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(DAG_ID, default_args=default_args, schedule_interval=SCHEDULE_INTERVAL, start_date=START_DATE)


def get_configuration():
    get_configuration = '''
    echo "Getting Configurations ..."
    BASE_LOG_FOLDER=''' + "'" + BASE_LOG_FOLDER + "'"

    get_configuration += '''
    MAX_LOG_AGE_IN_DAYS="{{dag_run.conf.maxLogAgeInDays}}"
    if [ "$MAX_LOG_AGE_IN_DAYS" == "" ]; then
        echo "maxLogAgeInDays conf variable isnt included. Using Default ''' + str(DEFAULT_MAX_LOG_AGE_IN_DAYS) + '"'

    get_configuration += '''
        MAX_LOG_AGE_IN_DAYS=''' + "'" + str(DEFAULT_MAX_LOG_AGE_IN_DAYS) + "'"

    get_configuration += '''
    fi
    '''

    if ENABLE_DELETE:
        get_configuration += '''
    ENABLE_DELETE='true'
        '''
    else:
        get_configuration += '''
    ENABLE_DELETE='false'
        '''

    get_configuration += '''
    echo "Finished Getting Configurations" '''

    return get_configuration

#print(get_configuration())



def print_configuration():
    print_configuration = '''
    echo ; echo "Configurations:"
    echo "BASE_LOG_FOLDER:      '${BASE_LOG_FOLDER}'"
    echo "MAX_LOG_AGE_IN_DAYS:  '${MAX_LOG_AGE_IN_DAYS}'"
    echo "ENABLE_DELETE:        '${ENABLE_DELETE}'"
    '''

    return print_configuration

#print(print_configuration())

# args : 'file' , 'dirs'
def delete_files_dirs(option='file'):
    delete_files_dirs = ""

    if option == 'file':
        # clean files modified before `mtime`
        delete_files_dirs += '''
        echo ; echo "Running cleanup **FILES process..."
        FIND_STATEMENT="find ${BASE_LOG_FOLDER}/*/* -type f -mtime ${MAX_LOG_AGE_IN_DAYS}" '''
    elif option == 'dirs':
        # clean empty directory
        delete_files_dirs += '''
        echo ; echo "Running cleanup empty **FOLDER process..."
        FIND_STATEMENT="find ${BASE_LOG_FOLDER}/* -type d -empty"
        '''

    delete_files_dirs += '''
    echo ; echo "Executing Find Statement: ${FIND_STATEMENT}"
    FILES_MARKED_FOR_DELETE=`eval ${FIND_STATEMENT}`

    echo ; echo "Process will be deleting the following files/dirs:"
    echo "${FILES_MARKED_FOR_DELETE}"

    # "grep -v '^$'" - removes empty lines. "wc -l" - Counts lines
    echo ; echo "Process will be deleting `echo "${FILES_MARKED_FOR_DELETE}" | grep -v '^$' | wc -l ` file/dir"

    if [ "$ENABLE_DELETE" == "true" ];
    then
        DELETE_STMT="${FIND_STATEMENT} -delete"
        echo ; echo "Executing Delete Statement: ${DELETE_STMT}"
        eval ${DELETE_STMT}

        DELETE_STMT_EXIT_CODE=$?
        if [ "$DELETE_STMT_EXIT_CODE" != "0" ]; then
            echo "Delete process failed with exit code ${DELETE_STMT_EXIT_CODE}"
            exit ${DELETE_STMT_EXIT_CODE}
        fi
    else
        echo ; echo "WARN: You are opted to skip delete!!!"
    fi
    echo ; echo "Finished Running Cleanup Process"
    echo "------------------------------------------"
    echo ;
    '''

    return delete_files_dirs


# Combine all script
cleanup_script = get_configuration() + print_configuration() + delete_files_dirs('file') + delete_files_dirs('dirs')


for log_cleanup_id in range(1, NUMBER_OF_WORKERS + 1):

    log_cleanup = BashOperator(
        task_id='log_cleanup_worker_' + str(log_cleanup_id),
        bash_command=cleanup_script,
        provide_context=True,
        dag=dag)

from airflow import AirflowException
from airflow.decorators import dag, task
from pymongo import MongoClient

from dags import DEFAULT_DAG_ARGUMENTS
from dags.dags_utils import get_dag_param, push_dag_downstream, pull_dag_upstream
from dags.operators.DagBatchPipelineOperator import NOTICE_IDS_KEY
from dags.pipelines.notice_fetcher_pipelines import notice_fetcher_by_query_pipeline
from ted_sws import config
from ted_sws.data_manager.adapters.notice_repository import NoticeRepository
from ted_sws.data_sampler.services.notice_xml_indexer import index_eforms_notice
from ted_sws.event_manager.adapters.event_log_decorator import event_log
from ted_sws.event_manager.model.event_message import TechnicalEventMessage, EventMessageMetadata, \
    EventMessageProcessType
from ted_sws.event_manager.services.log import log_error

DAG_NAME = "fetch_notices_by_date"
BATCH_SIZE = 2000
WILD_CARD_DAG_KEY = "wild_card"
TRIGGER_COMPLETE_WORKFLOW_DAG_KEY = "trigger_complete_workflow"
TRIGGER_PARTIAL_WORKFLOW_TASK_ID = "trigger_partial_notice_proc_workflow"
TRIGGER_COMPLETE_WORKFLOW_TASK_ID = "trigger_complete_notice_proc_workflow"
CHECK_IF_TRIGGER_COMPLETE_WORKFLOW_TASK_ID = "check_if_trigger_complete_workflow"
FINISH_FETCH_BY_DATE_TASK_ID = "finish_fetch_by_date"
VALIDATE_FETCHED_NOTICES_TASK_ID = "validate_fetched_notices"


@dag(max_active_runs=1, concurrency=1, default_args=DEFAULT_DAG_ARGUMENTS, schedule_interval=None,
     catchup=False, tags=['selector', 'daily-fetch'])
def fetch_and_index_eforms_notices_by_date():
    @task
    @event_log(TechnicalEventMessage(
        message="fetch_eforms_notice_from_ted",
        metadata=EventMessageMetadata(
            process_type=EventMessageProcessType.DAG, process_name=DAG_NAME
        ))
    )
    def fetch_by_date_notice_from_ted():

        date_wildcard = get_dag_param(key=WILD_CARD_DAG_KEY)
        query = f"""TD NOT IN (C E G I D P M Q O R 0 1 2 3 4 5 6 7 8 9 B S Y V F A H J K) AND
        notice-subtype IN (10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24) AND FT~"eforms-sdk-"  AND
        PD={date_wildcard}
        """

        notice_ids = notice_fetcher_by_query_pipeline(query=query)
        if not notice_ids:
            log_error("No notices has been fetched!")
            raise AirflowException("No notices has been")
        else:
            push_dag_downstream(key=NOTICE_IDS_KEY, value=notice_ids)

    @task
    @event_log(TechnicalEventMessage(
        message="index_eforms_notices_by_date",
        metadata=EventMessageMetadata(
            process_type=EventMessageProcessType.DAG, process_name=DAG_NAME
        ))
    )
    def index_eforms_notices_by_date():
        notice_ids = pull_dag_upstream(key=NOTICE_IDS_KEY)
        mongodb_client = MongoClient(config.MONGO_DB_AUTH_URL)
        notice_repository = NoticeRepository(mongodb_client)
        for notice_id in notice_ids:
            notice = notice_repository.get(notice_id)
            indexed_notice = index_eforms_notice(notice)
            notice_repository.update(indexed_notice)

    fetch_by_date_notice_from_ted() >> index_eforms_notices_by_date()


dag = fetch_and_index_eforms_notices_by_date()

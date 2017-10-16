<<<<<<< HEAD
import gzip
import logging
import os
import pprint

from airflow.operators import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
import psycopg2
import requests

logger = logging.getLogger(__name__)
pp = pprint.PrettyPrinter(indent=2)


def download_file(url, destination_directory):
    response = requests.get(url, stream=True)
    full_path = os.path.join(destination_directory, url.rpartition("/")[-1])
    with open(full_path, "wb") as fh:
        for chunk in response.iter_content(chunk_size=1024):
            fh.write(chunk)
    return full_path

=======
import logging
import pprint
import urllib
import gzip
import psycopg2
from datetime import datetime, timedelta
from airflow.operators import BaseOperator, BashOperator 
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults


log = logging.getLogger(__name__)
pp = pprint.PrettyPrinter(indent=2)
>>>>>>> a7778964d27e7c75cd0b3cb6bbac80a1792c7938

class DownloadSceneList(BaseOperator):

    @apply_defaults
<<<<<<< HEAD
    def __init__(self, download_dir, download_url, *args, **kwargs):
        super(DownloadSceneList, self).__init__(*args, **kwargs)
        self.download_url = download_url
        self.download_dir = download_dir

    def execute(self, context):
        try:
            os.makedirs(self.download_dir)
        except OSError as exc:
            if exc.errno == 17:
                pass  # directory already exists
            else:
                raise

        logger.info("Downloading {!r}...".format(self.download_url))
        download_file(self.download_url, self.download_dir)
        logger.info("Done!")

=======
    def __init__(self, download_url, download_dir, download_timeout=timedelta(hours=1),*args, **kwargs):
        self.download_url = download_url
        self.download_dir = download_dir
        log.info('-------------------- DownloadSceneList ------------')
        super(DownloadSceneList, self).__init__(*args, **kwargs)

    def execute(self, context):
        log.info('-------------------- DownloadSceneList Execute------------')
        urllib.urlretrieve(self.download_url,self.download_dir+"scene_list.gz")
        context['task_instance'].xcom_push(key='scene_list_gz_path', value=str(self.download_dir)+"scene_list.gz")
        return True
>>>>>>> a7778964d27e7c75cd0b3cb6bbac80a1792c7938

class ExtractSceneList(BaseOperator):

    @apply_defaults
<<<<<<< HEAD
    def __init__(self, download_dir, download_url, *args, **kwargs):
        super(ExtractSceneList, self).__init__(*args, **kwargs)
        self.download_dir = download_dir
        self.download_url = download_url

    def execute(self, context):
        path_to_extract = os.path.join(
            self.download_dir,
            self.download_url.rpartition("/")[-1]
        )
        target_path = "{}.csv".format(
            os.path.splitext(path_to_extract)[0])
        logger.info("Extracting {!r} to {!r}...".format(
            path_to_extract, target_path))
        with gzip.open(path_to_extract, 'rb') as zipped_fh, \
                open(target_path, "wb") as extracted_fh:
            extracted_fh.write(zipped_fh.read())

=======
    def __init__(self, download_dir,*args, **kwargs):
        log.info('-------------------- ExtractSceneList ------------')
        self.download_dir = download_dir
        super(ExtractSceneList, self).__init__(*args, **kwargs)

    def execute(self, context):
        log.info('-------------------- ExtractSceneList Execute------------')
        scene_list_gz_path = context['task_instance'].xcom_pull('download_scene_list_gz_task', key='scene_list_gz_path')
        with gzip.open(scene_list_gz_path, 'rb') as f:
             file_content = f.read()
        outF = file(self.download_dir+"scene_list.csv", 'wb')
        outF.write(file_content)
        outF.close()
        context['task_instance'].xcom_push(key='scene_list_csv_path', value=str(self.download_dir)+"scene_list.csv")
        return True
>>>>>>> a7778964d27e7c75cd0b3cb6bbac80a1792c7938

class UpdateSceneList(BaseOperator):

    @apply_defaults
<<<<<<< HEAD
    def __init__(self, download_dir, download_url, pg_dbname, pg_hostname,
                 pg_port, pg_username, pg_password,*args, **kwargs):
        super(UpdateSceneList, self).__init__(*args, **kwargs)
        self.download_dir = download_dir
        self.download_url = download_url
=======
    def __init__(self, pg_dbname, pg_hostname, pg_port, pg_username, pg_password,*args, **kwargs):
        log.info('-------------------- UpdateSceneList ------------')
>>>>>>> a7778964d27e7c75cd0b3cb6bbac80a1792c7938
        self.pg_dbname = pg_dbname
        self.pg_hostname = pg_hostname
        self.pg_port = pg_port
        self.pg_username = pg_username
        self.pg_password = pg_password
<<<<<<< HEAD

    def execute(self, context):
        db_connection = psycopg2.connect(
            "dbname='{}' user='{}' host='{}' password='{}'".format(
                self.pg_dbname, self.pg_username, self.pg_hostname,
                self.pg_password
            )
        )
        logger.info("Deleting previous data from db...")
        with db_connection as conn:
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM scene_list;")
        filename = os.path.splitext(self.download_url.rpartition("/")[-1])[0]
        scene_list_path = os.path.join(
            self.download_dir,
            "{}.csv".format(filename))
        logger.info("Loading data from {!r} into db...".format(
            scene_list_path))
        with db_connection as conn, open(scene_list_path) as fh:
            fh.readline()
            with conn.cursor() as cursor:
                cursor.copy_from(fh, "scene_list", sep=",")
        return True


class LANDSAT8DBPlugin(AirflowPlugin):
    name = "landsat8db_plugin"
    operators = [
        DownloadSceneList,
        ExtractSceneList,
        UpdateSceneList
    ]
=======
        super(UpdateSceneList, self).__init__(*args, **kwargs)

    def execute(self, context):
        log.info('-------------------- UpdateSceneList Execute------------')
        scene_list_csv_path = context['task_instance'].xcom_pull('extract_scene_list_task', key='scene_list_csv_path')
        
        delete_first_line_cmd = "tail -n +2 " + scene_list_csv_path + "> " + scene_list_csv_path+".tmp && mv "+ scene_list_csv_path +".tmp  " + scene_list_csv_path
        delete_line_operator = BashOperator(task_id='Delete_first_line_OP', bash_command = delete_first_line_cmd)
        delete_line_operator.execute(context)

        db = psycopg2.connect("dbname='{}' user='{}' host='{}' password='{}'".format(self.pg_dbname, self.pg_username, self.pg_hostname, self.pg_password))
        cursor = db.cursor()
        cursor.execute("delete from scene_list")
        db.commit()

        fo = open(scene_list_csv_path, 'r')	
        cursor.copy_from(fo, 'scene_list',sep=',')
        db.commit()

        fo.close()
        return True

class LANDSAT8DBPlugin(AirflowPlugin):
    name = "landsat8db_plugin"
    operators = [DownloadSceneList, ExtractSceneList, UpdateSceneList]

>>>>>>> a7778964d27e7c75cd0b3cb6bbac80a1792c7938

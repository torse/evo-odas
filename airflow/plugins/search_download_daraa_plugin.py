from datetime import timedelta
from itertools import chain
import logging
import os
import psycopg2
import urllib

from airflow.operators import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults

log = logging.getLogger(__name__)


class Landsat8SearchOperator(BaseOperator):
    """Landsat8SearchOperator searches for scenes/granules to be downloaded from Landsat8DownloadOperator. It has search criteria (area of interest and cloud coverage). The current implementation is searching for granules in the created DB (from Landsat8_Scene_List DAG)

        Args:
            area (tuple): Named tuple instance contains name, path, row and bands info
            cloud_coverage (float): allowed cloud coverage percentage
            db_credentials (dict): carrying postgres connection string info
            startdate (str): date to start searching for scenes (acquisitiondate)
            enddate (str): end of date range/interval for searching scenes (acquisitiondate)
            filter_max (int): number to limit search results
            order_by (str): the column to use for ordering the returned results
            order_type (str): descending or ascending ordering

        Returns:
            tuple contains:
            product_id, entity_id, download_url
    """
    @apply_defaults
    def __init__(self, area, cloud_coverage, startdate, enddate, filter_max, order_by, order_type, db_credentials, *args, **kwargs):
        super(Landsat8SearchOperator, self).__init__(*args, **kwargs)
        self.area = area
        self.cloud_coverage = cloud_coverage
        self.startdate = startdate
        self.enddate = enddate
        self.filter_max = filter_max
        self.order_by = order_by
        self.order_type = order_type
        self.db_credentials = dict(db_credentials)

    def execute(self, context):
        if self.area is None or self.db_credentials is None:
            log.info("Either area of interest or credentials received with None.")
            return
        connection = psycopg2.connect(
            dbname=self.db_credentials["dbname"],
            user=self.db_credentials["username"],
            password=self.db_credentials["password"],
            host=self.db_credentials["hostname"],
            port=self.db_credentials["port"],
        )
        cursor = connection.cursor()
        data = (self.cloud_coverage, self.area.path, self.area.row, self.startdate, self.enddate)
        query = "SELECT productid, entityid, download_url FROM scene_list WHERE cloudCover < %s AND path = %s AND row = %s AND acquisitiondate BETWEEN '%s' AND '%s'"%(data)
        #kindly note that table name and sql keywords cannot be parametrized (e.g: using %s) so we had to use .format to order by 
        query += " ORDER BY {} {} LIMIT {} ;".format(self.order_by, self.order_type, self.filter_max)
        cursor.execute(query)
        log.info(cursor.mogrify(query, data))
        #product_id, entity_id, download_url = cursor.fetchone()
        search_results = cursor.fetchall()

        if search_results is None:
            log.error("Could not find any product for the {} area".format(self.area))
            return
        else:
            for record in search_results:
               log.info(
                   "Found {} product with {} scene id, available for download "
                   "through {} ".format(record[0], record[1], record[2]))
            return search_results

class Landsat8DownloadOperator(BaseOperator):
    """Landsat8DownloadOperator downloads scenes/granules which were found using Landsat8SearchOperator.

        Args:
            download_dir (str): path to the download directory
            get_inputs_from (str): task_id to pull the xcom value from search task
            url_fragment (str): string to be replaced with the filename(.tif/.mtl/.jpg)

        Returns:
            target_path (str) : path to the downloaded Landsat-8 product/scene 
    """

    @apply_defaults
    def __init__(self, download_dir, get_inputs_from, url_fragment,
                 download_timeout=timedelta(hours=1), *args, **kwargs):
        super(Landsat8DownloadOperator, self).__init__(
            execution_timeout=download_timeout, *args, **kwargs)
        self.download_dir = download_dir
        self.get_inputs_from = get_inputs_from
        self.url_fragment = url_fragment

    def execute(self, context):
        task_inputs = context["task_instance"].xcom_pull(self.get_inputs_from)
        if task_inputs is None or len(task_inputs) == 0:
            log.info("Nothing to process.")
            return
        for scene in task_inputs:
            product_id, entity_id, download_url = scene
            target_dir = os.path.join(self.download_dir, entity_id)
            try:
               os.makedirs(target_dir)
            except OSError as exc:
               if exc.errno == 17:  # directory already exists
                pass
<<<<<<< HEAD
        url = download_url.replace(
            "index.html", "{}_{}".format(product_id, self.url_fragment))
        target_path = os.path.join(
            target_dir,
            "{}_{}".format(product_id, self.url_fragment)
        )
        try:
            urllib.urlretrieve(url, target_path)
        except Exception:
            log.exception(
                msg="Error downloading {}".format(self.url_fragment))
            raise
        else:
            return target_path
=======
            url = download_url.replace(
               "index.html", "{}_{}".format(product_id, self.url_fragment))
            target_path = os.path.join(
               target_dir,
               "{}_{}".format(product_id, self.url_fragment)
            )
            try:
                urllib.urlretrieve(url, target_path)
            except Exception:
               log.exception(
               msg="Error downloading {}".format(self.url_fragment))
               raise
            else:
               return target_path
>>>>>>> 49efb582f75c2331f5eaa4a00fbd69c3be72975c

class SearchDownloadDaraaPlugin(AirflowPlugin):
    name = "search_download_daraa_plugin"
    operators = [
        Landsat8SearchOperator,
        Landsat8DownloadOperator
    ]

import logging, os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators import DHUSSearchOperator, DHUSDownloadOperator, Sentinel2ThumbnailOperator, Sentinel2MetadataOperator, Sentinel2ProductZipOperator, RSYNCOperator, BashOperator, PythonOperator
from geoserver_plugin import publish_product
from sentinel1.secrets import dhus_credentials, geoserver_credentials
from sentinel2.config import sentinel2_config, geoserver_url, geoserver_collection_name

log = logging.getLogger(__name__)

# Settings
default_args = {
    ##################################################
    # General configuration
    #
    'start_date': datetime.today() - timedelta(days=1),
    'owner': 'airflow',
    'depends_on_past': False,
    'provide_context': True,
    'email': ['airflow@evoodas.dlr.de'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'max_threads': 1,
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    #
}

# DAG definition
dag = DAG('Sentinel2', description='DAG for searching, filtering and downloading Sentinel-2 data from DHUS server',
          default_args = default_args,
          dagrun_timeout = timedelta(hours=10),
          schedule_interval = '0 0 * * *',
          catchup = False)

# Sentinel2- Search Task Operator
search_task = DHUSSearchOperator(task_id = 'dhus_search_task',
                                 dhus_url = 'https://scihub.copernicus.eu/dhus',
                                 dhus_user = dhus_credentials['username'],
                                 dhus_pass = dhus_credentials['password'],
                                 geojson_bbox = sentinel2_config['geojson_bbox'],
                                 startdate = sentinel2_config['startdate'],
                                 enddate = sentinel2_config['enddate'],
                                 keywords = sentinel2_config['search_keywords'],
                                 dag = dag)

# Sentinel-2 Download Task Operator
download_task = DHUSDownloadOperator(task_id = 'dhus_download_task',
                                     dhus_url = 'https://scihub.copernicus.eu/dhus',
                                     dhus_user = dhus_credentials['username'],
                                     dhus_pass = dhus_credentials['password'],
                                     download_max = sentinel2_config['download_max'],
                                     download_dir = sentinel2_config['download_dir'],
                                     dag = dag)

# Archive Sentinel-2 RSYNC Task Operator
archive_task = RSYNCOperator(task_id="original_package_upload",
                             host = sentinel2_config["rsync_hostname"], 
                             remote_usr = sentinel2_config["rsync_username"],
                             ssh_key_file = sentinel2_config["rsync_ssh_key"], 
                             remote_dir = sentinel2_config['granules_upload_dir'], 
                             xk_pull_dag_id = 'Sentinel2', 
                             xk_pull_task_id = 'dhus_download_task', 
                             xk_pull_key = 'downloaded_products_paths',
                             dag=dag)


# Sentinel-2 Create thumbnail Operator
thumbnail_task = Sentinel2ThumbnailOperator(task_id = 'dhus_thumbnail_task',
                                            thumb_size_x = '64',
                                            thumb_size_y = '64',
                                            dag=dag)

# Sentinel-2 Metadata Operator
metadata_task = Sentinel2MetadataOperator(task_id = 'dhus_metadata_task',
                                          bands_res = sentinel2_config['bands_res'],
                                          remote_dir = sentinel2_config['granules_upload_dir'],
                                          bands_dict = sentinel2_config['bands_dict'],
                                          GS_WORKSPACE = sentinel2_config['GS_WORKSPACE'], 
                                          GS_LAYER = sentinel2_config['GS_LAYER'],
                                          GS_WMS_WIDTH = sentinel2_config['GS_WMS_WIDTH'],
                                          GS_WMS_HEIGHT = sentinel2_config['GS_WMS_HEIGHT'],
                                          GS_WMS_FORMAT = sentinel2_config['GS_WMS_FORMAT'],
                                          coverage_id = sentinel2_config['coverage_id'],
                                          dag = dag)

# Archive Sentinel-2 RSYNC with .prj and .wld files Task Operator
archive_wldprj_task = RSYNCOperator(task_id="sentinel2_upload_granules",
                                    host = sentinel2_config["rsync_hostname"],
                                    remote_usr = sentinel2_config["rsync_username"],
                                    ssh_key_file = sentinel2_config["rsync_ssh_key"], 
                                    remote_dir = sentinel2_config['granules_upload_dir'], 
                                    xk_pull_dag_id = 'Sentinel2',
                                    xk_pull_task_id = 'dhus_metadata_task', 
                                    xk_pull_key = 'downloaded_products_with_wldprj',
                                    dag=dag)

# Sentinel-2 Product.zip Operator.
# The following variables are just pointing to placeholders until we implement the real files.
CWR = os.path.dirname(os.path.realpath(__file__))
placeholders_list = [os.path.join(CWR,"metadata.xml"), os.path.join(CWR,"product_abstract.html")]
generated_files_list = ['product/product.json','product/granules.json','product/thumbnail.jpeg', 'product/owsLinks.json']

product_zip_task = Sentinel2ProductZipOperator(task_id = 'product_zip_task',
                                               target_dir = sentinel2_config["product_zip_target_dir"],
                                               generated_files = generated_files_list,
                                               placeholders = placeholders_list,
                                               dag = dag)

publish_task = PythonOperator(
    task_id="publish_product",
    python_callable=publish_product,
    op_kwargs={
        'geoserver_username': geoserver_credentials['username'],
        'geoserver_password': geoserver_credentials['password'],
        'geoserver_rest_endpoint': '{}/rest/oseo/collections/{}/products'.format(
            geoserver_url, geoserver_collection_name)
    },
    dag = dag
)


search_task >> download_task >> archive_task >> thumbnail_task >> metadata_task >> archive_wldprj_task >> product_zip_task >> publish_task

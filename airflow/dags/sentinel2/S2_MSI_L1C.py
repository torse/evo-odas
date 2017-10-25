import logging, os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators import DHUSSearchOperator, DHUSDownloadOperator, Sentinel2ThumbnailOperator, Sentinel2MetadataOperator, Sentinel2ProductZipOperator, RSYNCOperator, BashOperator, PythonOperator

from geoserver_plugin import publish_product
import config as CFG
import config.s2_msi_l1c as S2MSIL1C

log = logging.getLogger(__name__)

# Settings
default_args = {
    ##################################################
    # General configuration
    #
    'start_date': datetime.now() - timedelta(hours=1),
    'owner': 'airflow',
    'depends_on_past': False,
    'provide_context': True,
    'email': ['airflow@evoodas.dlr.de'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'max_threads': 1,
    'max_active_runs': 1,
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    #
}

print("#######################")
print("Interval: ".format(S2MSIL1C.dag_schedule_interval))
print("ID: {}".format(S2MSIL1C.id))
print("DHUS:  {} @ {}, Region: {}".format(CFG.dhus_username, CFG.dhus_url, S2MSIL1C.dhus_search_bbox) )
print("GeoServer: {} @ {}".format(CFG.geoserver_username, CFG.geoserver_rest_url) )
print("RSYNC: {} @ {} using {}".format(CFG.rsync_username, CFG.rsync_hostname, CFG.rsync_ssh_key))
print("Date: {} / {}".format(S2MSIL1C.dhus_search_startdate, S2MSIL1C.dhus_search_enddate))
print("Search: max={}, order_by={}, keywords={}".format(S2MSIL1C.dhus_filter_max, S2MSIL1C.dhus_search_orderby,S2MSIL1C.dhus_search_keywords))
print("Paths:\n  collection_dir={}\n  download_dir={}\n  process_dir={}\n  original_package_upload_dir={}\n  repository_dir={}".format(S2MSIL1C.collection_dir, S2MSIL1C.download_dir, S2MSIL1C.process_dir, S2MSIL1C.original_package_upload_dir, S2MSIL1C.repository_dir))
print("Collection:\n  workspace={}\n  layer={}".format(S2MSIL1C.geoserver_workspace, S2MSIL1C.geoserver_layer))
print("Product:\n  bands_res={}\n  bands_dict={}".format(S2MSIL1C.bands_res, S2MSIL1C.bands_dict))
print("#######################")

# DAG definition
dag = DAG(S2MSIL1C.id,
          description='DAG for searching, filtering and downloading Sentinel '+S2MSIL1C.id+' data from DHUS server',
          schedule_interval=S2MSIL1C.dag_schedule_interval,
          catchup=False,
          default_args=default_args
)

# DHUS Search Task Operator
search_task = DHUSSearchOperator(task_id='search_product_task',
                                 dhus_url=CFG.dhus_url,
                                 dhus_user=CFG.dhus_username,
                                 dhus_pass=CFG.dhus_password,
                                 geojson_bbox=S2MSIL1C.dhus_search_bbox,
                                 startdate=S2MSIL1C.dhus_search_startdate,
                                 enddate=S2MSIL1C.dhus_search_enddate,
                                 filter_max=S2MSIL1C.dhus_filter_max,
                                 order_by=S2MSIL1C.dhus_search_orderby,
                                 keywords=S2MSIL1C.dhus_search_keywords,
                                 dag=dag)

# DHUS Download Task Operator
# 
# if a specific product should be downloaded, use
# product_ids={'7c08fc13-934d-422a-aee5-260966a0f6ec'} 
# or
# product_ids=('7c08fc13-934d-422a-aee5-260966a0f6ec','b6a67950-3b72-4684-9d4f-ce078d38b54a')
# instead of 
# 'get_inputs_from=...'
#
download_task = DHUSDownloadOperator(task_id='download_product_task',
                                     dhus_url=CFG.dhus_url,
                                     dhus_user=CFG.dhus_username,
                                     dhus_pass=CFG.dhus_password,
                                     download_max=S2MSIL1C.dhus_download_max,
                                     download_dir=S2MSIL1C.download_dir,
                                     get_inputs_from=search_task.task_id,
                                     download_timeout=timedelta(hours=8),
                                     dag=dag)

# Rsync Archive Task
archive_task = RSYNCOperator(task_id="upload_original_package",
                             host = CFG.rsync_hostname, 
                             remote_usr = CFG.rsync_username,
                             ssh_key_file = CFG.rsync_ssh_key, 
                             remote_dir = S2MSIL1C.original_package_upload_dir,
                             get_inputs_from=download_task.task_id,
                             dag=dag)

# Sentinel-2 Create thumbnail Operator
thumbnail_task = Sentinel2ThumbnailOperator(task_id = 'extract_thumbnail_task',
                                            thumb_size_x = '128',
                                            thumb_size_y = '128',
                                            get_inputs_from=download_task.task_id,
                                            dag=dag)

# Sentinel-2 Metadata Operator
metadata_task = Sentinel2MetadataOperator(task_id = 'extract_metadata_task',
                                          bands_res = S2MSIL1C.bands_res,
                                          bands_dict = S2MSIL1C.bands_dict,
                                          remote_dir = S2MSIL1C.repository_dir,
                                          gs_workspace = S2MSIL1C.geoserver_workspace,
                                          gs_wms_layer = S2MSIL1C.geoserver_layer,
                                          gs_wfs_featuretype = S2MSIL1C.geoserver_featuretype,
                                          gs_wfs_format = S2MSIL1C.geoserver_oseo_wfs_format,
                                          gs_wfs_version = S2MSIL1C.geoserver_oseo_wfs_version,
                                          gs_wms_width = S2MSIL1C.geoserver_oseo_wms_width,
                                          gs_wms_height = S2MSIL1C.geoserver_oseo_wms_height,
                                          gs_wms_format = S2MSIL1C.geoserver_oseo_wms_format,
                                          gs_wms_version = S2MSIL1C.geoserver_oseo_wms_version,
                                          gs_wcs_coverage_id=S2MSIL1C.geoserver_coverage,
                                          gs_wcs_scale_i = S2MSIL1C.geoserver_oseo_wcs_scale_i,
                                          gs_wcs_scale_j = S2MSIL1C.geoserver_oseo_wcs_scale_j,
                                          gs_wcs_format = S2MSIL1C.geoserver_oseo_wcs_format,
                                          gs_wcs_version = S2MSIL1C.geoserver_oseo_wcs_version,
                                          get_inputs_from = [download_task.task_id, archive_task.task_id],
                                          original_package_download_base_url = S2MSIL1C.original_package_download_base_url,
                                          dag = dag)

# Archive Sentinel-2 RSYNC with .prj and .wld files Task Operator
archive_wldprj_task = RSYNCOperator(task_id="archive_wldprj_task",
                                    host = CFG.rsync_hostname, 
                                    remote_usr = CFG.rsync_username,
                                    ssh_key_file = CFG.rsync_ssh_key, 
                                    remote_dir = S2MSIL1C.repository_dir,
                                    get_inputs_from=metadata_task.task_id,                           
                                    dag=dag)

## Sentinel-2 Product.zip Operator.
# The following variables are just pointing to placeholders until we implement the real files.
CWR = os.path.dirname(os.path.realpath(__file__))
placeholders_list = [os.path.join(CWR,"metadata.xml"), os.path.join(CWR,"description.html")]
generated_files_list = ['product/product.json','product/granules.json','product/thumbnail.jpeg', 'product/owsLinks.json']

product_zip_task = Sentinel2ProductZipOperator(task_id = 'create_product_zip_task',
                                               target_dir = S2MSIL1C.download_dir,
                                               generated_files = generated_files_list,
                                               placeholders = placeholders_list,
                                               get_inputs_from=download_task.task_id,
                                               dag = dag)

# curl -vvv -u evoadmin:\! -XPOST -H "Content-type: application/zip" --data-binary @/var/data/Sentinel-2/S2_MSI_L1C/download/S2A_MSIL1C_20170909T093031_N0205_R136_T36VUQ_20170909T093032/product.zip "http://ows-oda.eoc.dlr.de/geoserver/rest/oseo/collections/SENTINEL2/products"
publish_task = PythonOperator(task_id="publish_product_task",
                              python_callable=publish_product,
                              op_kwargs={
                                'geoserver_username': CFG.geoserver_username,
                                'geoserver_password': CFG.geoserver_password,
                                'geoserver_rest_endpoint': '{}/oseo/collections/{}/products'.format(CFG.geoserver_rest_url, S2MSIL1C.geoserver_oseo_collection),
                                'get_inputs_from': product_zip_task.task_id,
                              },
                              dag = dag)

search_task >> download_task >> archive_task >> thumbnail_task >> metadata_task >> archive_wldprj_task >> product_zip_task >> publish_task

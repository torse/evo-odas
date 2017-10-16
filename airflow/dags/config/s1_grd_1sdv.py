from datetime import datetime, timedelta
import os
import config

#
<<<<<<< HEAD
# DAG
#

dag_schedule_interval='@hourly'

#
=======
>>>>>>> a7778964d27e7c75cd0b3cb6bbac80a1792c7938
# Collection
#
id = "S1_GRD_1SDV"
filename_filter = "S1?_*_GRD?_1SDV*"
platformname = 'Sentinel-1'
collection_dir = os.path.join(config.base_dir, platformname, id)
download_dir = os.path.join(collection_dir,"download")
process_dir = os.path.join(collection_dir,"process")
upload_dir = os.path.join(collection_dir,"upload")
repository_dir = os.path.join(collection_dir,"repository")

#
# DHUS specific
#
dhus_filter_max = 5
dhus_download_max = 1
dhus_search_bbox = os.path.join(config.regions_base_dir,'europe.geojson')
dhus_search_filename = filename_filter
dhus_search_startdate = datetime.today() - timedelta(days=14)
dhus_search_startdate = dhus_search_startdate.isoformat() + 'Z'
dhus_search_enddate = datetime.now().isoformat() + 'Z'
dhus_search_orderby = '+ingestiondate'
dhus_search_keywords = {
        'filename': filename_filter,
        'platformname': platformname
}

#
# GeoServer
#
geoserver_workspace = "sentinel"
geoserver_layer = "SENTINEL1"
geoserver_coverage = "SENTINEL1"
<<<<<<< HEAD
geoserver_featuretype = "SENTINEL1_FT"
=======
>>>>>>> a7778964d27e7c75cd0b3cb6bbac80a1792c7938
geoserver_oseo_collection="SENTINEL1"
geoserver_oseo_wms_width = 768
geoserver_oseo_wms_height = 768
geoserver_oseo_wms_format = "image/png"
<<<<<<< HEAD
geoserver_oseo_wcs_scale_i = 0.01
geoserver_oseo_wcs_scale_j = 0.01
geoserver_oseo_wcs_format = "image/tiff"
=======
>>>>>>> a7778964d27e7c75cd0b3cb6bbac80a1792c7938

#
# Product
#

try:
    from override.s1_grd_1sdv import *
except:
    pass

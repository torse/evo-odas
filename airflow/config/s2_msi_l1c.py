from datetime import datetime, timedelta
import os
import config

#
# Collection
#
collection_id = "S2_MSI_L1C"
collection_filename_filter = "S2*_MSIL1C*"
collection_platformname = 'Sentinel-2'
collection_download_dir = os.path.join(config.download_base_dir, collection_platformname, collection_id)
collection_process_dir = os.path.join(config.process_base_dir, collection_platformname, collection_id)
collection_repository_dir = os.path.join(config.repository_base_dir, "SENTINEL/S2/", collection_id)

#
# DHUS specific
#
dhus_url = 'https://scihub.copernicus.eu/dhus'
dhus_username = config.dhus_username
dhus_password = config.dhus_password
dhus_search_bbox = os.path.join(config.regions_base_dir,'europe.geojson')
dhus_search_filename = collection_filename_filter
dhus_search_startdate = datetime.today() - timedelta(days=4)
dhus_search_startdate = dhus_search_startdate.isoformat() + 'Z'
dhus_search_enddate = datetime.now().isoformat() + 'Z'
dhus_search_orderby = '-ingestiondate,+cloudcoverpercentage'
dhus_download_max = 1
dhus_download_dir = collection_download_dir

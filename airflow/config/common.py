from datetime import datetime, timedelta

#
# Paths
#
download_base_dir = '/var/data/download/'
process_base_dir = '/var/data/process/'
repository_base_dir = '/var/data/repository/'
regions_base_dir = '/var/data/regions/'

#
# Connections
#
dhus_url = 'https://scihub.copernicus.eu/dhus'

geoserver_rest_url = 'http://localhost:8080/geoserver/rest'

postgresql_dbname = 'oseo'
postgresql_hostname = 'localhost'
postgresql_port = '5432'

#
# Dates
#
# yesterday at beginning of day
yesterday_start = datetime.now() - timedelta(days=1)
yesterday_start = yesterday_start.replace(hour=0, minute=0, second=0, microsecond=0)
yesterday_start = yesterday_start.isoformat() + 'Z'
# yesterday at end of day
yesterday_end = datetime.now() - timedelta(days=1)
yesterday_end = yesterday_end.replace(hour=23, minute=59, second=59, microsecond=999999)
yesterday_end = yesterday_end.isoformat() + 'Z'
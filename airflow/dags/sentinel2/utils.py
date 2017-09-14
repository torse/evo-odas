import requests
import logging
from pprint import pprint, pformat
from airflow.models import XCOM_RETURN_KEY

log = logging.getLogger(__name__)

def generate_wfs_dict(s2_product, GS_WORKSPACE, GS_LAYER):
    
    return {"offering": "http://www.opengis.net/spec/owc-atom/1.0/req/wfs",
                          "method": "GET",
                          "code": "GetFeature",
                          "type": "application/json",
                          "href": r"${BASE_URL}"+"/geoserver/ows?service=wfs&version=2.0.0&request=GetFeature&typeNames={}:{}&CQL_FILTER=eoIdentifier='{}'&outputFormat=application/json".format(GS_WORKSPACE, GS_LAYER, s2_product.manifest_safe_path.rsplit('.SAFE', 1)[0])}

def generate_wms_dict(GS_WORKSPACE, GS_LAYER, granule_coordinates, GS_WMS_WIDTH, GS_WMS_HEIGHT, GS_WMS_FORMAT, s2_product):
    bbox = str(granule_coordinates[0][3][0])+","+str(granule_coordinates[0][1][0])
    return {
            "href": r"${BASE_URL}"+"/{}/{}/ows?service=wms&request=GetMap&version=1.3.0&LAYERS={}&BBOX={}&WIDTH={}&HEIGHT={}&FORMAT=image/jpeg&CQL_FILTER=eoIdentifier='{}'".format(GS_WORKSPACE, GS_LAYER, GS_LAYER, bbox.strip(), GS_WMS_WIDTH, GS_WMS_HEIGHT, s2_product.manifest_safe_path.rsplit('.SAFE', 1)[0]), 
            "code": "GetMap", 
            "type": "image/jpeg", 
            "method": "GET", 
            "offering": "http://www.opengis.net/spec/owc-atom/1.0/req/wms"
        }

def generate_wcs_dict(granule_coordinates, GS_WORKSPACE, s2_product, coverage_id):
    long1, long2 = (float(granule_coordinates[0][3][0].split(",")[0]),float(granule_coordinates[0][1][0].split(",")[0]))
    lat1, lat2 = (float(granule_coordinates[0][3][0].split(",")[1]),float(granule_coordinates[0][1][0].split(",")[1]))
    return {"offering": "http://www.opengis.net/spec/owc-atom/1.0/req/wcs",
                          "method": "GET",
                          "code": "GetCoverage",
                          "type": "image/jpeg",
                          "href": r"${BASE_URL}"+"/{}/wcs?service=WCS&version=2.0.1&coverageId={}&request=GetCoverage&format=jpeg2000&subset=http://www.opengis.net/def/axis/OGC/0/Long({},{})&subset=http://www.opengis.net/def/axis/OGC/0/Lat({},{})&scaleaxes=i(0.1),j(0.1)&CQL_FILTER=eoIdentifier='{}'".format(GS_WORKSPACE, coverage_id, str(long1).strip(), str(long2).strip(), str(lat1).strip(), str(lat2).strip(), s2_product.manifest_safe_path.rsplit('.SAFE', 1)[0])}

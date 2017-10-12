from collections import namedtuple
import json
import logging
import os
import re
import pprint
import shutil
import zipfile

from airflow.operators import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from pgmagick import Image
from osgeo import osr

log = logging.getLogger(__name__)
pp = pprint.PrettyPrinter(indent=2)

BAND_NAMES = {
    "B1": "B1",
    "B2": "B2",
    "B3": "B3",
    "B4": "B4",
    "B5": "B5",
    "B6": "B6",
    "B7": "B7",
    "B8": "B8",
    "B9": "B9",
    "B10": "B10",
    "B11": "B11",
    "B12": "B12",
}

BoundingBox = namedtuple("BoundingBox", [
    "ullon",
    "ullat",
    "urlon",
    "urlat",
    "lllon",
    "lllat",
    "lrlon",
    "lrlat"
])


def parse_mtl_data(buffer):
    """Parse input file-like object that contains metadata in MTL format."""
    metadata = {}
    current = metadata
    previous = metadata
    for line in buffer:
        key, value = (i.strip() for i in line.partition("=")[::2])
        if value == "L1_METADATA_FILE":
            pass
        elif key == "END":
            pass
        elif key == "GROUP":
            current[value] = {}
            previous = current
            current = current[value]
        elif key == "END_GROUP":
            current = previous
        elif key == "":
            pass
        else:
            try:
                parsed_value = int(value)
            except ValueError:
                try:
                    parsed_value = float(value)
                except ValueError:
                    parsed_value = str(value.replace('"', ""))
            current[key] = parsed_value
    return metadata


def get_bounding_box(product_metadata):
    return BoundingBox(
        ullon=float(product_metadata["CORNER_UL_LON_PRODUCT"]),
        ullat=float(product_metadata["CORNER_UL_LAT_PRODUCT"]),
        urlon=float(product_metadata["CORNER_UR_LON_PRODUCT"]),
        urlat=float(product_metadata["CORNER_UR_LAT_PRODUCT"]),
        lllon=float(product_metadata["CORNER_LL_LON_PRODUCT"]),
        lllat=float(product_metadata["CORNER_LL_LAT_PRODUCT"]),
        lrlon=float(product_metadata["CORNER_LR_LON_PRODUCT"]),
        lrlat=float(product_metadata["CORNER_LR_LAT_PRODUCT"]),
    )



def prepare_metadata(metadata, bounding_box, crs):

    date_acqired = metadata["PRODUCT_METADATA"]["DATE_ACQUIRED"]
    scene_center_time = metadata["PRODUCT_METADATA"]["SCENE_CENTER_TIME"]
    time_start_end = date_acqired + 'T' + scene_center_time.split('.')[0] + 'Z'

    return {
        "type": "Feature",
        "geometry": {
            "type": "Polygon",
            "coordinates": [[
                [bounding_box.ullat, bounding_box.ullon],
                [bounding_box.lllat, bounding_box.lllon],
                [bounding_box.lrlat, bounding_box.lrlon],
                [bounding_box.urlat, bounding_box.urlon],
                [bounding_box.ullat, bounding_box.ullon],
            ]],
        },
        "properties": {
            "eop:identifier": metadata[
                "METADATA_FILE_INFO"]["LANDSAT_PRODUCT_ID"],
            "timeStart": time_start_end,
            "timeEnd": time_start_end,
            "originalPackageLocation": None,
            "thumbnailURL": None,
            "quicklookURL": None,
            "crs": "EPSG:" + crs,
            "eop:parentIdentifier": "LANDSAT8",
            "eop:productionStatus": None,
            "eop:acquisitionType": None,
            "eop:orbitNumber": None,
            "eop:orbitDirection": None,
            "eop:track": None,
            "eop:frame": None,
            "eop:swathIdentifier": None,
            "opt:cloudCover": metadata["IMAGE_ATTRIBUTES"]["CLOUD_COVER"],
            "opt:snowCover": None,
            "eop:productQualityStatus": None,
            "eop:productQualityDegradationStatus": None,
            "eop:processorName": metadata[
                "METADATA_FILE_INFO"]["PROCESSING_SOFTWARE_VERSION"],
            "eop:processingCenter": None,
            "eop:creationDate": None,
            "eop:modificationDate": metadata[
                "METADATA_FILE_INFO"]["FILE_DATE"],
            "eop:processingDate": None,
            "eop:sensorMode": None,
            "eop:archivingCenter": None,
            "eop:processingMode": None,
            "eop:availabilityTime": None,
            "eop:acquisitionStation": metadata[
                "METADATA_FILE_INFO"]["STATION_ID"],
            "eop:acquisitionSubtype": None,
            "eop:startTimeFromAscendingNode": None,
            "eop:completionTimeFromAscendingNode": None,
            "eop:illuminationAzimuthAngle": metadata[
                "IMAGE_ATTRIBUTES"]["SUN_AZIMUTH"],
            "eop:illuminationZenithAngle": None,
            "eop:illuminationElevationAngle": metadata[
                "IMAGE_ATTRIBUTES"]["SUN_ELEVATION"],
            "eop:resolution": metadata[
                "PROJECTION_PARAMETERS"]["GRID_CELL_SIZE_REFLECTIVE"]
        }
    }


def prepare_granules(bounding_box, granule_paths):
    coordinates=[[
                        [bounding_box.ullon, bounding_box.ullat],
                        [bounding_box.lllon, bounding_box.lllat],
                        [bounding_box.lrlon, bounding_box.lrlat],
                        [bounding_box.urlon, bounding_box.urlat],
                        [bounding_box.ullon, bounding_box.ullat],
    ]]

    granules_dict = {
        "type": "FeatureCollection",
        "features": []
    }

    for i in range(len(granule_paths)):
        path = granule_paths[i]
        name_no_ext = os.path.splitext(os.path.basename(path))[0]
        band_name = name_no_ext.split('_')[-1:][0]
        log.info("Band Name: {} type: {}".format(band_name, type(band_name)))
        feature={
            "type": "Feature",
            "geometry": {
                "type": "Polygon",
                "coordinates": coordinates,
            },
            "properties": {
                "band": BAND_NAMES[band_name],
                "location": granule_paths[i]
            },
            "id": "GRANULE.{}".format(i+1)
        }
        granules_dict["features"].append(feature)

    return granules_dict


class Landsat8MTLReaderOperator(BaseOperator):
    """
    This class will read the .MTL file which is attached with the
    scene/product directory.The returned value from "final_metadata_dict"
    will be a python dictionary holding all the available keys from the
    .MTL file. this dictionary will be saved as json file to be added later
    to the product.zip. Also, the execute method will "xcom.push" the
    absolute path of the generated json file inside the context of the task
    """

    @apply_defaults
    def __init__(self, get_inputs_from, loc_base_dir, metadata_xml_path,
                 *args, **kwargs):
        super(Landsat8MTLReaderOperator, self).__init__(*args, **kwargs)
        self.get_inputs_from = get_inputs_from
        self.metadata_xml_path = metadata_xml_path
        self.loc_base_dir = loc_base_dir

    def execute(self, context):
        # fetch MTL file path from XCom
        mtl_path = context["task_instance"].xcom_pull(self.get_inputs_from["metadata_task_id"])
        # Uploaded granules paths from XCom
        upload_granules_task_ids = self.get_inputs_from["upload_task_ids"]
        granule_paths=[]
        for tid in upload_granules_task_ids:
            granule_paths += context["task_instance"].xcom_pull(tid)
        # Get GDALInfo output from XCom
        gdalinfo_task_id = self.get_inputs_from["gdalinfo_task_id"]
        gdalinfo_dict = context["task_instance"].xcom_pull(gdalinfo_task_id)
        # Get GDALInfo output of one of the granules, CRS will be the same for all granules
        k = gdalinfo_dict.keys()[0]
        gdalinfo_out=gdalinfo_dict[k]
        # Extract projection WKT and get EPSG code
        match = re.findall(r'^(PROJCS.*]])', gdalinfo_out, re.MULTILINE | re.DOTALL)
        wkt_def = match[0]
        assert wkt_def is not None
        assert isinstance(wkt_def, basestring) or isinstance(wkt_def, str)
        sref = osr.SpatialReference()
        sref.ImportFromWkt(wkt_def)
        crs = sref.GetAttrValue("AUTHORITY",1)

        with open(mtl_path) as mtl_fh:
            parsed_metadata = parse_mtl_data(mtl_fh)
        bounding_box = get_bounding_box(parsed_metadata["PRODUCT_METADATA"])
        log.debug("BoundingBox: {}".format(pprint.pformat(bounding_box)))
        prepared_metadata = prepare_metadata(parsed_metadata, bounding_box, crs)
        product_directory, mtl_name = os.path.split(mtl_path)
        location = os.path.join(self.loc_base_dir, product_directory, mtl_name)
        granules_dict = prepare_granules(bounding_box, granule_paths)
        log.debug("Granules Dict: {}".format(pprint.pformat(granules_dict)))
        json_path = os.path.join(product_directory, "product.json")
        granules_path = os.path.join(product_directory, "granules.json")
        xml_template_path = os.path.join(product_directory, "metadata.xml")
        with open(json_path, 'w') as out_json_fh:
            json.dump(prepared_metadata, out_json_fh)
        with open(granules_path, 'w') as out_granules_fh:
            json.dump(granules_dict, out_granules_fh)
        shutil.copyfile(self.metadata_xml_path, xml_template_path)
        return json_path, granules_path, xml_template_path


class Landsat8ThumbnailOperator(BaseOperator):
    """This class will create a compressed, low resolution, square shaped
    thumbnail for the original scene then return the absolute path of the
    generated thumbnail
    """

    @apply_defaults
    def __init__(self, get_inputs_from, thumb_size_x, thumb_size_y,
                 *args, **kwargs):
        super(Landsat8ThumbnailOperator, self).__init__(*args, **kwargs)
        self.get_inputs_from = get_inputs_from
        self.thumb_size_x = thumb_size_x
        self.thumb_size_y = thumb_size_y

    def execute(self, context):
        downloaded_thumbnail = context["task_instance"].xcom_pull(
            self.get_inputs_from)
        log.info("downloaded_thumbnail: {}".format(downloaded_thumbnail))
        img = Image(downloaded_thumbnail)
        least_dim = min(int(img.columns()), int(img.rows()))
        img.crop("{dim}x{dim}".format(dim=least_dim))
        img.scale(self.thumb_size_x+'x'+self.thumb_size_y)
        img.scale("{}x{}".format(self.thumb_size_x, self.thumb_size_y))
        output_path = os.path.join(
            os.path.dirname(downloaded_thumbnail), "thumbnail.jpeg")
        img.write(output_path)
        return output_path


class Landsat8ProductDescriptionOperator(BaseOperator):
    """This class will create a .html description file by copying it from
    its config path
    """
    @apply_defaults
    def __init__(self, description_template, download_dir, *args, **kwargs):
        super(Landsat8ProductDescriptionOperator, self).__init__(
            *args, **kwargs)
        self.description_template = description_template
        self.download_dir = download_dir

    def execute(self, context):
        output_path = os.path.join(self.download_dir, "description.html")
        shutil.copyfile(self.description_template, output_path)
        return output_path


class Landsat8ProductZipFileOperator(BaseOperator):
    """This class will create product.zip file utilizing from the previous
    tasks
    """

    @apply_defaults
    def __init__(self, get_inputs_from, output_dir, *args, **kwargs):
        super(Landsat8ProductZipFileOperator, self).__init__(*args, **kwargs)
        self.get_inputs_from = get_inputs_from
        self.output_dir = output_dir

    def execute(self, context):
        paths_to_zip = []
        for input_provider in self.get_inputs_from:
            inputs = context["task_instance"].xcom_pull(input_provider)
            if isinstance(inputs, basestring):
                paths_to_zip.append(inputs)
            else:  # the Landsat8MTLReaderOperator returns a tuple of strings
                paths_to_zip.extend(inputs)
        log.info("paths_to_zip: {}".format(paths_to_zip))
        output_path = os.path.join(self.output_dir, "product.zip")
        output_paths = [ output_path ]
        with zipfile.ZipFile(output_path, "w") as zip_handler:
            for path in paths_to_zip:
                zip_handler.write(path, os.path.basename(path))
        return output_paths


class Landsat8GranuleJsonFileOperator(BaseOperator):

    @apply_defaults
    def __init__(self, location_prop, *args, **kwargs):
        self.location_prop = location_prop
        super(Landsat8GranuleJsonFileOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        return True


class LANDSAT8METADATAPlugin(AirflowPlugin):
    name = "landsat8_metadata_plugin"
    operators = [
        Landsat8MTLReaderOperator,
        Landsat8ThumbnailOperator,
        Landsat8ProductDescriptionOperator,
        Landsat8ProductZipFileOperator
    ]

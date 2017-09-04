from airflow.operators import BaseOperator, BashOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
import logging
import os
import s2reader
log = logging.getLogger(__name__)
from pgmagick import Image, Blob
import zipfile, json
import shutil
import xml.etree.ElementTree as ET


''' This class will create a compressed, low resolution, square shaped thumbnail for the
original granule. the current approach is saving the created thumbnail inside the zip file
'''
class Sentinel2ThumbnailOperator(BaseOperator):

        @apply_defaults
        def __init__(self, thumb_size_x, thumb_size_y, *args, **kwargs):
                self.thumb_size_x = thumb_size_x
                self.thumb_size_y = thumb_size_y
                super(Sentinel2ThumbnailOperator, self).__init__(*args, **kwargs)

        def execute(self, context):
            self.downloaded_products = context['task_instance'].xcom_pull('dhus_download_task', key='downloaded_products')
            log.info(self.downloaded_products)
            ids=[]
            for p in self.downloaded_products:
                ids.append(self.downloaded_products[p]["id"])
            print "downloaded products keys :",self.downloaded_products.keys()[0]
            for product in self.downloaded_products.keys():
                with s2reader.open(product) as safe_product:
                  print safe_product.generation_time
                  for granule in safe_product.granules:
                     try:
                         zipf = zipfile.ZipFile(product, 'a')
                         imgdata = zipf.read(granule.pvi_path,'r')
                         img = Blob(imgdata)
                         img = Image(img)
                         img.scale(self.thumb_size_x+'x'+self.thumb_size_y)
                         img.quality(80)
                         thumbnail_path = product.split(".")[0]+".jpg"
                         img.write(str(thumbnail_path))
                         print str(thumbnail_path)
                         print os.path.join(str(thumbnail_path).rsplit('/',1)[-1])
                         #print str(thumbnail_path).split(".")[0]
                         zipf.write(str(thumbnail_path),"product/thumbnail.jpeg")
                     except:
                         return False
            context['task_instance'].xcom_push(key='thumbnail_jpeg_abs_path', value=str(thumbnail_path))
            context['task_instance'].xcom_push(key='ids', value=ids)
            #return os.path.join(self.downloaded_products,"thumbnail.jpeg")

'''
This class is creating the product.zip contents and passing the absolute path per every file so that the Sentinel2ProductZipOperator can generate the product.zip file.
Also, this class is creating the .wld and .prj files which are required by Geoserver in order to be publish the granules successfully. 
'''
class Sentinel2MetadataOperator(BaseOperator):
    @apply_defaults
    def __init__(self, bands_res, remote_dir, *args, **kwargs):
        self.bands_res = bands_res
        self.remote_dir = remote_dir
        super(Sentinel2MetadataOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        self.downloaded_products = context['task_instance'].xcom_pull('dhus_download_task', key='downloaded_products')
        for product in self.downloaded_products.keys():
            with s2reader.open(product) as s2_product:
                coords = []
                x = [[[m.replace(" ", ",")] for m in str(s2_product.footprint).replace(", ", ",").partition('((')[-1].rpartition('))')[0].split(",")]]
                for item in x[0]:
                    [x, y] = item[0].split(",")
                    coords.append([float(x), float(y)])
                final_metadata_dict = {"type": "Feature", "geometry":
                {"type": "Polygon", "coordinates":
                [coords]},
                "properties": {"eop:identifier": s2_product.manifest_safe_path.rsplit('.SAFE', 1)[0],
                "timeStart": s2_product.product_start_time,
                "timeEnd": s2_product.product_stop_time,
                "originalPackageLocation": None, "thumbnailURL": None,
                "quicklookURL": None, "eop:parentIdentifier": "SENTINEL2",
                "eop:productionStatus": None, "eop:acquisitionType": None,
                "eop:orbitNumber": s2_product.sensing_orbit_number, "eop:orbitDirection": s2_product.sensing_orbit_direction,
                "eop:track": None, "eop:frame": None, "eop:swathIdentifier": None,
                "opt:cloudCover": None,
                "opt:snowCover": None, "eop:productQualityStatus": None,
                "eop:productQualityDegradationStatus": None,
                "eop:processorName": None,
                "eop:processingCenter": None, "eop:creationDate": None,
                "eop:modificationDate": None,
                "eop:processingDate": None, "eop:sensorMode": None,
                "eop:archivingCenter": None, "eop:processingMode": None,
                "eop:availabilityTime": None,
                "eop:acquisitionStation": None,
                "eop:acquisitionSubtype": None,
                "eop:startTimeFromAscendingNode": None,
                "eop:completionTimeFromAscendingNode": None,
                "eop:illuminationAzimuthAngle": None,
                "eop:illuminationZenithAngle": None,
                "eop:illuminationElevationAngle": None, "eop:resolution": None}}
                for i in self.bands_res.values():
                    features_list = []
                    granule_counter = 1
                    for granule in s2_product.granules:
                        coords = []
                        x = [[[m.replace(" ", ",")] for m in str(granule.footprint).replace(", ", ",").partition('((')[-1].rpartition('))')[0].split(",")]]
                        for item in x[0]:
                            [x, y] = item[0].split(",")
                            coords.append([float(x), float(y)])
                        zipped_product = zipfile.ZipFile(product)
                        for file_name in zipped_product.namelist():
                            if file_name.endswith('.jp2') and not file_name.endswith('PVI.jp2'):
                                 log.info("file_name")
                                 log.info(file_name)
                                 features_list.append({"type": "Feature", "geometry": { "type": "Polygon", "coordinates": [coords]},\
                        "properties": {\
                        "location":os.path.join(self.remote_dir, granule.granule_path.rsplit("/")[-1], "IMG_DATA", file_name.rsplit("/")[-1])},\
                        "id": "GRANULE.{}".format(granule_counter)})
                                 granule_counter+=1
            final_granules_dict = {"type": "FeatureCollection", "features": features_list}
            with open('product.json', 'w') as product_outfile:
                json.dump(final_metadata_dict, product_outfile)
            product_zipf = zipfile.ZipFile(product, 'a')
            product_zipf.write("product.json","product/product.json")
            with open('granules.json', 'w') as granules_outfile:
                json.dump(final_granules_dict, granules_outfile)
            product_zipf.write("granules.json","product/granules.json")
            product_zipf.close()
        archives = context['task_instance'].xcom_pull('dhus_download_task', key='downloaded_products_paths')
        archives_list = archives.split()
        log.info(archives)
        self.custom_archived = []
        for archive_line in archives_list:
            jp2_files_paths = []
            files_to_archive = []
            archive_path = archive_line
            archived_product = zipfile.ZipFile(archive_line)
            for file_name in archived_product.namelist():
                if file_name.endswith('.jp2') and not file_name.endswith('PVI.jp2'):
                    archived_product.extract(file_name, archive_path.strip(".zip"))
                    jp2_files_paths.append(os.path.join(archive_path.strip(".zip"),file_name))
                if file_name.endswith('MTD_TL.xml'):
                    archived_product.extract(file_name, archive_path.strip(".zip"))
                    mtd_tl_xml = os.path.join(archive_path.strip(".zip"),file_name)
            tree = ET.parse(mtd_tl_xml)
            root = tree.getroot()
            geometric_info = root.find(root.tag.split('}', 1)[0]+"}Geometric_Info")
            tile_geocoding = geometric_info.find("Tile_Geocoding")
            wld_files = []
            prj_files = []
            for jp2_file in jp2_files_paths:
                wld_name = os.path.splitext(jp2_file)[0]
                gdalinfo_cmd = "gdalinfo {} > {}".format(jp2_file, wld_name+".prj")
                gdalinfo_BO = BashOperator(task_id="bash_operator_gdalinfo_{}".format(wld_name[-3:]), bash_command = gdalinfo_cmd)
                gdalinfo_BO.execute(context)
                sed_cmd = "sed -i -e '1,4d;29,37d' {}".format(wld_name+".prj")
                sed_BO = BashOperator(task_id="bash_operator_sed_{}".format(wld_name[-3:]), bash_command = sed_cmd)
                sed_BO.execute(context)
                prj_files.append(wld_name+".prj")
                wld_file = open(wld_name+".wld","w")
                wld_files.append(wld_name+".wld")
                for key,value in  self.bands_res.items():
                    if wld_name[-3:] in value:
                        element = key
                geo_position = tile_geocoding.find('.//Geoposition[@resolution="{}"]'.format(element))
                wld_file.write(geo_position.find("XDIM").text + "\n" + "0" + "\n" + "0" +"\n")
                wld_file.write(geo_position.find("YDIM").text + "\n")
                wld_file.write(geo_position.find("ULX").text + "\n")
                wld_file.write(geo_position.find("ULY").text + "\n")
            files_to_archive.extend(prj_files + wld_files + jp2_files_paths)
            parent_dir = os.path.dirname(jp2_files_paths[0])
            self.custom_archived.append(os.path.dirname(parent_dir))
        context['task_instance'].xcom_push(key='downloaded_products', value=self.downloaded_products)
        context['task_instance'].xcom_push(key='downloaded_products_with_wldprj', value=' '.join(self.custom_archived))


'''
This class is receiving the meta data files paths from the Sentinel2MetadataOperator then creates the product.zip
Later, this class will pass the path of the created product.zip to the next task to publish on Geoserver.
'''
class Sentinel2ProductZipOperator(BaseOperator):

    @apply_defaults
    def __init__(self, target_dir, generated_files, placeholders, *args, **kwargs):
        self.target_dir = target_dir
        self.generated_files = generated_files
        self.placeholders = placeholders
        super(Sentinel2ProductZipOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        self.downloaded_products = context['task_instance'].xcom_pull('dhus_metadata_task', key='downloaded_products')

        for zipf in self.downloaded_products.keys():
            with zipfile.ZipFile(zipf) as zf:
                dirname = os.path.join(self.target_dir, os.path.splitext(os.path.basename(zipf))[0])
                for item_file in self.generated_files:
                    zf.extract(item_file, path=dirname)
            for ph in self.placeholders:
                shutil.copyfile(ph, os.path.join(dirname, os.path.join("product",ph.split("/")[-1])))
            product_zip_path = os.path.join(os.path.join(zipf.strip(".zip"), "product.zip"))
            product_zip = zipfile.ZipFile(product_zip_path , 'a')
            for item in os.listdir(os.path.join(zipf.strip(".zip"),"product")):
                print os.path.join(zipf.strip(".zip"),"product",item)
                product_zip.write(os.path.join(zipf.strip(".zip"),"product",item), item)
            product_zip.close()
            context['task_instance'].xcom_push(key='product_zip_path', value=product_zip_path)

class SENTINEL2Plugin(AirflowPlugin):
    name = "sentinel2_plugin"
    operators = [Sentinel2ThumbnailOperator,Sentinel2MetadataOperator,Sentinel2ProductZipOperator]

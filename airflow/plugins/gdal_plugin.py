<<<<<<< HEAD
from itertools import count
import logging
import os
import pprint
from subprocess import check_output

=======
>>>>>>> a7778964d27e7c75cd0b3cb6bbac80a1792c7938
from airflow.operators import BashOperator
from airflow.operators import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
<<<<<<< HEAD
from airflow.models import XCOM_RETURN_KEY

log = logging.getLogger(__name__)


def get_overview_levels(max_level):
    levels = []
    current = 2
    while current <= max_level:
        levels.append(current)
        current *= 2
    return levels


def get_gdaladdo_command(source, overview_levels, resampling_method,
                         compress_overview=None):
    compress_token = (
        "--config COMPRESS_OVERVIEW {}".format(compress_overview) if
        compress_overview is not None else ""
    )
    return "gdaladdo {compress} -r {method} {src} {levels}".format(
        method=resampling_method,
        compress=compress_token,
        src=source,
        levels=" ".join(str(level) for level in overview_levels),
    )


def get_gdal_translate_command(source, destination, output_type,
                               creation_options):
    return (
        "gdal_translate -ot {output_type} {creation_opts} "
        "{src} {dst}".format(
            output_type=output_type,
            creation_opts=_get_gdal_creation_options(**creation_options),
            src=source,
            dst=destination
        )
    )


class GDALWarpOperator(BaseOperator):

    @apply_defaults
    def __init__(self, target_srs, tile_size, overwrite, srcfile=None,
                 dstdir=None, xk_pull_dag_id=None, xk_pull_task_id=None,
                 xk_pull_key_srcfile=None, xk_pull_key_dstdir=None,
                 xk_push_key=None, *args, **kwargs):
=======
import os
import logging
import math
import json
import shutil
from pgmagick import Image, Geometry
import zipfile

log = logging.getLogger(__name__)

class GDALWarpOperator(BaseOperator):

    @apply_defaults
    def __init__(self, target_srs, tile_size, overwrite, srcfile=None, dstdir=None, xk_pull_dag_id=None, xk_pull_task_id=None, xk_pull_key_srcfile=None, xk_pull_key_dstdir=None, xk_push_key=None, *args, **kwargs):
>>>>>>> a7778964d27e7c75cd0b3cb6bbac80a1792c7938
        self.target_srs = target_srs
        self.tile_size = str(tile_size)
        self.overwrite = overwrite
        self.srcfile = srcfile
        self.dstdir = dstdir
        self.xk_pull_dag_id = xk_pull_dag_id
        self.xk_pull_task_id = xk_pull_task_id
        self.xk_pull_key_srcfile = xk_pull_key_srcfile
        self.xk_pull_key_dstdir = xk_pull_key_dstdir
        self.xk_push_key = xk_push_key
<<<<<<< HEAD
=======

>>>>>>> a7778964d27e7c75cd0b3cb6bbac80a1792c7938
        super(GDALWarpOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        log.info('--------------------GDAL_PLUGIN Warp running------------')
        task_instance = context['task_instance']
<<<<<<< HEAD
=======

>>>>>>> a7778964d27e7c75cd0b3cb6bbac80a1792c7938
        log.info("""
            target_srs: {}
            tile_size: {}
            overwrite: {}
            srcfile: {}
            dstdir: {}
            xk_pull_task_id: {}
            xk_pull_key_srcfile: {}
            xk_pull_key_dstdir: {}
            xk_push_key: {}
            xk_pull_dag_id: {}
            """.format(
            self.target_srs,
            self.tile_size,
            self.overwrite,
            self.srcfile,
            self.dstdir,
            self.xk_pull_task_id,
            self.xk_pull_key_srcfile,
            self.xk_pull_key_dstdir,
            self.xk_push_key,
            self.xk_pull_dag_id
            )
        )
        # init XCom parameters
        if self.xk_pull_dag_id is None:
            self.xk_pull_dag_id = context['dag'].dag_id
        # check input file path passed otherwise look for it in XCom
        if self.srcfile is not None:
            srcfile = self.srcfile
        else:
            if self.xk_pull_key_srcfile is None:
                self.xk_pull_key_srcfile = 'srcdir'
            log.debug('Fetching srcfile from XCom')
<<<<<<< HEAD
            srcfile = task_instance.xcom_pull(
                task_ids=self.xk_pull_task_id,
                key=self.xk_pull_key_srcfile,
                dag_id=self.xk_pull_dag_id
            )
=======
            srcfile = task_instance.xcom_pull(task_ids=self.xk_pull_task_id, key=self.xk_pull_key_srcfile, dag_id=self.xk_pull_dag_id)
>>>>>>> a7778964d27e7c75cd0b3cb6bbac80a1792c7938
            if srcfile is None:
                log.warn('No srcfile fetched from XCom. Nothing to do')
                return False
        assert srcfile is not None
        log.info('srcfile: %s', srcfile)

        # extract filename and directory path
        srcfilename = os.path.basename(srcfile)
        srcdir = os.path.dirname(srcfile)

        # check output file path passed otherwise try to find it in XCom
        if self.dstdir is not None:
            dstdir = self.dstdir
        else:
            if self.xk_pull_key_dstdir is None:
                self.xk_pull_key_dstdir = 'dstdir'
            log.debug('Fetching dstdir from XCom')
<<<<<<< HEAD
            dstdir = task_instance.xcom_pull(
                task_ids=self.xk_pull_task_id,
                key=self.xk_pull_key_dstdir,
                dag_id=context['dag'].dag_id
            )
=======
            dstdir = task_instance.xcom_pull(task_ids=self.xk_pull_task_id, key=self.xk_pull_key_dstdir, dag_id=context['dag'].dag_id)
>>>>>>> a7778964d27e7c75cd0b3cb6bbac80a1792c7938
            log.info('No dstdir fetched from XCom')
        # not found in XCom? use source directory
        if dstdir is None:
            log.info("using srcdir as dstdir")
            dstdir = srcdir
        log.info('dstdir: %s', dstdir)
        dstfile = os.path.join(dstdir, srcfilename)
        assert dstfile is not None
        log.info('dstfile: %s', dstfile)

        # build gdalwarp command
        self.overwrite = '-overwrite' if self.overwrite else ''
<<<<<<< HEAD
        gdalwarp_command = (
            'gdalwarp ' + self.overwrite + ' -t_srs ' + self.target_srs +
            ' -co TILED=YES -co BLOCKXSIZE=' + self.tile_size +
            ' -co BLOCKYSIZE=' + self.tile_size + ' ' + srcfile + ' ' +
            dstfile
        )
=======
        gdalwarp_command = 'gdalwarp ' + self.overwrite + ' -t_srs ' + self.target_srs + ' -co TILED=YES -co BLOCKXSIZE=' + self.tile_size + ' -co BLOCKYSIZE=' + self.tile_size + ' ' + srcfile + ' ' + dstfile
>>>>>>> a7778964d27e7c75cd0b3cb6bbac80a1792c7938
        log.info('The complete GDAL warp command is: %s', gdalwarp_command)

        # push output path to XCom
        if self.xk_push_key is None:
            self.xk_push_key = 'dstfile'
        task_instance.xcom_push(key='dstfile', value=dstfile)

        # run the command
<<<<<<< HEAD
        bo = BashOperator(
            task_id="bash_operator_warp", bash_command=gdalwarp_command)
=======
        bo = BashOperator(task_id="bash_operator_warp", bash_command=gdalwarp_command)
>>>>>>> a7778964d27e7c75cd0b3cb6bbac80a1792c7938
        bo.execute(context)


class GDALAddoOperator(BaseOperator):

    @apply_defaults
<<<<<<< HEAD
    def __init__(self, get_inputs_from, resampling_method,
                 max_overview_level, compress_overview=None,
                 *args, **kwargs):
        super(GDALAddoOperator, self).__init__(*args, **kwargs)
        self.get_inputs_from = get_inputs_from
        self.resampling_method = resampling_method
        self.max_overview_level = int(max_overview_level)
        self.compress_overview = compress_overview

    def execute(self, context):
        input_paths = context["task_instance"].xcom_pull(self.get_inputs_from, key=XCOM_RETURN_KEY)
        input_path = input_paths[0]

        levels = get_overview_levels(self.max_overview_level)
        log.info("Generating overviews for {!r}...".format(input_path))
        command = get_gdaladdo_command(
            input_path, overview_levels=levels,
            resampling_method=self.resampling_method,
            compress_overview=self.compress_overview
        )
        output_path= input_path
        bo = BashOperator(
            task_id='bash_operator_addo_{}'.format(
                os.path.basename(input_path)),
            bash_command=command
        )
        bo.execute(context)
        return output_path


class GDALTranslateOperator(BaseOperator):

    @apply_defaults
    def __init__(self, get_inputs_from, output_type="UInt16",
                 creation_options=None, *args, **kwargs):
        super(GDALTranslateOperator, self).__init__(*args, **kwargs)
        self.get_inputs_from = get_inputs_from
        self.output_type = str(output_type)
        self.creation_options = dict(
            creation_options) if creation_options is not None else {
            "tiled": True,
            "blockxsize": 512,
            "blockysize": 512,
        }

    def execute(self, context):
        downloaded_path = context["task_instance"].xcom_pull(
            self.get_inputs_from)
        working_dir = os.path.join(os.path.dirname(downloaded_path),
                                   "__translated")
        try:
            os.makedirs(working_dir)
        except OSError as exc:
            if exc.errno == 17:
                pass  # directory already exists
            else:
                raise

        output_img_filename = 'translated_{}'.format(
            os.path.basename(downloaded_path))
        output_path = os.path.join(working_dir, output_img_filename)
        command = get_gdal_translate_command(
            source=downloaded_path, destination=output_path,
            output_type=self.output_type,
            creation_options=self.creation_options
        )
        log.info("The complete GDAL translate command is: {}".format(command))
        b_o = BashOperator(
            task_id="bash_operator_translate",
            bash_command=command
        )
        b_o.execute(context)

        output_paths = [output_path]
        return output_paths

class GDALInfoOperator(BaseOperator):

    @apply_defaults
    def __init__(self, get_inputs_from, *args, **kwargs):
        super(GDALInfoOperator, self).__init__(*args, **kwargs)
        self.get_inputs_from = get_inputs_from

    def execute(self, context):
        input_paths = []
        for get_input in self.get_inputs_from:
            input_paths.append(context["task_instance"].xcom_pull(get_input))
        gdalinfo_outputs = {}
        for input_path in input_paths:
            command = ["gdalinfo"]
            log.info("Running GDALInfo on {}...".format(input_path))
            command.append(input_path)
            gdalinfo_output = check_output(command)
            log.info("{}".format(gdalinfo_output))
            gdalinfo_outputs[input_path] = gdalinfo_output
        return gdalinfo_outputs
=======
    def __init__(self, resampling_method, max_overview_level, compress_overview = None, photometric_overview = None, interleave_overview = None, srcfile = None, xk_pull_dag_id=None, xk_pull_task_id=None, xk_pull_key_srcfile=None, xk_push_key=None, *args, **kwargs):
        self.resampling_method = resampling_method
        self.max_overview_level = max_overview_level
        self.compress_overview = ' --config COMPRESS_OVERVIEW ' + compress_overview +' ' if compress_overview else ' '
        self.photometric_overview = ' --config PHOTOMETRIC_OVERVIEW ' + photometric_overview +' ' if photometric_overview else ' '
        self.interleave_overview = ' --config INTERLEAVE_OVERVIEW ' + interleave_overview +' ' if interleave_overview else ' '
        self.srcfile = srcfile
        self.xk_pull_dag_id = xk_pull_dag_id
        self.xk_pull_task_id = xk_pull_task_id
        self.xk_pull_key_srcfile = xk_pull_key_srcfile
        self.xk_push_key = xk_push_key

        """
        levels = ''
        for i in range(1, int(math.log(max_overview_level, 2)) + 1):
            levels += str(2**i) + ' '
        self.levels = levels
        """
        level = 2
        levels = ''
        while(level <= int(self.max_overview_level)):
            level = level*2
            levels += str(level)
            levels += ' '
        self.levels = levels
        super(GDALAddoOperator, self).__init__(*args, **kwargs)
        super(GDALAddoOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        log.info('-------------------- GDAL_PLUGIN Addo ------------')
        task_instance = context['task_instance']

        log.info("""
            resampling_method: {}
            max_overview_level: {}
            compress_overview: {}
            photometric_overview: {}
            interleave_overview: {}
            srcfile: {}
            xk_pull_dag_id: {}
            xk_pull_task_id: {}
            xk_pull_key_srcfile: {}
            xk_push_key: {}
            """.format(
            self.resampling_method,
            self.max_overview_level,
            self.compress_overview,
            self.photometric_overview,
            self.interleave_overview,
            self.srcfile,
            self.xk_pull_dag_id,
            self.xk_pull_task_id,
            self.xk_pull_key_srcfile,
            self.xk_push_key
            )
        )

        # init XCom parameters
        if self.xk_pull_dag_id is None:
            self.xk_pull_dag_id = context['dag'].dag_id
        # check input file path passed otherwise look for it in XCom
        if self.srcfile is not None:
            srcfile = self.srcfile
        else:
            if self.xk_pull_key_srcfile is None:
                self.xk_pull_key_srcfile = 'srcdir'
            log.debug('Fetching srcdir from XCom')
            srcfile = task_instance.xcom_pull(task_ids=self.xk_pull_task_id, key=self.xk_pull_key_srcfile, dag_id=self.xk_pull_dag_id)
            if srcfile is None:
                log.warn('No srcdir fetched from XCom. Nothing to do')
                return False
        assert srcfile is not None
        log.info('srcfile: %s', srcfile)
        log.info('levels %s', self.levels)

        gdaladdo_command = 'gdaladdo -r ' + self.resampling_method + ' ' + self.compress_overview + self.photometric_overview+ self.interleave_overview + srcfile + ' ' + self.levels
        log.info('The complete GDAL addo command is: %s', gdaladdo_command)

        # push output path to XCom
        if self.xk_push_key is None:
            self.xk_push_key = 'dstfile'
        task_instance.xcom_push(key='dstfile', value=srcfile)

        # run the command
        bo = BashOperator(task_id='bash_operator_addo_', bash_command=gdaladdo_command)
        bo.execute(context)

class GDALTranslateOperator(BaseOperator):

    @apply_defaults
    def __init__(self, 
            tiled = None, 
            working_dir = None,
            blockx_size = None,
            blocky_size = None,
            compress = None,
            photometric = None,
            ot = None, of = None,
            b = None, mask = None,
            outsize = None,
            scale = None,
            *args, **kwargs):

        self.working_dir = working_dir
 
        self.tiled = ' -co "TILED=' + tiled +'" ' if tiled else ' '
        self.blockx_size = ' -co "BLOCKXSIZE=' + blockx_size + '" ' if blockx_size else ' '
        self.blocky_size = ' -co "BLOCKYSIZE=' + blocky_size + '" ' if blocky_size else ' '
        self.compress = ' -co "COMPRESS=' + compress + '"' if compress else ' '
        self.photometric = ' -co "PHOTOMETRIC=' + photometric + '" ' if photometric else ' '

        self.ot = ' -ot ' + str(ot) if ot else ''
        self.of = ' -of ' + str(of) if of else ''
        self.b = ' -b ' + str(b) if b else ''
        self.mask = '-mask ' + str(mask) if mask else ''
        self.outsize = '-outsize ' + str(outsize) if outsize else ''
        self.scale = ' -scale ' + str(scale) if scale else ''

        log.info('--------------------GDAL_PLUGIN Translate initiated------------')
        super(GDALTranslateOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        log.info('--------------------GDAL_PLUGIN Translate running------------')
        task_instance = context['task_instance']
        log.info("GDAL Translate Operator params list")
        log.info('Working dir: %s', self.working_dir)
        scene_fullpath = context['task_instance'].xcom_pull('landsat8_download_daraa_task', key='scene_fullpath')
        scenes = os.listdir(scene_fullpath)
        if not os.path.exists(scene_fullpath+"__translated"):
            create_translated_dir = BashOperator(task_id="bash_operator_translate", bash_command="mkdir {}".format(scene_fullpath+"__translated"))
            create_translated_dir.execute(context)
        for scene in scenes:
          if scene.endswith(".TIF"):
            output_img_filename = 'translated_' +str(scene)
            gdaltranslate_command = 'gdal_translate ' + self.ot + self.of + self.b + self.mask + self.outsize + self.scale + self.tiled + self.blockx_size + self.blocky_size + self.compress + self.photometric + os.path.join(scene_fullpath ,scene) +'  ' + os.path.join(scene_fullpath+"__translated" , output_img_filename)
            log.info('The complete GDAL translate command is: %s', gdaltranslate_command)
            b_o = BashOperator(task_id="bash_operator_translate_scenes", bash_command = gdaltranslate_command)
            b_o.execute(context)
        task_instance.xcom_push(key = 'translated_scenes_dir', value = os.path.join(scene_fullpath+"__translated",output_img_filename))
        task_instance.xcom_push(key = 'product_dir', value = scene_fullpath)
        inner_tiffs = os.path.join(scene_fullpath,"*.TIF")
        #rm_cmd = "rm -r {}".format(inner_tiffs)
        #delete_b_o = BashOperator(task_id="bash_operator_delete_scenes", bash_command = rm_cmd)
        #delete_b_o.execute(context)
        return True
>>>>>>> a7778964d27e7c75cd0b3cb6bbac80a1792c7938


class GDALPlugin(AirflowPlugin):
    name = "GDAL_plugin"
<<<<<<< HEAD
    operators = [
        GDALWarpOperator,
        GDALAddoOperator,
        GDALTranslateOperator,
        GDALInfoOperator
    ]


def _get_gdal_creation_options(**creation_options):
    result = ""
    for name, value in creation_options.items():
        opt = '-co "{}={}"'.format(name.upper(), value)
        " ".join((result, opt))
    return result

=======
    operators = [GDALWarpOperator, GDALAddoOperator, GDALTranslateOperator]
>>>>>>> a7778964d27e7c75cd0b3cb6bbac80a1792c7938

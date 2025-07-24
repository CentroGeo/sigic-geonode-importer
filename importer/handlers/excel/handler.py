import logging
import os
import re
import tempfile
import unicodedata
import atexit

import pandas as pd
from openpyxl import Workbook

from geonode.resource.enumerator import ExecutionRequestAction as exa
from geonode.upload.api.exceptions import UploadParallelismLimitException
from geonode.upload.utils import UploadLimitValidator
from importer.celery_tasks import create_dynamic_structure
from importer.handlers.excel.exceptions import InvalidExcelException
from osgeo import ogr
from celery import group
from geonode.base.models import ResourceBase
from dynamic_models.models import ModelSchema
from importer.handlers.common.vector import BaseVectorFileHandler
from importer.handlers.utils import GEOM_TYPE_MAPPING
from importer.utils import ImporterRequestAction as ira

logger = logging.getLogger(__name__)


def sanitize_name(name: str, max_length: int = 64) -> str:
    name = unicodedata.normalize('NFKD', name).encode('ascii', 'ignore').decode()
    name = re.sub(r'[^a-zA-Z0-9_]+', '_', name)
    name = name.strip('_').lower()
    return name[:max_length]


class XLSXFileHandler(BaseVectorFileHandler):
    """
    Handler para archivos .xlsx y .xls, con soporte multihoja, limpieza de nombres y conversión.
    """

    ACTIONS = {
        exa.IMPORT.value: (
            "start_import",
            "importer.import_resource",
            "importer.publish_resource",
            "importer.create_geonode_resource",
        ),
        exa.COPY.value: (
            "start_copy",
            "importer.copy_dynamic_model",
            "importer.copy_geonode_data_table",
            "importer.publish_resource",
            "importer.copy_geonode_resource",
        ),
        ira.ROLLBACK.value: (
            "start_rollback",
            "importer.rollback",
        ),
    }

    @property
    def supported_file_extension_config(self):
        return {
            "id": "xlsx",
            "label": "Excel",
            "format": "vector",
            "mimeType": [
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                "application/vnd.ms-excel",
            ],
            "ext": ["xlsx", "xls"],
            "optional": ["sld", "xml"],
        }

    possible_geometry_column_name = ["geom", "geometry", "wkt_geom", "the_geom"]
    possible_lat_column = ["latitude", "lat", "y"]
    possible_long_column = ["longitude", "long", "x"]
    possible_latlong_column = possible_lat_column + possible_long_column

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._temp_xlsx_file = None
        atexit.register(self._cleanup_temp_file)

    def _cleanup_temp_file(self):
        if self._temp_xlsx_file and os.path.exists(self._temp_xlsx_file):
            try:
                os.remove(self._temp_xlsx_file)
                logger.info(f"Temporary XLSX file removed: {self._temp_xlsx_file}")
            except Exception as e:
                logger.warning(f"Could not remove temporary file: {e}")
            self._temp_xlsx_file = None

    def convert_xls_to_xlsx(self, xls_path: str) -> str:
        df = pd.read_excel(xls_path)
        temp = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
        df.to_excel(temp.name, index=False, engine="openpyxl")
        self._temp_xlsx_file = temp.name
        logger.info(f"Converted XLS to XLSX: {temp.name}")
        return temp.name

    def get_effective_file(self, files: dict) -> str:
        base_file = files.get("base_file")
        if not base_file:
            raise InvalidExcelException("No base_file provided")
        filename = base_file if isinstance(base_file, str) else base_file.name

        if filename.lower().endswith(".xls"):
            converted_path = self.convert_xls_to_xlsx(base_file)
            converted_file = open(converted_path, "rb")
            converted_file.name = converted_path
            converted_file.size = os.path.getsize(converted_path)
            files["base_file"] = converted_file
            return converted_path

        elif filename.lower().endswith(".xlsx"):
            if isinstance(base_file, str):
                size = os.path.getsize(base_file)
                f = open(base_file, "rb")
                f.name = base_file
                f.size = size
                files["base_file"] = f
                return base_file
            else:
                # Asegurar que base_file.size esté definido
                if not hasattr(base_file, "size") or base_file.size is None:
                    try:
                        file_path = getattr(base_file, "file", {}).name or base_file.name
                        base_file.size = os.path.getsize(file_path)
                    except Exception as e:
                        logger.warning(f"No se pudo determinar el tamaño del archivo: {e}")
                        base_file.size = 0  # valor seguro para evitar NoneType

                files["base_file"] = base_file
                return base_file.name

        raise InvalidExcelException("Unsupported file format. Only .xls and .xlsx are allowed.")

    def get_ogr2ogr_driver(self):
        return ogr.GetDriverByName("XLSX")

    def create_ogr2ogr_command(self, files, original_name, overwrite_layer, alternate):
        base_command = BaseVectorFileHandler.create_ogr2ogr_command(
            files, original_name, overwrite_layer, alternate
        )
        additional_option = (
            ' -oo "GEOM_POSSIBLE_NAMES=geom*,the_geom*,wkt_geom"'
            ' -oo "X_POSSIBLE_NAMES=x,long*"'
            ' -oo "Y_POSSIBLE_NAMES=y,lat*"'
        )
        return (
            f"{base_command} -oo KEEP_GEOM_COLUMNS=NO -lco GEOMETRY_NAME={self.default_geometry_column_name} "
            + additional_option
        )

    def is_valid(self, files, user):
        BaseVectorFileHandler.is_valid(files, user)
        upload_validator = UploadLimitValidator(user)
        upload_validator.validate_parallelism_limit_per_user()
        actual_upload = upload_validator._get_parallel_uploads_count()
        max_upload = upload_validator._get_max_parallel_uploads()

        effective_file = self.get_effective_file(files)

        try:
            layers = self.get_ogr2ogr_driver().Open(effective_file)
            if not layers:
                raise InvalidExcelException("Invalid Excel file")

            layers_count = len(layers)
            if layers_count >= max_upload:
                raise UploadParallelismLimitException(
                    detail=f"Too many layers ({layers_count}). Max allowed: {max_upload}"
                )
            elif layers_count + actual_upload >= max_upload:
                raise UploadParallelismLimitException(
                    detail=f"Upload would exceed parallel limit ({max_upload})"
                )

            schema_keys = [x.name.lower() for layer in layers for x in layer.schema]
            geom_is_in_schema = any(x in schema_keys for x in self.possible_geometry_column_name)
            has_lat = any(x in self.possible_lat_column for x in schema_keys)
            has_long = any(x in self.possible_long_column for x in schema_keys)

        finally:
            self._cleanup_temp_file()

        return True

    @staticmethod
    def can_handle(_data) -> bool:
        base = _data.get("base_file")
        if not base:
            return False
        filename = base if isinstance(base, str) else base.name
        return filename.lower().endswith((".xlsx", ".xls"))

    def get_base_filename(self, files: dict) -> str:
        base_file = files.get("base_file")
        if not base_file:
            return "archivo"

        filename = base_file if isinstance(base_file, str) else getattr(base_file, "name", "archivo")
        filename = os.path.basename(filename)
        filename = os.path.splitext(filename)[0]
        return sanitize_name(filename)

    def extract_resource_to_publish(self, files, action, layer_name, alternate, **kwargs):
        effective_file = self.get_effective_file(files)

        if action == exa.COPY.value:
            return [{
                "name": alternate,
                "crs": ResourceBase.objects.filter(alternate__istartswith=layer_name).first().srid,
            }]

        layers = self.get_ogr2ogr_driver().Open(effective_file, 0)
        if not layers:
            return []

        base_name = self.get_base_filename(files)

        if len(layers) == 1:
            return [{
                "name": base_name[:64],
                "crs": self.identify_authority(layers[0]),
            }]

        resources = []
        for layer in layers:
            hoja_clean = sanitize_name(layer.GetName())
            combined = f"{base_name}_{hoja_clean}"
            name = sanitize_name(combined, max_length=64)
            crs = self.identify_authority(layer)
            resources.append({
                "name": name,
                "crs": crs,
            })

        return resources

    create_dynamic_model_fields = BaseVectorFileHandler.create_dynamic_model_fields
    identify_authority = BaseVectorFileHandler.identify_authority

import logging
import re
import unicodedata

from osgeo import ogr
from celery import group

from geonode.resource.enumerator import ExecutionRequestAction as exa
from geonode.upload.api.exceptions import UploadParallelismLimitException
from geonode.upload.utils import UploadLimitValidator
from geonode.base.models import ResourceBase
from dynamic_models.models import ModelSchema

from importer.celery_tasks import create_dynamic_structure
from importer.handlers.common.vector import BaseVectorFileHandler
from importer.handlers.utils import GEOM_TYPE_MAPPING
from importer.utils import ImporterRequestAction as ira

logger = logging.getLogger(__name__)


def sanitize_name(name: str, max_length: int = 64) -> str:
    name = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode()
    name = re.sub(r"[^a-zA-Z0-9_]+", "_", name)
    name = name.strip("_").lower()
    return name[:max_length]


class XLSXFileHandler(BaseVectorFileHandler):
    """
    Handler to import .xlsx files as vector datasets into GeoNode
    """

    EXTENSIONS = [".xlsx"]

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
            "label": "Excel (XLSX)",
            "format": "vector",
            "mimetype": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "ext": ["xlsx"],
            "optional": ["sld", "xml"],
        }

    @staticmethod
    def is_valid(files, user):
        """
        Validate if this handler supports the uploaded file
        """
        if not files or "base_file" not in files:
            return False

        file_path = files["base_file"]
        try:
            BaseVectorFileHandler.is_valid(files, user)

            upload_validator = UploadLimitValidator(user)
            upload_validator.validate_parallelism_limit_per_user()

            actual_upload = upload_validator._get_parallel_uploads_count()
            max_upload = upload_validator._get_max_parallel_uploads()

            driver = XLSXFileHandler().get_ogr2ogr_driver()
            ds = driver.Open(file_path)

            if not ds:
                logger.warning("[XLSX] OGR failed to open file")
                return False

            layers_count = ds.GetLayerCount()
            if layers_count == 0:
                logger.warning("[XLSX] No layers in file")
                return False

            if layers_count >= max_upload:
                raise UploadParallelismLimitException(
                    detail=f"The number of sheets in the Excel file ({layers_count}) exceeds the upload limit ({max_upload})"
                )
            elif layers_count + actual_upload >= max_upload:
                raise UploadParallelismLimitException(
                    detail=f"Uploading this file would exceed your max parallel upload limit ({max_upload})"
                )

            return True
        except Exception as e:
            logger.exception("[XLSX] Validation error")
            return False

    def get_ogr2ogr_driver(self):
        return ogr.GetDriverByName("XLSX")

    def create_tasks(self):
        """
        Creates one task per sheet (layer) in the XLSX file
        """
        base_file = self.files.get("base_file")
        ds = self.get_ogr2ogr_driver().Open(base_file)
        if not ds:
            raise Exception("Could not open XLSX file with OGR")

        layer_count = ds.GetLayerCount()
        if layer_count == 0:
            raise Exception("No sheets found in the XLSX file")

        UploadLimitValidator(self.user).validate_files_sum_of_sizes(self.storage_manager.data_retriever)
        UploadLimitValidator(self.user).validate_parallelism(layer_count)

        layer_names = [sanitize_name(ds.GetLayer(i).GetName()) for i in range(layer_count)]
        self.layer_names = layer_names

        tasks = group(
            create_dynamic_structure.s(self.job.id, name, self.files, self.user.id, self.subtask_status_callback())
            for name in layer_names
        )
        return tasks

    def create_dynamic_model_fields(self, layer, name=None):
        """
        Tries to infer geometry from columns if not detected natively
        """
        geom_type = None
        lat_field = lon_field = None

        layer_defn = layer.GetLayerDefn()
        field_names = [layer_defn.GetFieldDefn(i).GetNameRef().lower() for i in range(layer_defn.GetFieldCount())]

        for lat_candidate in ["lat", "latitude", "y"]:
            if lat_candidate in field_names:
                lat_field = lat_candidate
                break

        for lon_candidate in ["lon", "long", "longitude", "x"]:
            if lon_candidate in field_names:
                lon_field = lon_candidate
                break

        if lat_field and lon_field:
            geom_type = "POINT"

        model_fields = []
        for i in range(layer_defn.GetFieldCount()):
            field_defn = layer_defn.GetFieldDefn(i)
            name = field_defn.GetNameRef()
            field_type = field_defn.GetType()
            model_fields.append((name, field_type))

        return {
            "geom_type": geom_type,
            "lat_field": lat_field,
            "lon_field": lon_field,
            "model_fields": model_fields,
        }

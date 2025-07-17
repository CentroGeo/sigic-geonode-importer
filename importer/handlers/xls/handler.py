from importer.handlers.common.vector import BaseVectorFileHandler
from geonode.resource.enumerator import ExecutionRequestAction as exa
from importer.utils import ImporterRequestAction as ira
import logging
from osgeo import ogr

logger = logging.getLogger(__name__)

class XLSFileHandler(BaseVectorFileHandler):
    ACTIONS = {
        exa.IMPORT.value: (
            "start_import",
            "importer.import_resource",
            "importer.publish_resource",
            "importer.create_geonode_resource",
        ),
        ira.ROLLBACK.value: (
            "start_rollback",
            "importer.rollback",
        ),
    }

    @property
    def supported_file_extension_config(self):
        return {
            "id": "xls",
            "label": "XLS",
            "format": "vector",
            "mimeType": ["application/vnd.ms-excel"],
            "ext": ["xls"],
        }

    @staticmethod
    def can_handle(_data) -> bool:
        base = _data.get("base_file")
        if not base:
            return False
        return base.lower().endswith(".xls") if isinstance(base, str) else base.name.lower().endswith(".xls")

    def get_ogr2ogr_driver(self):
        return ogr.GetDriverByName("XLS")

import logging
from osgeo import ogr
from geonode.resource.enumerator import ExecutionRequestAction as exa
from importer.utils import ImporterRequestAction as ira
from importer.handlers.common.vector import BaseVectorFileHandler

logger = logging.getLogger(__name__)


class XLSFileHandler(BaseVectorFileHandler):
    """
    Handler para importar archivos .xls, con o sin geometría.
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
            "id": "xls",
            "label": "XLS",
            "format": "vector",
            "mimeType": ["application/vnd.ms-excel"],
            "ext": ["xls"],
            "optional": ["sld", "xml"],
        }

    @staticmethod
    def can_handle(_data) -> bool:
        logger.warning("[XLS handler] can_handle fue llamado")
        base = _data.get("base_file")

        if not base:
            return False

        return (
            base.lower().endswith(".xls")
            if isinstance(base, str)
            else base.name.lower().endswith(".xls")
        )

    # def get_ogr2ogr_driver(self):
    #     logger.warning("[XLS handler] get_ogr2ogr_driver fue llamado")
    #     return ogr.GetDriverByName("XLS")

    # @staticmethod
    # def is_valid(files, user):
    #     logger.warning("[XLS handler] is_valid fue llamado")

    #     base_file = files.get("base_file")
    #     file_size = getattr(base_file, "size", None)
    #     logger.warning(f"[XLS handler] Tamaño de archivo: {file_size}")

    #     if file_size is None:
    #         raise Exception("❌ No se pudo determinar el tamaño del archivo.")

    #     # Llama validaciones del padre
    #     BaseVectorFileHandler.is_valid(files, user)

    #     # Asegura que OGR pueda abrir el archivo
    #     dataset = ogr.Open(base_file.temporary_file_path())
    #     if not dataset:
    #         raise Exception("❌ OGR no pudo abrir el archivo .xls")

    #     return True
    
    @staticmethod
    def is_valid(files, user):
        logger.warning("[XLS handler] is_valid fue llamado")

        base_file = files.get("base_file")
        if base_file is None:
            logger.warning("[XLS handler] ⚠️ base_file es None")
            raise Exception("base_file no encontrado")

        file_size = getattr(base_file, "size", None)
        logger.warning(f"[XLS handler] Tamaño de archivo: {file_size}")

        if file_size is None:
            raise Exception("❌ No se pudo determinar el tamaño del archivo.")

        # ⚠️ Llama validaciones del padre — esto puede lanzar una excepción silenciosa
        try:
            BaseVectorFileHandler.is_valid(files, user)
        except Exception as e:
            logger.warning(f"[XLS handler] ❌ Error en BaseVectorFileHandler.is_valid: {e}")
            raise

        # Asegura que OGR pueda abrir el archivo
        try:
            dataset = ogr.Open(base_file.temporary_file_path())
            if not dataset:
                raise Exception("❌ OGR no pudo abrir el archivo .xls")
        except Exception as e:
            logger.warning(f"[XLS handler] ❌ Error al abrir con OGR: {e}")
            raise

        logger.warning("[XLS handler] ✅ Validación pasada")
        return True


    # def create_ogr2ogr_command(self, files, original_name, ovverwrite_layer, alternate):
    #     logger.warning("[XLS handler] create_ogr2ogr_command fue llamado")
    #     return BaseVectorFileHandler.create_ogr2ogr_command(
    #         files, original_name, ovverwrite_layer, alternate
    #     )





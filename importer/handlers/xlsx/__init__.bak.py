import logging

from .handlers.xlsx.handler import XLSXFileHandler

logger = logging.getLogger(__name__)


def patch_uploaded_file_size():
    """
    Parchea _get_uploaded_files_total_size para evitar errores si algún archivo tiene .size == None.
    Compatible con GeoNode 4.4+.
    """
    try:
        from geonode.upload import utils

        def patched_get_uploaded_files_total_size(self, file_dict):
            excluded_files = ("zip_file", "shp_file")
            _iterate_files = file_dict.data_items if hasattr(file_dict, "data_items") else file_dict
            uploaded_files_sizes = []

            for field_name, file_obj in _iterate_files.items():
                if field_name in excluded_files:
                    continue
                size = getattr(file_obj, "size", None)
                if size is None:
                    try:
                        if hasattr(file_obj, "seek") and hasattr(file_obj, "tell"):
                            pos = file_obj.tell()
                            file_obj.seek(0, 2)
                            size = file_obj.tell()
                            file_obj.seek(pos)
                            if hasattr(file_obj, "__dict__"):
                                file_obj.size = size
                        else:
                            size = 0
                    except Exception as e:
                        logger.warning(f"⚠️ No se pudo calcular size para '{field_name}': {e}")
                        size = 0
                uploaded_files_sizes.append(size)

            total_size = sum(uploaded_files_sizes)
            return total_size

        utils.UploadLimitValidator._get_uploaded_files_total_size = patched_get_uploaded_files_total_size
        logger.info("✅ Patch aplicado correctamente a `_get_uploaded_files_total_size`")

    except Exception as e:
        logger.error(f"❌ Falló el parche a `_get_uploaded_files_total_size`: {e}")


patch_uploaded_file_size()

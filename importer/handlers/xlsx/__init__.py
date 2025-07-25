import atexit
import logging

from importer.handlers.xlsx.handler import XLSXFileHandler

logger = logging.getLogger(__name__)


def patch_uploaded_file_size():
    """
    Parchea UploadLimitValidator._get_uploaded_files_total_size para evitar errores
    si algún archivo tiene size == None.
    """
    try:
        from geonode.upload import utils
        original_method = utils.UploadLimitValidator._get_uploaded_files_total_size

        def patched_get_uploaded_files_total_size(self, file_dict):
            fixed_sizes = []
            for f in file_dict.values():
                size = getattr(f, "size", None)
                if size is None:
                    if hasattr(f, "seek") and hasattr(f, "tell"):
                        try:
                            pos = f.tell()
                            f.seek(0, 2)
                            size = f.tell()
                            f.seek(pos)
                            if hasattr(f, "__dict__"):
                                f.size = size
                        except Exception as e:
                            logger.warning(f"No se pudo calcular size del archivo: {e}")
                            size = 0
                    else:
                        size = 0
                fixed_sizes.append(size)
            return sum(fixed_sizes)

        utils.UploadLimitValidator._get_uploaded_files_total_size = patched_get_uploaded_files_total_size
        logger.info("✅ Patch de `_get_uploaded_files_total_size` aplicado con éxito.")

    except Exception as e:
        logger.error(f"❌ Falló el parcheo de validación de tamaños: {e}")


# Ejecutar parche al cargar módulo
patch_uploaded_file_size()

# Registrar el handler
HANDLERS = [XLSXFileHandler]

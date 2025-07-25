import logging

from importer.handlers.xlsx.handler import XLSXFileHandler

logger = logging.getLogger(__name__)


def patch_uploaded_file_size():
    """
    Parchea UploadLimitValidator._get_uploaded_files_total_size para que funcione
    con DataRetriever (no es un dict), sin causar errores si `.size` es None.
    """
    try:
        from geonode.upload import utils

        original_method = utils.UploadLimitValidator._get_uploaded_files_total_size

        def patched_get_uploaded_files_total_size(self, data_retriever):
            file_ids = data_retriever.get_file_list()
            fixed_sizes = []

            for fid in file_ids:
                f = data_retriever.get_file(fid)
                size = getattr(f, "size", None)

                if size is None:
                    try:
                        if hasattr(f, "seek") and hasattr(f, "tell"):
                            pos = f.tell()
                            f.seek(0, 2)
                            size = f.tell()
                            f.seek(pos)
                            if hasattr(f, "__dict__"):
                                f.size = size
                        else:
                            size = 0
                    except Exception as e:
                        logger.warning(f"⚠️ No se pudo determinar el size del archivo {fid}: {e}")
                        size = 0

                fixed_sizes.append(size)

            return sum(fixed_sizes)

        utils.UploadLimitValidator._get_uploaded_files_total_size = patched_get_uploaded_files_total_size
        logger.info("✅ Patch aplicado: manejo seguro de tamaño de archivos para DataRetriever.")

    except Exception as e:
        logger.error(f"❌ Falló el parche de _get_uploaded_files_total_size: {e}")


patch_uploaded_file_size()

# No necesitas declarar HANDLERS si usas SYSTEM_HANDLERS

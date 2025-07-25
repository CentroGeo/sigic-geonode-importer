import logging

from importer.handlers.xlsx.handler import XLSXFileHandler

logger = logging.getLogger(__name__)


def patch_uploaded_file_size():
    """
    Parchea UploadLimitValidator._get_uploaded_files_total_size para soportar DataRetriever
    incluso si no tiene métodos como `get_file_list()`, usando su protocolo de diccionario.
    """
    try:
        from geonode.upload import utils

        def patched_get_uploaded_files_total_size(self, data_retriever):
            fixed_sizes = []

            try:
                for file_id in data_retriever:
                    f = data_retriever[file_id]
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
                            logger.warning(f"⚠️ No se pudo determinar el size del archivo {file_id}: {e}")
                            size = 0

                    fixed_sizes.append(size)

            except Exception as e:
                logger.error(f"❌ Error al iterar sobre data_retriever: {e}")
                return 0

            return sum(fixed_sizes)

        utils.UploadLimitValidator._get_uploaded_files_total_size = patched_get_uploaded_files_total_size
        logger.info("✅ Patch aplicado: validación robusta de tamaño de archivos.")

    except Exception as e:
        logger.error(f"❌ Falló el parche de validación de tamaño: {e}")


patch_uploaded_file_size()

# El handler ya está registrado en SYSTEM_HANDLERS

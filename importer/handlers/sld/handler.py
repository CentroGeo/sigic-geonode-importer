import logging
import os

from geonode.resource.manager import resource_manager
from importer.handlers.common.metadata import MetadataFileHandler
from importer.handlers.sld.exceptions import InvalidSldException
from owslib.etree import etree as dlxml

logger = logging.getLogger(__name__)


class SLDFileHandler(MetadataFileHandler):
    """
    Handler to import SLD files into GeoNode data db
    It must provide the task_lists required to comple the upload
    """
    @property
    def supported_file_extension_config(self):
        return {
            "id": "sld",
            "label": "Styled Layer Descriptor (SLD)",
            "format": "metadata",
            "ext": ["sld"],
            "mimeType": ["application/json"],
            "needsFiles": [
                "shp",
                "prj",
                "dbf",
                "shx",
                "csv",
                "tiff",
                "zip",
                "xml",
                "geojson",
            ],
        }

    @staticmethod
    def can_handle(_data) -> bool:
        """
        This endpoint will return True or False if with the info provided
        the handler is able to handle the file or not
        """
        base = _data.get("base_file")
        if not base:
            return False
        return (
            base.endswith(".sld")
            if isinstance(base, str)
            else base.name.endswith(".sld")
        )

    @staticmethod
    def is_valid(files, user):
        """
        Define basic validation steps
        """
        # calling base validation checks

        try:
            with open(files.get("base_file")) as _xml:
                dlxml.fromstring(_xml.read().encode())
        except Exception as err:
            raise InvalidSldException(
                f"Uploaded document is not SLD or is invalid: {str(err)}"
            )
        return True

    def handle_metadata_resource(self, _exec, dataset, original_handler):
        # Usamos siempre nuestro flujo import_resource
        self.import_resource(_exec, dataset)
    
    def import_resource(self, _exec, dataset):
        """
        Publishes the SLD in GeoServer and, if no default, assigns it.
        """
        files = _exec.input_params.get("files", {})
        sld_path = files.get("sld_file") or _exec.input_params.get("base_file")
        style_name = os.path.splitext(os.path.basename(sld_path))[0]

        # 1) Publish the SLD as an additional style (does not change default)
        resource_manager.exec(
            "publish_sld", None,
            instance=dataset,
            sld_file=sld_path,
            sld_uploaded=bool(sld_path),
            vals={"dirty_state": True},
        )

        # 2) If there's no default style yet, assign the new one
        if not getattr(dataset.styles, "default", None):
            resource_manager.exec(
                "set_style", None,
                instance=dataset,
                style=style_name,
                vals={"dirty_state": True},
            )

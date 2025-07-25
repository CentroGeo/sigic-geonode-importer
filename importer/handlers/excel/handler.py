import logging
import os
import re
import tempfile
import unicodedata

import pandas as pd
from openpyxl import load_workbook
from openpyxl import Workbook
from xlrd import open_workbook
from django.core.files.uploadedfile import InMemoryUploadedFile
import io

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


class XLSXFileHandler(BaseVectorFileHandler):
    """
    Handler to import XLSX (or XLS converted) files into GeoNode
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
        }

    @staticmethod
    def can_handle(_data) -> bool:
        base = _data.get("base_file")
        if not base:
            return False
        return (
            base.lower().endswith((".xls", ".xlsx"))
            if isinstance(base, str)
            else base.name.lower().endswith((".xls", ".xlsx"))
        )

    @staticmethod
    def is_valid(files, user):
        # Validación base
        BaseVectorFileHandler.is_valid(files, user)
        upload_validator = UploadLimitValidator(user)
        upload_validator.validate_parallelism_limit_per_user()

        file_obj = files.get("base_file")
        if not file_obj:
            raise InvalidExcelException("No file provided")

        filename = file_obj.name.lower()

        # Conversión de .xls a .xlsx
        if filename.endswith(".xls") and not filename.endswith(".xlsx"):
            book = open_workbook(file_contents=file_obj.read())
            temp = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
            workbook = Workbook()
            sheet = workbook.active
            old_sheet = book.sheet_by_index(0)

            for row_idx in range(old_sheet.nrows):
                row = [old_sheet.cell_value(row_idx, col) for col in range(old_sheet.ncols)]
                sheet.append(row)

            workbook.save(temp.name)
            temp.flush()

            with open(temp.name, "rb") as converted:
                content = converted.read()
                file_io = io.BytesIO(content)
                file_io.name = temp.name
                size = len(content)
                uploaded_file = InMemoryUploadedFile(
                    file_io,
                    field_name=None,
                    name=os.path.basename(temp.name),
                    content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    size=size,
                    charset=None,
                )
                uploaded_file.size = size  # Evita el NoneType
                files["base_file"] = uploaded_file

        # Validación del archivo convertido
        try:
            pd.read_excel(files.get("base_file"), sheet_name=None)
        except Exception as e:
            raise InvalidExcelException(f"Error parsing Excel file: {str(e)}")

        return True

    def get_ogr2ogr_driver(self):
        return ogr.GetDriverByName("XLSX")

    @staticmethod
    def create_ogr2ogr_command(files, original_name, ovverwrite_layer, alternate):
        base_command = BaseVectorFileHandler.create_ogr2ogr_command(
            files, original_name, ovverwrite_layer, alternate
        )
        return f"{base_command} -lco GEOMETRY_NAME={BaseVectorFileHandler().default_geometry_column_name}"

    def create_dynamic_model_fields(
        self,
        layer: str,
        dynamic_model_schema: ModelSchema,
        overwrite: bool,
        execution_id: str,
        layer_name: str,
    ):
        layer_schema = [
            {"name": x.name.lower(), "class_name": self._get_type(x), "null": True}
            for x in layer.schema
        ]

        if (
            layer.GetGeometryColumn()
            or self.default_geometry_column_name
            and ogr.GeometryTypeToName(layer.GetGeomType())
            not in ["Geometry Collection", "Unknown (any)"]
        ):
            schema_keys = [x["name"] for x in layer_schema]
            geom_is_in_schema = (
                x in schema_keys for x in self.possible_geometry_column_name
            )
            if (
                any(geom_is_in_schema) and layer.GetGeomType() == 100
            ):
                field_name = [
                    x for x in self.possible_geometry_column_name if x in schema_keys
                ][0]
                index = layer.GetFeature(1).keys().index(field_name)
                geom = [x for x in layer.GetFeature(1)][index]
                class_name = GEOM_TYPE_MAPPING.get(
                    self.promote_to_multi(geom.split("(")[0].replace(" ", "").title())
                )
                layer_schema = [x for x in layer_schema if field_name not in x["name"]]
            elif any(x in self.possible_latlong_column for x in schema_keys):
                class_name = GEOM_TYPE_MAPPING.get(self.promote_to_multi("Point"))
            else:
                class_name = GEOM_TYPE_MAPPING.get(
                    self.promote_to_multi(ogr.GeometryTypeToName(layer.GetGeomType()))
                )

            layer_schema += [
                {
                    "name": layer.GetGeometryColumn()
                    or self.default_geometry_column_name,
                    "class_name": class_name,
                    "dim": (
                        2
                        if not ogr.GeometryTypeToName(layer.GetGeomType())
                        .lower()
                        .startswith("3d")
                        else 3
                    ),
                }
            ]

        list_chunked = [
            layer_schema[i : i + 30] for i in range(0, len(layer_schema), 30)
        ]

        celery_group = group(
            create_dynamic_structure.s(
                execution_id, schema, dynamic_model_schema.id, overwrite, layer_name
            )
            for schema in list_chunked
        )

        return dynamic_model_schema, celery_group

    def extract_resource_to_publish(
        self, files, action, layer_name, alternate, **kwargs
    ):
        if action == exa.COPY.value:
            return [
                {
                    "name": alternate,
                    "crs": ResourceBase.objects.filter(
                        alternate__istartswith=layer_name
                    )
                    .first()
                    .srid,
                }
            ]

        layers = self.get_ogr2ogr_driver().Open(files.get("base_file"), 0)
        if not layers:
            return []
        return [
            {
                "name": alternate or layer_name,
                "crs": (self.identify_authority(_l)),
            }
            for _l in layers
            if self.fixup_name(_l.GetName()) == layer_name
        ]

    def identify_authority(self, layer):
        try:
            authority_code = super().identify_authority(layer=layer)
            return authority_code
        except Exception:
            return "EPSG:4326"

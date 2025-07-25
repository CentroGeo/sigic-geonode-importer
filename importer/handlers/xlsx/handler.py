import logging

from geonode.resource.enumerator import ExecutionRequestAction as exa
from geonode.upload.api.exceptions import UploadParallelismLimitException
from geonode.upload.utils import UploadLimitValidator
from importer.celery_tasks import create_dynamic_structure
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
    Handler to import XLSX files into GeoNode data db
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

    possible_geometry_column_name = ["geom", "geometry", "wkt_geom", "the_geom"]
    possible_lat_column = ["latitude", "lat", "y"]
    possible_long_column = ["longitude", "long", "x"]
    possible_latlong_column = possible_lat_column + possible_long_column

    @property
    def supported_file_extension_config(self):
        return {
            "id": "xlsx",
            "label": "XLSX",
            "format": "vector",
            "mimeType": ["application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"],
            "ext": ["xlsx"],
            "optional": [],
        }

    @staticmethod
    def can_handle(_data) -> bool:
        base = _data.get("base_file")
        if not base:
            return False
        return (
            base.lower().endswith(".xlsx")
            if isinstance(base, str)
            else base.name.lower().endswith(".xlsx")
        )

    @staticmethod
    def is_valid(files, user):
        # Forzar cálculo de tamaño si es necesario
        base_file = files.get("base_file")
        if hasattr(base_file, "seek") and hasattr(base_file, "tell"):
            current = base_file.tell()
            base_file.seek(0, 2)
            _ = base_file.tell()
            base_file.seek(current)

        BaseVectorFileHandler.is_valid(files, user)
        upload_validator = UploadLimitValidator(user)
        upload_validator.validate_parallelism_limit_per_user()
        actual_upload = upload_validator._get_parallel_uploads_count()
        max_upload = upload_validator._get_max_parallel_uploads()

        layers = XLSXFileHandler().get_ogr2ogr_driver().Open(files.get("base_file"))

        if not layers:
            raise Exception("The XLSX provided is invalid, no layers found")

        layers_count = len(layers)

        if layers_count >= max_upload:
            raise UploadParallelismLimitException(
                detail=f"The number of layers in the XLSX {layers_count} is greater than "
                f"the max parallel upload permitted: {max_upload} "
                f"please upload a smaller file"
            )
        elif layers_count + actual_upload >= max_upload:
            raise UploadParallelismLimitException(
                detail=f"With the provided XLSX, the number of max parallel upload will exceed the limit of {max_upload}"
            )

        schema_keys = [x.name.lower() for layer in layers for x in layer.schema]
        geom_is_in_schema = any(
            x in schema_keys for x in XLSXFileHandler().possible_geometry_column_name
        )
        has_lat = any(x in XLSXFileHandler().possible_lat_column for x in schema_keys)
        has_long = any(x in XLSXFileHandler().possible_long_column for x in schema_keys)

        return True

    def get_ogr2ogr_driver(self):
        return ogr.GetDriverByName("XLSX")

    @staticmethod
    def create_ogr2ogr_command(files, original_name, ovverwrite_layer, alternate):
        base_command = BaseVectorFileHandler.create_ogr2ogr_command(
            files, original_name, ovverwrite_layer, alternate
        )
        return (
            f"{base_command} -lco GEOMETRY_NAME={BaseVectorFileHandler().default_geometry_column_name}"
        )

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
            if any(geom_is_in_schema) and layer.GetGeomType() == 100:
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
                "crs": self.identify_authority(_l),
            }
            for _l in layers
            if self.fixup_name(_l.GetName()) == layer_name
        ]

    def identify_authority(self, layer):
        try:
            return super().identify_authority(layer=layer)
        except Exception:
            return "EPSG:4326"

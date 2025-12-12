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
from importer.handlers.utils import GEOM_TYPE_MAPPING, normalize_field_name
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
            "optional": ["sld", "xml"],
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

        logger.debug("Validando archivo XLSX")
        logger.debug(f"files: {files}")
        logger.debug(f"user: {user}")
        logger.debug(f"user.username: {getattr(user, 'username', None)}")
        logger.debug(f"base_file: {files.get('base_file')}")

        # --- PARCHE: forzar que el archivo tenga .size válido ---
        from shutil import copyfileobj
        base_file = files.get("base_file")
        if base_file and getattr(base_file, "size", None) in [None, 0]:
            try:
                if hasattr(base_file, "seek") and hasattr(base_file, "tell"):
                    current_pos = base_file.tell()
                    base_file.seek(0, 2)  # Final
                    file_size = base_file.tell()
                    base_file.seek(current_pos)  # Restaurar posición
                    # Forzar seteo de .size si no existe (algunos tipos lo permiten)
                    if hasattr(base_file, "__dict__"):
                        base_file.size = file_size
            except Exception as e:
                logger.warning(f"Could not force size on uploaded file: {e}")
        # --- FIN DEL PARCHE ---

        BaseVectorFileHandler.is_valid(files, user)
        upload_validator = UploadLimitValidator(user)
        upload_validator.validate_parallelism_limit_per_user()
        actual_upload = upload_validator._get_parallel_uploads_count()
        max_upload = upload_validator._get_max_parallel_uploads()

        logger.info("files", files)
        logger.info("files.get(base_file)", files.get("base_file"))

        # layers = XLSXFileHandler().get_ogr2ogr_driver().Open(files.get("base_file"))

        ds = XLSXFileHandler().get_ogr2ogr_driver().Open(files.get("base_file"))
        if ds is None:
            raise Exception("The XLSX provided is invalid or unreadable", files.get("base_file"), files)

        if ds.GetLayerCount() == 0:
            raise Exception("The XLSX file contains no readable layers")

        # Opcionalmente puedes recuperar las capas
        layers = [ds.GetLayerByIndex(i) for i in range(ds.GetLayerCount())]

        logger.info("layers", layers)

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

        schema_keys = [normalize_field_name(x.name) for layer in layers for x in layer.schema]
        geom_is_in_schema = any(
            x in schema_keys for x in XLSXFileHandler().possible_geometry_column_name
        )
        has_lat = any(x in XLSXFileHandler().possible_lat_column for x in schema_keys)
        has_long = any(x in XLSXFileHandler().possible_long_column for x in schema_keys)

        fields = (
            XLSXFileHandler().possible_geometry_column_name
            + XLSXFileHandler().possible_latlong_column
        )

        return True

    def get_ogr2ogr_driver(self):
        return ogr.GetDriverByName("XLSX")

    @staticmethod
    def create_ogr2ogr_command(files, original_name, ovverwrite_layer, alternate):
        """
        Define the ogr2ogr command to be executed.
        This is a default command that is needed to import a vector file
        """
        base_command = BaseVectorFileHandler.create_ogr2ogr_command(
            files, original_name, ovverwrite_layer, alternate
        )
        additional_option = ' -oo "GEOM_POSSIBLE_NAMES=geom*,the_geom*,wkt_geom" -oo "X_POSSIBLE_NAMES=x,long*" -oo "Y_POSSIBLE_NAMES=y,lat*"'
        return (
                f"{base_command} -oo KEEP_GEOM_COLUMNS=NO -lco GEOMETRY_NAME={BaseVectorFileHandler().default_geometry_column_name} "
                + additional_option
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
            {"name": normalize_field_name(x.name), "class_name": self._get_type(x), "null": True}
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
            resource = ResourceBase.objects.filter(alternate__istartswith=layer_name).first()
            return [
                {
                    "name": alternate,
                    "crs": resource.srid if resource and resource.srid else "EPSG:4326",
                }
            ]

        ds = self.get_ogr2ogr_driver().Open(files.get("base_file"), 0)
        if ds is None or ds.GetLayerCount() == 0:
            return []

        return [
            {
                "name": alternate or layer_name,
                "crs": self.identify_authority(ds.GetLayerByIndex(i)),
            }
            for i in range(ds.GetLayerCount())
            if self.fixup_name(ds.GetLayerByIndex(i).GetName()) == layer_name
        ]

    def identify_authority(self, layer):
        try:
            authority_code = super().identify_authority(layer=layer)
            return authority_code
        except Exception:
            return "EPSG:4326"

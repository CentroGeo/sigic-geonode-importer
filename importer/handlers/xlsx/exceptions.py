from rest_framework.exceptions import APIException
from rest_framework import status


class InvalidXLSXException(APIException):
    status_code = status.HTTP_400_BAD_REQUEST
    default_detail = "The csv provided is invalid"
    default_code = "invalid_csv"
    category = "importer"

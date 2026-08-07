from core.validation import BaseModelValidation

from .models import KoboFieldMapping, KoboForm, KoboToken


class KoboTokenValidation(BaseModelValidation):
    OBJECT_TYPE = KoboToken


class KoboFormValidation(BaseModelValidation):
    OBJECT_TYPE = KoboForm


class KoboFieldMappingValidation(BaseModelValidation):
    OBJECT_TYPE = KoboFieldMapping

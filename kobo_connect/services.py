from core.services import BaseService
from core.signals import register_service_signal

from .models import KoboFieldMapping, KoboForm, KoboToken
from .validations import (
    KoboFieldMappingValidation,
    KoboFormValidation,
    KoboTokenValidation,
)


class OwnedService(BaseService):
    def _adjust_create_payload(self, obj_data):
        data = dict(obj_data)
        data.pop("user", None)
        data["user_id"] = self.user.id
        return super()._adjust_create_payload(data)

    def _adjust_update_payload(self, obj_data):
        data = dict(obj_data)
        data.pop("user", None)
        data.pop("user_id", None)
        return super()._adjust_update_payload(data)


class KoboTokenService(OwnedService):
    OBJECT_TYPE = KoboToken

    def __init__(self, user, validation_class=KoboTokenValidation):
        super().__init__(user, validation_class)

    @register_service_signal("kobo_token_service.create")
    def create(self, obj_data):
        return super().create(obj_data)

    @register_service_signal("kobo_token_service.update")
    def update(self, obj_data):
        return super().update(obj_data)

    @register_service_signal("kobo_token_service.delete")
    def delete(self, obj_data):
        return super().delete(obj_data)


class KoboFormService(OwnedService):
    OBJECT_TYPE = KoboForm

    def __init__(self, user, validation_class=KoboFormValidation):
        super().__init__(user, validation_class)

    @register_service_signal("kobo_form_service.create")
    def create(self, obj_data):
        return super().create(obj_data)

    @register_service_signal("kobo_form_service.update")
    def update(self, obj_data):
        return super().update(obj_data)

    @register_service_signal("kobo_form_service.delete")
    def delete(self, obj_data):
        return super().delete(obj_data)


class KoboFieldMappingService(BaseService):
    OBJECT_TYPE = KoboFieldMapping

    def __init__(self, user, validation_class=KoboFieldMappingValidation):
        super().__init__(user, validation_class)

    @register_service_signal("kobo_field_mapping_service.create")
    def create(self, obj_data):
        return super().create(obj_data)

    @register_service_signal("kobo_field_mapping_service.update")
    def update(self, obj_data):
        return super().update(obj_data)

    @register_service_signal("kobo_field_mapping_service.delete")
    def delete(self, obj_data):
        return super().delete(obj_data)

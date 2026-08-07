import graphene
from django.core.exceptions import PermissionDenied
from django.utils.translation import gettext_lazy as _

from core.gql.gql_mutations.base_mutation import (
    BaseHistoryModelCreateMutationMixin,
    BaseHistoryModelDeleteMutationMixin,
    BaseHistoryModelUpdateMutationMixin,
    BaseMutation,
)
from core.schema import OpenIMISMutation

from .apps import KoboConnectConfig
from .models import KoboFieldMapping, KoboForm, KoboToken
from .services import (
    KoboFieldMappingService,
    KoboFormService,
    KoboTokenService,
)


def _require_permissions(user, permissions):
    if not user.has_perms(permissions):
        raise PermissionDenied(_("Unauthorized"))


def _clean_mutation_metadata(data):
    cleaned = dict(data)
    cleaned.pop("client_mutation_id", None)
    cleaned.pop("client_mutation_label", None)
    return cleaned


def _delete_with_service(service_class, user, data):
    cleaned = _clean_mutation_metadata(data)
    ids = cleaned.get("ids") or [cleaned.get("id")]
    for object_id in filter(None, ids):
        response = service_class(user).delete({"id": object_id, "user": user})
        if not response.get("success"):
            return response
    return None


class KoboTokenInput(OpenIMISMutation.Input):
    url_kobo = graphene.String(required=True)
    api_version = graphene.String()
    api_key = graphene.String(required=True)


class UpdateKoboTokenInput(OpenIMISMutation.Input):
    id = graphene.UUID(required=True)
    url_kobo = graphene.String()
    api_version = graphene.String()
    api_key = graphene.String()


class DeleteInput(OpenIMISMutation.Input):
    ids = graphene.List(graphene.UUID, required=True)


class CreateKoboTokenMutation(BaseHistoryModelCreateMutationMixin, BaseMutation):
    _mutation_class = "CreateKoboTokenMutation"
    _mutation_module = "kobo_connect"
    _model = KoboToken

    class Input(KoboTokenInput):
        pass

    @classmethod
    def _validate_mutation(cls, user, **data):
        super()._validate_mutation(user, **data)
        _require_permissions(user, KoboConnectConfig.gql_mutation_tokens_add_perms)

    @classmethod
    def _mutate(cls, user, **data):
        response = KoboTokenService(user).create(_clean_mutation_metadata(data))
        return None if response.get("success") else response


class UpdateKoboTokenMutation(BaseHistoryModelUpdateMutationMixin, BaseMutation):
    _mutation_class = "UpdateKoboTokenMutation"
    _mutation_module = "kobo_connect"
    _model = KoboToken

    class Input(UpdateKoboTokenInput):
        pass

    @classmethod
    def _validate_mutation(cls, user, **data):
        super()._validate_mutation(user, **data)
        _require_permissions(user, KoboConnectConfig.gql_mutation_tokens_update_perms)

    @classmethod
    def _mutate(cls, user, **data):
        response = KoboTokenService(user).update(_clean_mutation_metadata(data))
        return None if response.get("success") else response


class DeleteKoboTokenMutation(BaseHistoryModelDeleteMutationMixin, BaseMutation):
    _mutation_class = "DeleteKoboTokenMutation"
    _mutation_module = "kobo_connect"
    _model = KoboToken

    class Input(DeleteInput):
        pass

    @classmethod
    def _validate_mutation(cls, user, **data):
        super()._validate_mutation(user, **data)
        _require_permissions(user, KoboConnectConfig.gql_mutation_tokens_delete_perms)

    @classmethod
    def _mutate(cls, user, **data):
        return _delete_with_service(KoboTokenService, user, data)


class KoboFormInput(OpenIMISMutation.Input):
    code = graphene.String()
    kobo_id = graphene.String()
    name = graphene.String(required=True)
    description = graphene.String()
    kobo_uid = graphene.String(required=True)
    api_key_id = graphene.UUID(required=True)
    auto_sync = graphene.Boolean()
    is_active = graphene.Boolean()
    sync_interval = graphene.Int()
    module = graphene.String()
    form_uid = graphene.String()


class UpdateKoboFormInput(KoboFormInput):
    id = graphene.UUID(required=True)
    name = graphene.String()
    kobo_uid = graphene.String()
    api_key_id = graphene.UUID()


class CreateKoboFormMutation(BaseHistoryModelCreateMutationMixin, BaseMutation):
    _mutation_class = "CreateKoboFormMutation"
    _mutation_module = "kobo_connect"
    _model = KoboForm

    class Input(KoboFormInput):
        pass

    @classmethod
    def _validate_mutation(cls, user, **data):
        super()._validate_mutation(user, **data)
        _require_permissions(user, KoboConnectConfig.gql_mutation_forms_add_perms)

    @classmethod
    def _mutate(cls, user, **data):
        response = KoboFormService(user).create(_clean_mutation_metadata(data))
        return None if response.get("success") else response


class UpdateKoboFormMutation(BaseHistoryModelUpdateMutationMixin, BaseMutation):
    _mutation_class = "UpdateKoboFormMutation"
    _mutation_module = "kobo_connect"
    _model = KoboForm

    class Input(UpdateKoboFormInput):
        pass

    @classmethod
    def _validate_mutation(cls, user, **data):
        super()._validate_mutation(user, **data)
        _require_permissions(user, KoboConnectConfig.gql_mutation_forms_update_perms)

    @classmethod
    def _mutate(cls, user, **data):
        response = KoboFormService(user).update(_clean_mutation_metadata(data))
        return None if response.get("success") else response


class DeleteKoboFormMutation(BaseHistoryModelDeleteMutationMixin, BaseMutation):
    _mutation_class = "DeleteKoboFormMutation"
    _mutation_module = "kobo_connect"
    _model = KoboForm

    class Input(DeleteInput):
        pass

    @classmethod
    def _validate_mutation(cls, user, **data):
        super()._validate_mutation(user, **data)
        _require_permissions(user, KoboConnectConfig.gql_mutation_forms_delete_perms)

    @classmethod
    def _mutate(cls, user, **data):
        return _delete_with_service(KoboFormService, user, data)


class KoboFieldMappingInput(OpenIMISMutation.Input):
    kobo_form_id = graphene.UUID(required=True)
    kobo_field = graphene.String(required=True)
    grievance_field = graphene.String(required=True)


class UpdateKoboFieldMappingInput(KoboFieldMappingInput):
    id = graphene.UUID(required=True)
    kobo_form_id = graphene.UUID()


class CreateKoboFieldMappingMutation(BaseHistoryModelCreateMutationMixin, BaseMutation):
    _mutation_class = "CreateKoboFieldMappingMutation"
    _mutation_module = "kobo_connect"
    _model = KoboFieldMapping

    class Input(KoboFieldMappingInput):
        pass

    @classmethod
    def _validate_mutation(cls, user, **data):
        super()._validate_mutation(user, **data)
        _require_permissions(user, KoboConnectConfig.gql_mutation_forms_add_perms)

    @classmethod
    def _mutate(cls, user, **data):
        response = KoboFieldMappingService(user).create(_clean_mutation_metadata(data))
        return None if response.get("success") else response


class UpdateKoboFieldMappingMutation(BaseHistoryModelUpdateMutationMixin, BaseMutation):
    _mutation_class = "UpdateKoboFieldMappingMutation"
    _mutation_module = "kobo_connect"
    _model = KoboFieldMapping

    class Input(UpdateKoboFieldMappingInput):
        pass

    @classmethod
    def _validate_mutation(cls, user, **data):
        super()._validate_mutation(user, **data)
        _require_permissions(user, KoboConnectConfig.gql_mutation_forms_update_perms)

    @classmethod
    def _mutate(cls, user, **data):
        response = KoboFieldMappingService(user).update(_clean_mutation_metadata(data))
        return None if response.get("success") else response


class DeleteKoboFieldMappingMutation(BaseHistoryModelDeleteMutationMixin, BaseMutation):
    _mutation_class = "DeleteKoboFieldMappingMutation"
    _mutation_module = "kobo_connect"
    _model = KoboFieldMapping

    class Input(DeleteInput):
        pass

    @classmethod
    def _validate_mutation(cls, user, **data):
        super()._validate_mutation(user, **data)
        _require_permissions(user, KoboConnectConfig.gql_mutation_forms_delete_perms)

    @classmethod
    def _mutate(cls, user, **data):
        return _delete_with_service(KoboFieldMappingService, user, data)
import graphene
from core.gql.gql_mutations.base_mutation import BaseHistoryModelCreateMutationMixin, BaseMutation, \
    BaseHistoryModelUpdateMutationMixin, BaseHistoryModelDeleteMutationMixin
from core.schema import OpenIMISMutation
from .models import KoboToken, KoboForm, KoboFormMutation, KoboSyncLog, KoboFieldMapping
from django.core.exceptions import PermissionDenied
from django.utils.translation import gettext_lazy as _
from django.contrib.auth.models import User
from .gql_types import KoboFormGQLType, KoboTokenGQLType, KoboSyncLogGQLType, KoboFieldMappingGQLType


class CreateKoboTokenMutation(OpenIMISMutation):
    @classmethod
    def mutate(cls, root, info, **data):
        if not info.context.user.has_perms('kobo_connect.add_kobotoken'):
            raise PermissionDenied(_("Unauthorized"))
        kobo_token = KoboToken(**data)
        kobo_token.save(user=info.context.user)
        return None


class UpdateKoboTokenMutation(OpenIMISMutation):
    @classmethod
    def mutate(cls, root, info, **data):
        if not info.context.user.has_perms('kobo_connect.change_kobotoken'):
            raise PermissionDenied(_("Unauthorized"))
        kobo_token = KoboToken.objects.get(id=data['id'])
        for key, value in data.items():
            setattr(kobo_token, key, value)
        kobo_token.save(user=info.context.user)
        return None



class DeleteKoboTokenMutation(OpenIMISMutation):
    @classmethod
    def mutate(cls, root, info, **data):
        if not info.context.user.has_perms('kobo_connect.delete_kobotoken'):
            raise PermissionDenied(_("Unauthorized"))
        KoboToken.objects.filter(id__in=data['ids']).delete()
        return None



class CreateKoboFormMutation(OpenIMISMutation):
    @classmethod
    def mutate(cls, root, info, **data):
        if not info.context.user.has_perms('kobo_connect.add_koboform'):
            raise PermissionDenied(_("Unauthorized"))
        kobo_form = KoboForm(**data)
        kobo_form.save(user=info.context.user)
        return None



class UpdateKoboFormMutation(OpenIMISMutation):
    @classmethod
    def mutate(cls, root, info, **data):
        if not info.context.user.has_perms('kobo_connect.change_koboform'):
            raise PermissionDenied(_("Unauthorized"))
        kobo_form = KoboForm.objects.get(id=data['id'])
        for key, value in data.items():
            setattr(kobo_form, key, value)
        kobo_form.save(user=info.context.user)
        return None



class DeleteKoboFormMutation(OpenIMISMutation):
    @classmethod
    def mutate(cls, root, info, **data):
        if not info.context.user.has_perms('kobo_connect.delete_koboform'):
            raise PermissionDenied(_("Unauthorized"))
        KoboForm.objects.filter(id__in=data['ids']).delete()
        return None



class CreateKoboSyncLogMutation(OpenIMISMutation):
    @classmethod
    def mutate(cls, root, info, **data):
        if not info.context.user.has_perms('kobo_connect.add_kobosynclog'):
            raise PermissionDenied(_("Unauthorized"))
        kobo_sync_log = KoboSyncLog(**data)
        kobo_sync_log.save(user=info.context.user)
        return None



class UpdateKoboSyncLogMutation(OpenIMISMutation):
    @classmethod
    def mutate(cls, root, info, **data):
        if not info.context.user.has_perms('kobo_connect.change_kobosynclog'):
            raise PermissionDenied(_("Unauthorized"))
        kobo_sync_log = KoboSyncLog.objects.get(id=data['id'])
        for key, value in data.items():
            setattr(kobo_sync_log, key, value)
        kobo_sync_log.save(user=info.context.user)
        return None



class DeleteKoboSyncLogMutation(OpenIMISMutation):
    @classmethod
    def mutate(cls, root, info, **data):
        if not info.context.user.has_perms('kobo_connect.delete_kobosynclog'):
            raise PermissionDenied(_("Unauthorized"))
        KoboSyncLog.objects.filter(id__in=data['ids']).delete()
        return None



class CreateKoboFieldMappingMutation(OpenIMISMutation):
    @classmethod
    def mutate(cls, root, info, **data):
        if not info.context.user.has_perms('kobo_connect.add_kobofieldmapping'):
            raise PermissionDenied(_("Unauthorized"))
        kobo_field_mapping = KoboFieldMapping(**data)
        kobo_field_mapping.save(user=info.context.user)
        return None



class UpdateKoboFieldMappingMutation(OpenIMISMutation):
    @classmethod
    def mutate(cls, root, info, **data):
        if not info.context.user.has_perms('kobo_connect.change_kobofieldmapping'):
            raise PermissionDenied(_("Unauthorized"))
        kobo_field_mapping = KoboFieldMapping.objects.get(id=data['id'])
        for key, value in data.items():
            setattr(kobo_field_mapping, key, value)
        kobo_field_mapping.save(user=info.context.user)
        return None



class DeleteKoboFieldMappingMutation(OpenIMISMutation):
    @classmethod
    def mutate(cls, root, info, **data):
        if not info.context.user.has_perms('kobo_connect.delete_kobofieldmapping'):
            raise PermissionDenied(_("Unauthorized"))
        KoboFieldMapping.objects.filter(id__in=data['ids']).delete()
        return None

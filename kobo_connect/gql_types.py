import graphene
from graphene_django import DjangoObjectType
from django.core.exceptions import PermissionDenied
from django.utils.translation import gettext as _
from .models import KoboForm, KoboToken, KoboSyncLog, KoboFieldMapping
from django.contrib.auth.models import User


class KoboFormGQLType(DjangoObjectType):
    class Meta:
        model = KoboForm
        interfaces = (graphene.relay.Node,)
        filter_fields = {
            "id": ["exact", "isnull"],
            "name": ["exact", "istartswith", "icontains", "iexact"],
            "kobo_uid": ["exact", "isnull"],
            "api_key": ["exact", "isnull"],
            "user": ["exact", "isnull"],
            "auto_sync": ["exact", "istartswith", "icontains", "iexact"],
            "sync_interval": ["exact", "istartswith", "icontains", "iexact"],
        }
        description = "Type for Kobo forms in the system"

    @classmethod
    def get_node(cls, info, id):
        try:
            kobo_form = cls._meta.model.objects.get(id=id)
        except cls._meta.model.DoesNotExist:
            return None

        if info.context.user.has_perm("kobo_connect.view_koboform"):
            return kobo_form
        return None


class KoboTokenGQLType(DjangoObjectType):
    class Meta:
        model = KoboToken
        interfaces = (graphene.relay.Node,)
        filter_fields = {
            "id": ["exact"],
            "url_kobo": ["exact"],
            "api_version": ["exact", "icontains"],
            "api_key": ["exact", "icontains"],
            "user": ["exact", "isnull"],
        }
        description = "Type for Kobo tokens"


class KoboSyncLogGQLType(DjangoObjectType):
    class Meta:
        model = KoboSyncLog
        interfaces = (graphene.relay.Node,)
        filter_fields = {
            "id": ["exact"],
            "kobo_form": ["exact"],
            "sync_date": ["exact", "lt", "lte", "gt", "gte"],
            "status": ["exact", "in"],
        }
        description = "Type for Kobo synchronization logs"



class KoboFieldMappingGQLType(DjangoObjectType):
    class Meta:
        model = KoboFieldMapping
        interfaces = (graphene.relay.Node,)
        filter_fields = {
            "id": ["exact"],
            "kobo_form": ["exact"],
            "kobo_field": ["exact", "icontains"],
            "grievance_field": ["exact", "icontains"],
        }
        description = "Type for mapping between Kobo fields and Grievance fields"


# End of file

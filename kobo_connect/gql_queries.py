import graphene
from django.core.exceptions import PermissionDenied
from django.utils.translation import gettext as _
from .apps import KoboConnectConfig
from .models import KoboForm, KoboToken, KoboSyncLog, KoboFieldMapping
from core.schema import OrderedDjangoFilterConnectionField
from .gql_types import KoboFormGQLType, KoboTokenGQLType, KoboSyncLogGQLType, KoboFieldMappingGQLType



class Query(graphene.ObjectType):
    # kobo_forms = OrderedDjangoFilterConnectionField(KoboFormGQLType)
    kobo_forms = OrderedDjangoFilterConnectionField(
        KoboFormGQLType,
        # name=graphene.String(),
        # kobo_uid=graphene.String(),
        # auto_sync=graphene.Boolean()
    )
    kobo_form = graphene.Field(KoboFormGQLType, id=graphene.Int())

    kobo_tokens = OrderedDjangoFilterConnectionField(KoboTokenGQLType)
    kobo_token = graphene.Field(KoboTokenGQLType, id=graphene.Int())

    kobo_sync_logs = OrderedDjangoFilterConnectionField(KoboSyncLogGQLType)
    kobo_sync_log = graphene.Field(KoboSyncLogGQLType, id=graphene.Int())

    kobo_field_mappings = OrderedDjangoFilterConnectionField(KoboFieldMappingGQLType)
    kobo_field_mapping = graphene.Field(KoboFieldMappingGQLType, id=graphene.Int())

    def resolve_kobo_forms(self, info, **kwargs):
        self.check_permissions(info, KoboConnectConfig.gql_query_forms_perms)
        return KoboForm.objects.all()

    def resolve_kobo_form(self, info, id):
        self.check_permissions(info, KoboConnectConfig.gql_query_forms_perms)
        return KoboForm.objects.get(pk=id)

    def resolve_kobo_tokens(self, info, **kwargs):
        self.check_permissions(info, KoboConnectConfig.gql_query_tokens_perms)
        return KoboToken.objects.all()

    def resolve_kobo_token(self, info, id):
        self.check_permissions(info, KoboConnectConfig.gql_query_tokens_perms)
        return KoboToken.objects.get(pk=id)

    def resolve_kobo_sync_logs(self, info, **kwargs):
        self.check_permissions(info, KoboConnectConfig.gql_query_forms_perms)
        return KoboSyncLog.objects.all()

    def resolve_kobo_sync_log(self, info, id):
        self.check_permissions(info, KoboConnectConfig.gql_query_forms_perms)
        return KoboSyncLog.objects.get(pk=id)

    def resolve_kobo_field_mappings(self, info, **kwargs):
        self.check_permissions(info, KoboConnectConfig.gql_query_forms_perms)
        return KoboFieldMapping.objects.all()

    def resolve_kobo_field_mapping(self, info, id):
        self.check_permissions(info, KoboConnectConfig.gql_query_forms_perms)
        return KoboFieldMapping.objects.get(pk=id)

    def check_permissions(self, info, permission):
        if not info.context.user.has_perms(permission):
            raise PermissionDenied(_("Unauthorized"))


# End of file

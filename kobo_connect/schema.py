import graphene
import graphene_django_optimizer as gql_optimizer
from django.utils.translation import gettext_lazy as _

from core.schema import OrderedDjangoFilterConnectionField
# from .gql_queries import *


from .gql_mutations import (
    CreateKoboTokenMutation, UpdateKoboTokenMutation, DeleteKoboTokenMutation,
    CreateKoboFormMutation, UpdateKoboFormMutation, DeleteKoboFormMutation,
    CreateKoboSyncLogMutation, UpdateKoboSyncLogMutation, DeleteKoboSyncLogMutation,
    CreateKoboFieldMappingMutation, UpdateKoboFieldMappingMutation, DeleteKoboFieldMappingMutation
)


from .models import KoboToken, KoboForm
from django.contrib.auth.models import AnonymousUser
from core.utils import append_validity_filter
from .gql_queries import Query as KoboConnectQuery


class Mutation(graphene.ObjectType):

    create_kobo_token = CreateKoboTokenMutation.Field()
    update_kobo_token = UpdateKoboTokenMutation.Field()
    delete_kobo_token = DeleteKoboTokenMutation.Field()

    create_kobo_form = CreateKoboFormMutation.Field()
    update_kobo_form = UpdateKoboFormMutation.Field()
    delete_kobo_form = DeleteKoboFormMutation.Field()

    create_kobo_sync_log = CreateKoboSyncLogMutation.Field()
    update_kobo_sync_log = UpdateKoboSyncLogMutation.Field()
    delete_kobo_sync_log = DeleteKoboSyncLogMutation.Field()

    create_kobo_field_mapping = CreateKoboFieldMappingMutation.Field()
    update_kobo_field_mapping = UpdateKoboFieldMappingMutation.Field()
    delete_kobo_field_mapping = DeleteKoboFieldMappingMutation.Field()


class Query(KoboConnectQuery, graphene.ObjectType):
    pass


schema = graphene.Schema(query=Query, mutation=Mutation)


# Add this extra blank line

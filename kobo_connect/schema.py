import graphene

from .gql_mutations import (
    CreateKoboFieldMappingMutation,
    CreateKoboFormMutation,
    CreateKoboTokenMutation,
    DeleteKoboFieldMappingMutation,
    DeleteKoboFormMutation,
    DeleteKoboTokenMutation,
    UpdateKoboFieldMappingMutation,
    UpdateKoboFormMutation,
    UpdateKoboTokenMutation,
)
from .gql_queries import Query as KoboConnectQuery


class Mutation(graphene.ObjectType):
    create_kobo_token = CreateKoboTokenMutation.Field()
    update_kobo_token = UpdateKoboTokenMutation.Field()
    delete_kobo_token = DeleteKoboTokenMutation.Field()

    create_kobo_form = CreateKoboFormMutation.Field()
    update_kobo_form = UpdateKoboFormMutation.Field()
    delete_kobo_form = DeleteKoboFormMutation.Field()

    create_kobo_field_mapping = CreateKoboFieldMappingMutation.Field()
    update_kobo_field_mapping = UpdateKoboFieldMappingMutation.Field()
    delete_kobo_field_mapping = DeleteKoboFieldMappingMutation.Field()


class Query(KoboConnectQuery, graphene.ObjectType):
    pass


schema = graphene.Schema(query=Query, mutation=Mutation)
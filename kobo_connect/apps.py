from django.apps import AppConfig

MODULE_NAME = "kobo_connect"



DEFAULT_CFG = {
    # ... autres permissions ...
    "gql_query_forms_perms": ["121805"],  # Nouvelle permission pour interroger les formulaires
}


class KoboConnectConfig(AppConfig):
    name = MODULE_NAME

    gql_query_forms_perms = []
    # gql_mutation_forms_add_perms = []
    # gql_mutation_forms_update_perms = []
    # gql_mutation_forms_delete_perms = []

    def __load_config(self, cfg):
        for field in cfg:
            if hasattr(KoboConnectConfig, field):
                setattr(KoboConnectConfig, field, cfg[field])

    def ready(self):
        from core.models import ModuleConfiguration
        # ModuleConfiguration.register_module(MODULE_NAME)
        cfg = ModuleConfiguration.get_or_default(MODULE_NAME, DEFAULT_CFG)
        self.__load_config(cfg)


# End of file

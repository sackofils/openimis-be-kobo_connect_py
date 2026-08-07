from types import SimpleNamespace

from django.core.exceptions import PermissionDenied
from django.test import SimpleTestCase

from .apps import DEFAULT_CFG
from .gql_mutations import CreateKoboTokenMutation
from .gql_types import KoboTokenGQLType
from .models import KoboToken
from .services import KoboTokenService


class PermissionUser:
    id = "00000000-0000-0000-0000-000000000001"
    username = "permission-user"

    def __init__(self, allowed):
        self.allowed = allowed

    def has_perms(self, permissions):
        return self.allowed


class KoboConfigurationTestCase(SimpleTestCase):
    def test_default_permission_ids_are_unique(self):
        permission_ids = [
            permission
            for name, permissions in DEFAULT_CFG.items()
            if name.endswith("_perms")
            for permission in permissions
        ]
        self.assertEqual(len(permission_ids), len(set(permission_ids)))

    def test_api_key_is_encrypted_and_not_exposed_by_graphql(self):
        self.assertEqual(
            KoboToken._meta.get_field("api_key").get_internal_type(),
            "BinaryField",
        )
        self.assertNotIn("api_key", KoboTokenGQLType._meta.fields)

    def test_token_creation_requires_configured_permission(self):
        user = PermissionUser(allowed=False)
        with self.assertRaises(PermissionDenied):
            CreateKoboTokenMutation._validate_mutation(
                user,
                url_kobo="https://kobo.example",
                api_key="secret",
            )

        user.allowed = True
        CreateKoboTokenMutation._validate_mutation(
            user,
            url_kobo="https://kobo.example",
            api_key="secret",
        )

    def test_token_service_forces_authenticated_owner(self):
        user = SimpleNamespace(id="owner-id", username="owner")
        payload = KoboTokenService(user)._adjust_create_payload(
            {"url_kobo": "https://kobo.example", "api_key": "secret", "user_id": "other"}
        )
        self.assertEqual(payload["user_id"], "owner-id")

from unittest.mock import Mock, patch

from .client import KoboClient


class KoboClientTestCase(SimpleTestCase):
    @patch("kobo_connect.client.requests.get")
    def test_iter_submissions_follows_kpi_pagination(self, get):
        first = Mock()
        first.raise_for_status.return_value = None
        first.json.return_value = {
            "results": [{"_uuid": "one"}],
            "next": "https://kobo.example/api/v2/assets/form/data/?page=2",
        }
        second = Mock()
        second.raise_for_status.return_value = None
        second.json.return_value = {
            "results": [{"_uuid": "two"}],
            "next": None,
        }
        get.side_effect = [first, second]

        submissions = list(
            KoboClient("https://kobo.example", "secret").iter_submissions("form")
        )

        self.assertEqual([item["_uuid"] for item in submissions], ["one", "two"])
        self.assertEqual(get.call_count, 2)
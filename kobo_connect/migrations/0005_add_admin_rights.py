from django.db import migrations


RIGHTS = [121805, 121806, 121807, 121808, 121809, 121810, 121811, 121812]
ADMINISTRATOR_SYSTEM_ROLE = 64


def add_rights(apps, schema_editor):
    Role = apps.get_model("core", "Role")
    RoleRight = apps.get_model("core", "RoleRight")
    role = Role.objects.get(is_system=ADMINISTRATOR_SYSTEM_ROLE)
    for right_id in RIGHTS:
        RoleRight.objects.get_or_create(
            role=role,
            right_id=right_id,
            validity_to=None,
            defaults={"audit_user_id": 1},
        )


def remove_rights(apps, schema_editor):
    RoleRight = apps.get_model("core", "RoleRight")
    RoleRight.objects.filter(
        role__is_system=ADMINISTRATOR_SYSTEM_ROLE,
        right_id__in=RIGHTS,
        validity_to__isnull=True,
    ).delete()


class Migration(migrations.Migration):

    dependencies = [
        ("kobo_connect", "0004_encrypt_kobotoken_api_key"),
    ]

    operations = [
        migrations.RunPython(add_rights, remove_rights),
    ]

from django.db import migrations, models
import django_cryptography.fields
from django.db.migrations.exceptions import IrreversibleError


def encrypt_existing_keys(apps, schema_editor):
    for model_name in ("KoboToken", "HistoricalKoboToken"):
        model = apps.get_model("kobo_connect", model_name)
        for token in model.objects.only("api_key", "api_key_encrypted").iterator():
            token.api_key_encrypted = token.api_key
            token.save(update_fields=["api_key_encrypted"])


def irreversible_reverse(apps, schema_editor):
    raise IrreversibleError('API key encryption cannot be safely reversed.')


class Migration(migrations.Migration):

    dependencies = [
        ("kobo_connect", "0003_koboform_module_and_form_uid"),
    ]

    operations = [
        migrations.AddField(
            model_name="kobotoken",
            name="api_key_encrypted",
            field=django_cryptography.fields.encrypt(
                models.CharField(max_length=255, null=True)
            ),
        ),
        migrations.AddField(
            model_name="historicalkobotoken",
            name="api_key_encrypted",
            field=django_cryptography.fields.encrypt(
                models.CharField(max_length=255, null=True)
            ),
        ),
        migrations.RunPython(encrypt_existing_keys, irreversible_reverse),
        migrations.RemoveField(model_name="kobotoken", name="api_key"),
        migrations.RemoveField(model_name="historicalkobotoken", name="api_key"),
        migrations.RenameField(
            model_name="kobotoken",
            old_name="api_key_encrypted",
            new_name="api_key",
        ),
        migrations.RenameField(
            model_name="historicalkobotoken",
            old_name="api_key_encrypted",
            new_name="api_key",
        ),
        migrations.AlterField(
            model_name="kobotoken",
            name="api_key",
            field=django_cryptography.fields.encrypt(
                models.CharField(max_length=255)
            ),
        ),
        migrations.AlterField(
            model_name="historicalkobotoken",
            name="api_key",
            field=django_cryptography.fields.encrypt(
                models.CharField(max_length=255)
            ),
        ),
    ]

from django.db import migrations, models


MODULE_CHOICES = [
    ("grievance_social_protection", "Grievance / Plaintes"),
    ("monitoring_evaluation", "Monitoring Evaluation"),
]


class Migration(migrations.Migration):

    dependencies = [
        ("kobo_connect", "0002_alter_historicalkobotoken_api_version_and_more"),
    ]

    operations = [
        migrations.AddField(
            model_name="historicalkoboform",
            name="form_uid",
            field=models.CharField(blank=True, db_index=True, max_length=255, null=True),
        ),
        migrations.AddField(
            model_name="historicalkoboform",
            name="module",
            field=models.CharField(
                blank=True, choices=MODULE_CHOICES, max_length=128, null=True
            ),
        ),
        migrations.AddField(
            model_name="koboform",
            name="form_uid",
            field=models.CharField(
                blank=True, max_length=255, null=True, unique=True
            ),
        ),
        migrations.AddField(
            model_name="koboform",
            name="module",
            field=models.CharField(
                blank=True, choices=MODULE_CHOICES, max_length=128, null=True
            ),
        ),
    ]

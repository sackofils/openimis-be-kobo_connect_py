from django.conf import settings
from django.db import models
from core import models as core_models
from core.models import HistoryBusinessModel, User, HistoryModel
from django.contrib.contenttypes.fields import GenericForeignKey
from django.contrib.contenttypes.models import ContentType
from graphql import ResolveInfo


class KoboToken(HistoryBusinessModel):
    url_kobo = models.CharField(max_length=255)
    api_version = models.CharField(max_length=8, null=True, blank=True)
    api_key = models.CharField(max_length=255)
    user = models.ForeignKey(User, on_delete=models.CASCADE)

    def __str__(self):
        return f"url {self.url_kobo}"


class KoboForm(HistoryBusinessModel):
    kobo_id = models.CharField(max_length=255, null=True, blank=True)
    name = models.CharField(max_length=255, blank=False, null=False)
    description = models.TextField(blank=True, null=True)
    kobo_uid = models.CharField(max_length=255, unique=True, blank=False, null=False)
    api_key = models.ForeignKey(KoboToken, on_delete=models.CASCADE, related_name='forms')
    user = models.ForeignKey(User, on_delete=models.DO_NOTHING, blank=False, null=False)
    auto_sync = models.BooleanField(default=False)
    sync_interval = models.IntegerField(null=True, blank=True)  # interval in minutes
    last_sync_date = models.DateTimeField(null=True, blank=True)

    def __str__(self):
        return f"Sync Settings for {self.name} by {self.user.username}"



class KoboSyncLog(HistoryBusinessModel):
    kobo_form = models.ForeignKey(KoboForm, on_delete=models.CASCADE, related_name='sync_logs')
    sync_date = models.DateTimeField(auto_now_add=True)
    status = models.CharField(max_length=20, choices=[('success', 'Success'), ('failed', 'Failed')])
    action = models.CharField(max_length=50, null=True, blank=True)  # 'created', 'updated', etc.
    error_message = models.TextField(null=True, blank=True)
    details = models.TextField(null=True, blank=True)  # Ex: "Ticket code: TK-001"

    def __str__(self):
        return f"status {self.status}"



class KoboFieldMapping(HistoryBusinessModel):
    kobo_form = models.ForeignKey(KoboForm, on_delete=models.CASCADE, related_name='field_mappings')
    kobo_field = models.CharField(max_length=255)
    grievance_field = models.CharField(max_length=255)



class KoboFormMutation(core_models.UUIDModel, core_models.ObjectMutation):
    kobo_form = models.ForeignKey(KoboForm, models.DO_NOTHING, related_name='mutations')
    mutation = models.ForeignKey(core_models.MutationLog, models.DO_NOTHING, related_name='kobo_forms')

    class Meta:
        managed = True
        db_table = "kobo_koboformmutation"

from django import forms
from django.contrib import admin, messages
from django.utils.translation import gettext_lazy as _
from .models import KoboFieldMapping, KoboForm, KoboSyncLog, KoboToken
from .synchronizer import start_sync


@admin.register(KoboFieldMapping)
class KoboFieldMappingAdmin(admin.ModelAdmin):
    list_display = ('kobo_form', 'kobo_field', 'grievance_field')
    list_filter = ('kobo_form',)
    search_fields = ('kobo_field', 'grievance_field')

    def save_model(self, request, obj, form, change):
        obj.save(user=request.user)


class KoboFieldMappingInline(admin.TabularInline):
    model = KoboFieldMapping
    extra = 1


class KoboSyncLogInline(admin.TabularInline):
    model = KoboSyncLog
    extra = 0
    readonly_fields = ('sync_date', 'status', 'action', 'error_message', 'details')
    can_delete = False
    show_change_link = False


@admin.register(KoboForm)
class KoboFormAdmin(admin.ModelAdmin):
    list_display = ('name', 'kobo_uid', 'user', 'auto_sync', 'is_active', 'last_sync_date', 'last_sync_status')
    search_fields = ('name', 'kobo_uid')
    # inlines = [KoboFieldMappingInline,]
    actions = ['sync_selected_forms', "clone_for_update"]

    save_as = True

    @admin.action(description="Cloner pour mise à jour (nouvelle version)")
    def clone_for_update(self, request, queryset):
        if queryset.count() != 1:
            self.message_user(request, "Sélectionnez un seul formulaire.", level=messages.WARNING)
            return
        old = queryset.first()
        # Crée une nouvelle instance sans PK, en copiant les champs utiles
        new_obj = KoboForm()
        for f in old._meta.fields:
            if f.primary_key:
                continue

            if f.name in ("validity_to",):
                continue
            setattr(new_obj, f.name, getattr(old, f.name))
        new_obj.save()
        self.message_user(request, f"Nouvelle version créée: {new_obj}", level=messages.SUCCESS)

    @admin.action(description="Lancer la synchronisation des formulaires sélectionnés")
    def sync_selected_forms(self, request, queryset):
        success, failure = 0, 0

        for form in queryset:
            try:
                start_sync(form, request.user)
                success += 1
            except Exception as e:
                messages.error(request, _(f"Erreur pour {form.name} : {str(e)}"))
                failure += 1

        if success:
            self.message_user(request, _(f"{success} synchronisation(s) réussie(s)."), level=messages.SUCCESS)
        if failure:
            self.message_user(request, _(f"{failure} échec(s) de synchronisation."), level=messages.ERROR)

    @admin.display(description="Dernier statut sync")
    def last_sync_status(self, obj):
        last_log = obj.sync_logs.order_by('-sync_date').first()
        if last_log:
            return f"{last_log.status} ({last_log.action})"
        return "Jamais synchronisé"

    def save_model(self, request, obj, form, change):
        obj.save(user=request.user)


class KoboTokenAdminForm(forms.ModelForm):
    api_key = forms.CharField(
        label=_("Cle API"),
        required=False,
        widget=forms.PasswordInput(render_value=False),
    )

    class Meta:
        model = KoboToken
        fields = "__all__"

    def clean_api_key(self):
        api_key = self.cleaned_data.get("api_key")
        if api_key:
            return api_key
        if self.instance.pk:
            return self.instance.api_key
        raise forms.ValidationError(_("La cle API est obligatoire."))

@admin.register(KoboToken)
class KoboTokenAdmin(admin.ModelAdmin):
    form = KoboTokenAdminForm
    list_display = ('user', 'url_kobo', 'api_version', 'has_api_key')
    search_fields = ('user__username', 'url_kobo')
    list_filter = ('api_version',)
    # readonly_fields = ('created_at', 'updated_at')

    fieldsets = (
        (None, {
            'fields': ('user', 'url_kobo', 'api_version', 'api_key')
        }),
    )

    def save_model(self, request, obj, form, change):
        obj.save(user=request.user)

    @admin.display(boolean=True, description=_("Cle API configuree"))
    def has_api_key(self, obj):
        return bool(obj.api_key)

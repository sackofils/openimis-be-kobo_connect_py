import logging
from datetime import timedelta
from django.utils import timezone
from django.contrib.auth import get_user_model
from django.contrib.contenttypes.models import ContentType
from django.utils.dateparse import parse_datetime
from .models import KoboToken, KoboForm, KoboSyncLog, KoboFieldMapping
from grievance_social_protection.models import Ticket
from core.models import User
from location.models import Location
from django.db.models import Q
from django.apps import apps

logger = logging.getLogger(__name__)
user_ct = ContentType.objects.get_for_model(User)


def get_kobo_manager(api_key):
    """
    Initialise un client KoboManager à partir d'un api_key.
    """
    from pykobo import Manager

    try:
        kobo_token = KoboToken.objects.filter(api_key=api_key).first()
        if not kobo_token:
            logger.warning(f"[Token] Aucun token Kobo trouvé pour l'API Key : {api_key}")
            return None
        return Manager(
            url=kobo_token.url_kobo,
            api_version=int(kobo_token.api_version),
            token=kobo_token.api_key
        )
    except Exception as e:
        logger.error(f"[Token] Erreur d'initialisation du client Kobo : {str(e)}")
        return None


def fetch_single_form(km, form_uid):
    try:
        return km.get_form(form_uid) if km else None
    except Exception as e:
        logger.error(f"[Form] Erreur de récupération : {str(e)}")
        return None


def fetch_form_data(form_obj):
    try:
        form_obj.fetch_data()
        return form_obj.data if form_obj else None
    except Exception as e:
        logger.error(f"[Data] Erreur de récupération des données Kobo : {str(e)}")
        return None


def should_sync(form):
    if not form.sync_interval:
        return True
    last_sync = form.last_sync_date or form.created_at
    return timezone.now() >= last_sync + timedelta(minutes=form.sync_interval)


def sync_all_kobo_forms():
    """
    Synchronise tous les KoboForms avec auto_sync=True.
    """
    forms = KoboForm.objects.filter(auto_sync=True)
    for form in forms:
        if should_sync(form):
            start_sync(form)
        else:
            logger.info(f"[Sync] {form.name} : Synchronisation non nécessaire.")


def start_sync(form, user=None):
    logger.info(f"[Sync] Début de la synchronisation : {form.name} ({form.kobo_uid})")

    try:
        km = get_kobo_manager(form.api_key.api_key)
        if not km:
            raise Exception("Client Kobo non initialisé.")

        form_obj = fetch_single_form(km, form.kobo_uid)
        if not form_obj:
            raise Exception("Formulaire introuvable dans Kobo.")

        data = fetch_form_data(form_obj)
        if data is None:
            raise Exception("Données vides.")

        process_kobo_data_for_ticket(form, data)

        form.kobo_id = form_obj.id
        form.last_sync_date = timezone.now()
        form.save(user=user)

        log = KoboSyncLog(
            kobo_form=form,
            status='success',
            action='sync_completed',
            details=f"{len(data)} soumissions traitées"
        )
        log.save(user=user)

    except Exception as e:
        logger.error(f"[Sync] Erreur critique : {str(e)}")
        sync = KoboSyncLog(
            kobo_form=form,
            status='failed',
            action='sync_error',
            error_message=str(e),
            details=f"Erreur durant la synchronisation du formulaire {form.kobo_uid}"
        )
        sync.save(user=user)


def generate_field_mappings_from_submission(kobo_form, sample_submission: dict):
    """
    Génère automatiquement des KoboFieldMapping pour un KoboForm donné à partir d’une soumission.
    Associe les champs Kobo aux champs existants du modèle Ticket si le nom correspond.
    """
    ticket_fields = {f.name for f in Ticket._meta.get_fields()}
    created = []

    for kobo_field in sample_submission.keys():
        field_name = kobo_field.split("/")[-1]  # Prend le dernier segment

        if field_name in ticket_fields:
            mapping, created_flag = KoboFieldMapping.objects.get_or_create(
                kobo_form=kobo_form,
                kobo_field=kobo_field,
                grievance_field=field_name
            )
            if created_flag:
                created.append(mapping)

    return created

# sample = response["results"][0]  # une soumission Kobo en JSON
# generate_field_mappings_from_submission(kobo_form, sample)

def suggest_unmapped_fields(kobo_form, submission: dict):
    """
    Retourne une liste des champs Kobo qui ne sont pas mappés
    et qui ne correspondent à aucun champ du modèle Ticket.
    """
    mapped_fields = set(KoboFieldMapping.objects.filter(kobo_form=kobo_form)
                        .values_list("kobo_field", flat=True))
    ticket_fields = {f.name for f in Ticket._meta.get_fields()}

    unmapped = []

    for kobo_field in submission.keys():
        if kobo_field not in mapped_fields:
            candidate = kobo_field.split("/")[-1]
            if candidate not in ticket_fields:
                unmapped.append(kobo_field)

    return unmapped


def get_location_by_name_or_code(value, loc_type):
    if not value:
        return None
    return Location.objects.filter(
        Q(name__iexact=value.strip()) | Q(code__iexact=value.strip()),
        type=loc_type
    ).first()


def process_kobo_data_for_ticket(kobo_form, data):
    mappings = KoboFieldMapping.objects.filter(kobo_form=kobo_form)
    field_map = {m.kobo_field: m.grievance_field for m in mappings}

    for submission in data:
        ticket_data = {}
        reporter_value = None

        for kobo_field, ticket_field in field_map.items():
            value = submission.get(kobo_field)

            if ticket_field == 'reporter':
                reporter_value = value
                continue

            if ticket_field == 'date_of_incident':
                dt = parse_datetime(value)
                ticket_data[ticket_field] = dt.date() if dt else None
            else:
                ticket_data[ticket_field] = value

        # Traitement du reporter (via id ou username)
        submitted_by = submission.get("_submitted_by") or reporter_value
        if submitted_by:
            user = User.objects.filter(Q(id=submitted_by) | Q(username=submitted_by)).first()
            if user:
                ticket_data["reporter_type"] = user_ct
                ticket_data["reporter_id"] = user.id
            else:
                log = KoboSyncLog(
                    kobo_form=kobo_form,
                    status='failed',
                    action='invalid_reporter',
                    error_message=f"Utilisateur non trouvé : {submitted_by}",
                    details=str(submission)
                )
                log.save(user=user)
                continue

        # Mapping géographique via modèles Location
        ticket_data["region"] = get_location_by_name_or_code(submission.get("group_geo/region"), "region")
        ticket_data["prefecture"] = get_location_by_name_or_code(submission.get("group_geo/prefecture"), "prefecture")
        ticket_data["sous_prefecture"] = get_location_by_name_or_code(submission.get("group_geo/sous_prefecture"),
                                                                      "sous_prefecture")
        ticket_data["district"] = get_location_by_name_or_code(submission.get("group_geo/district"), "district")

        # Vérification des doublons
        existing_ticket = None
        if "code" in ticket_data:
            existing_ticket = Ticket.objects.filter(code=ticket_data["code"]).first()
        elif "title" in ticket_data:
            existing_ticket = Ticket.objects.filter(title=ticket_data["title"]).first()

        try:
            if existing_ticket:
                for field, value in ticket_data.items():
                    setattr(existing_ticket, field, value)
                existing_ticket.save()

                log = KoboSyncLog(
                    kobo_form=kobo_form,
                    status='success',
                    action='updated',
                    details=f"Ticket mis à jour: {existing_ticket.code or existing_ticket.title}"
                )
                log.save(user=user)
                logger.info(f"[Sync] Ticket mis à jour : {existing_ticket.title}")
            else:
                new_ticket = Ticket.objects.create(**ticket_data)
                log = KoboSyncLog(
                    kobo_form=kobo_form,
                    status='success',
                    action='created',
                    details=f"Ticket créé: {new_ticket.code or new_ticket.title}"
                )
                log.save(user=user)
                logger.info(f"[Sync] Ticket créé : {new_ticket.title}")

        except Exception as e:
            logger.error(f"[Sync] Erreur création/màj ticket : {str(e)}")
            log = KoboSyncLog(
                kobo_form=kobo_form,
                status='failed',
                action='error',
                error_message=str(e),
                details=str(submission)
            )
            log.save(user=user)

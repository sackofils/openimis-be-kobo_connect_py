import logging
from django.utils import timezone
from datetime import timedelta
from .models import KoboToken, KoboForm, KoboSyncLog
import pykobo
from .util import model_obj_to_json

logger = logging.getLogger(__name__)


def check_data_token(api_key):
    try:
        kobo_token = KoboToken.objects.filter(api_key=api_key).first()
        if kobo_token:
            token_data = {
                'url_kobo': kobo_token.url_kobo,
                'api_version': kobo_token.api_version,
                'token': kobo_token.token,
                'user_name': kobo_token.user.username,
                # Add any other fields you want to retrieve
            }
            # Initialize the Manager object
            km = pykobo.Manager(
                url=token_data['url_kobo'],
                api_version=token_data['api_version'],
                token=token_data['token']
            )
            return km
        else:
            logger.warning(f"Aucun token Kobo correspondant à '{api_key}' n'a été trouvé.")
            return None

    except Exception as e:
        logger.error(f"Une erreur s'est produite lors de la vérification de synchronisation : {str(e)}")


# Get the list of forms you have access to
def fetch_forms(km):
    if km:
        forms = km.get_forms()
        return forms
    else:
        return None


# Fetch a single form with its uid
def fetch_single_form(km, form_uid):
    if km:
        form = km.get_form(form_uid)
        return form
    else:
        return None


# Fetch the data of a form
def fetch_form_data(form):
    if form:
        data = form.fetch_data()
        return data.data
    else:
        return None




# Commencer la synchronisation
def start_sync(form):
    if form:
        sync_form(form)
    else:
        logger.warning(f"Aucun formulaire trouvé pour la synchronisation.")


# Check if the form should be synced
def should_sync(form):
    if not form.sync_interval:
        return True
    last_sync = form.last_sync_date or form.created_at
    next_sync = last_sync + timedelta(minutes=form.sync_interval)
    return timezone.now() >= next_sync



def sync_form(form):
    try:
        logger.info(f"Synchronisation du formulaire {form.name} (ID: {form.kobo_uid})")
        # Récupérer les données du formulaire depuis Kobo
        data = form.fetch_data()
        # Traiter les données ici (par exemple, les sauvegarder dans la base de données OpenIMIS)
        process_kobo_data(data.data)

        # Sectionner id du formulaire dans koboForm
        form.kobo_id = data.id
        form.save()

        # Mettre à jour la date de dernière synchronisation
        form.last_sync_date = timezone.now()
        form.save()

        # Créer un log de synchronisation
        KoboSyncLog.objects.create(
            kobo_form=form,
            status='success',
            sync_date=timezone.now()
        )

    except Exception as e:
        logger.error(f"Erreur lors de la synchronisation du formulaire {form.name} : {str(e)}")
        KoboSyncLog.objects.create(
            kobo_form=form,
            status='failed',
            sync_date=timezone.now(),
            error_message=str(e)
        )



def process_kobo_data(data):
    # Implémentez ici la logique pour traiter les données Kobo
    # Par exemple, créer ou mettre à jour des enregistrements dans OpenIMIS
    pass

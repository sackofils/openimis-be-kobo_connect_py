from .synchronizer import sync_all_kobo_forms
import logging

logger = logging.getLogger(__name__)


def run_kobo_sync_job(*args, **kwargs):
    """
    Fonction appelée par le scheduler pour lancer la synchronisation Kobo.
    """
    logger.info("[Scheduler] Démarrage de la tâche de synchronisation Kobo")
    sync_all_kobo_forms()
    logger.info("[Scheduler] Fin de la synchronisation Kobo")

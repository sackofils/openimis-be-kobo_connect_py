import logging
import re
from copy import deepcopy
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Dict, Iterable, List, Optional, Tuple

from django.apps import apps
from django.core.exceptions import ValidationError
from django.db import transaction, models as djm
from django.utils import timezone
from django.utils.dateparse import parse_datetime

from .models import KoboToken, KoboForm, KoboFieldMapping, KoboSyncLog
from grievance_social_protection.escalation_services import next_due_date, bootstrap_escalation_fields

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers: Label resolver for select_one / select_multiple
# ---------------------------------------------------------------------------

def _norm_lang_key(s: str) -> str:
    import unicodedata
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    return re.sub(r"[\s()]+", "", s).lower()


def _pick_lang(label_obj, pref_lang: Optional[str]):
    """Return best text from a possibly multilingual label object."""
    if isinstance(label_obj, str):
        return label_obj
    if isinstance(label_obj, dict) and label_obj:
        if pref_lang:
            want = _norm_lang_key(pref_lang)
            for k, v in label_obj.items():
                if _norm_lang_key(k) == want:
                    return v
        return next(iter(label_obj.values()))
    if isinstance(label_obj, list) and label_obj:
        first = label_obj[0]
        if isinstance(first, dict):
            if pref_lang:
                want = _norm_lang_key(pref_lang)
                for it in label_obj:
                    if _norm_lang_key(str(it.get("lang", ""))) == want:
                        return it.get("text") or it
            return first.get("text") or first
        # e.g. ["Oui"] → "Oui"
        return first
    return None


def _build_choice_resolver(client, asset_uid: str, lang: Optional[str] = None):
    """
    Construit un résolveur (kobo_key, raw_val) -> label(s) en s'appuyant sur
    content.survey / content.choices de l'asset KPI v2.
    Gère: choices en dict OU liste, select_from_list_name, multi-select (espaces/virgules/listes).
    """
    asset = client.get_json(f"assets/{asset_uid}/")  # format=json est forcé par get_json
    content = asset.get("content") or {}
    survey = content.get("survey") or []
    choices_raw = content.get("choices") or []

    # Normaliser choices en dict(list_name -> list[choice])
    if isinstance(choices_raw, dict):
        choices_by_list = choices_raw
    else:
        choices_by_list: Dict[str, List[Dict[str, Any]]] = {}
        for ch in choices_raw:
            ln = ch.get("list_name")
            if ln:
                choices_by_list.setdefault(ln, []).append(ch)

    # list_name -> { code: label }
    list_to_labels: Dict[str, Dict[str, str]] = {}
    for ln, items in (choices_by_list or {}).items():
        lut: Dict[str, str] = {}
        for ch in items or []:
            code = ch.get("name")
            label = _pick_lang(ch.get("label"), lang)
            if code is not None and label is not None:
                lut[str(code)] = str(label)
        list_to_labels[ln] = lut

    # question_name -> list_name
    field_to_list: Dict[str, str] = {}
    for q in survey:
        qname = q.get("name")
        qtype = (q.get("type") or "").strip()
        ln = None
        if q.get("select_from_list_name"):
            ln = q["select_from_list_name"]
        elif qtype.startswith("select_one") or qtype.startswith("select_multiple"):
            parts = qtype.split()
            ln = parts[1] if len(parts) > 1 else None
        if qname and ln:
            field_to_list[qname] = ln

    logger.debug("[Kobo Label] field_to_list: %s", field_to_list)
    logger.debug("[Kobo Label] lists: %s", {k: len(v) for k, v in list_to_labels.items()})

    def resolve(kobo_key: str, raw_val):
        leaf = kobo_key.split("/")[-1]
        ln = field_to_list.get(leaf)
        if not ln:
            return raw_val  # pas une question select_*
        lut = list_to_labels.get(ln, {})
        # multi-select
        if isinstance(raw_val, list):
            codes = raw_val
        elif isinstance(raw_val, str):
            codes = re.split(r"[,\s]+", raw_val.strip())
            codes = [c for c in codes if c]
        else:
            codes = None
        if codes is not None:
            return [lut.get(str(c), c) for c in codes]
        # select_one
        return lut.get(str(raw_val), raw_val)

    return resolve


def _coerce_bool_fr(value: Any) -> Any:
    if isinstance(value, str):
        v = value.strip().lower()
        if v in ("oui", "yes", "true", "1"):
            return True
        if v in ("non", "no", "false", "0"):
            return False
    return value


# ---------------------------------------------------------------------------
# Réglages
# ---------------------------------------------------------------------------
API_PREFIX = "/api/v2"
REQUEST_TIMEOUT = 60          # secondes
PAGE_SIZE = 1000              # éléments par page KPI
BACKOFF_MINUTES = 1           # marge pour éviter les “bords” temporels

# Ticket (module plainte/grievance)
try:
    from grievance_social_protection.models import Ticket  # type: ignore
except Exception:  # pragma: no cover
    Ticket = apps.get_model("grievance_social_protection", "Ticket")  # type: ignore

# Location
try:
    from location.models import Location  # type: ignore
except Exception:  # pragma: no cover
    Location = None  # type: ignore


# ---------------------------------------------------------------------------
# Client Kobo KPI v2
# ---------------------------------------------------------------------------
@dataclass
class KoboClient:
    base_url: str
    token: str

    def _headers(self) -> Dict[str, str]:
        return {"Authorization": f"Token {self.token}", "Accept": "application/json"}

    def _abs(self, path: str) -> str:
        return f"{self.base_url.rstrip('/')}{API_PREFIX}/{path.lstrip('/')}"

    def get_json(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        import requests, json as _json
        url = self._abs(path)
        params = dict(params or {})
        params.setdefault("format", "json")
        r = requests.get(url, headers=self._headers(), params=params, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        try:
            return r.json()
        except ValueError:
            text = r.text.lstrip("\ufeff").strip()
            return _json.loads(text)

    def iter_submissions(self, asset_uid: str, page_size: int = PAGE_SIZE) -> Iterable[Dict[str, Any]]:
        """Itère toutes les soumissions pour un asset (UID) avec pagination."""
        import requests
        url = self._abs(f"assets/{asset_uid}/data/")
        params: Optional[Dict[str, Any]] = {"format": "json", "limit": page_size}
        while url:
            r = requests.get(url, headers=self._headers(), params=params, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            payload = r.json()
            for row in payload.get("results", []):
                yield row
            url = payload.get("next")
            params = None  # les URLs "next" sont absolues

    def get_submission(self, asset_uid: str, submission_id: Any) -> Optional[Dict[str, Any]]:
        """Récupère UNE soumission. Selon l'instance, l'endpoint /data/{id}/ peut être disponible."""
        import requests
        url = self._abs(f"assets/{asset_uid}/data/{submission_id}/")
        try:
            r = requests.get(url, headers=self._headers(), params={"format": "json"}, timeout=REQUEST_TIMEOUT)
            if r.status_code == 404:
                return None
            r.raise_for_status()
            return r.json()
        except Exception:
            logger.warning("[KPI] get_submission: endpoint unitaire non disponible")
            return None


# ---------------------------------------------------------------------------
# Utilitaires de dates / booléens / logs / mapping
# ---------------------------------------------------------------------------

def _parse_ts(value: Any) -> Optional[timezone.datetime]:
    if not value:
        return None
    if isinstance(value, timezone.datetime):
        return value if timezone.is_aware(value) else timezone.make_aware(value)
    dt = parse_datetime(str(value))
    if dt is None:
        return None
    return dt if timezone.is_aware(dt) else timezone.make_aware(dt)


def _bool_fr(value: Any) -> Any:
    if isinstance(value, str):
        v = value.strip().lower()
        if v in ("oui", "yes", "true", "1"):
            return True
        if v in ("non", "no", "false", "0"):
            return False
    return value


def should_sync(form: KoboForm) -> bool:
    """Renvoie True si la sync est due en fonction de auto_sync/sync_interval/last_sync_date."""
    if not getattr(form, "auto_sync", False):
        return False
    interval = getattr(form, "sync_interval", None)
    last = getattr(form, "last_sync_date", None)
    if not interval or not isinstance(interval, int) or interval <= 0:
        return True
    if not last:
        return True
    return timezone.now() - last >= timedelta(minutes=interval)


def _get_token(form: KoboForm) -> KoboToken:
    token_obj = getattr(form, "kobo_token", None) or getattr(form, "api_key", None)
    if not isinstance(token_obj, KoboToken):
        raise ValueError("Aucun token n'est associé au KoboForm (champ 'kobo_token' ou 'api_key').")
    return token_obj


def _get_uid(form: KoboForm) -> str:
    uid = getattr(form, "kobo_uid", None)
    if not uid:
        raise ValueError("Le KoboForm n'a pas de 'kobo_uid'.")
    return uid


def _build_mapping(form: KoboForm) -> Tuple[Dict[str, str], List[Tuple[str, str]]]:
    """Retourne: (direct_map, extras_map) à partir de KoboFieldMapping."""
    direct_map: Dict[str, str] = {}
    extras_map: List[Tuple[str, str]] = []
    for m in KoboFieldMapping.objects.filter(kobo_form=form).iterator():
        source = (m.kobo_field or "").strip()
        target = (m.grievance_field or "").strip()
        if not source or not target:
            continue
        if target.startswith("json_ext"):
            extras_map.append((source, target))
        else:
            direct_map[source] = target
    return direct_map, extras_map


def _dot_set(d: Dict[str, Any], dotted: str, value: Any) -> None:
    """Affecte une valeur dans un dict imbriqué via un chemin en pointillés."""
    parts = dotted.split(".")
    cur = d
    for i, p in enumerate(parts):
        if i == len(parts) - 1:
            cur[p] = value
        else:
            if p not in cur or not isinstance(cur[p], dict):
                cur[p] = {}
            cur = cur[p]


def _save_log(form: KoboForm, user, status: str, action: str, message: str = "", details: Any = None) -> KoboSyncLog:
    valid = {"success", "failed"}
    if status not in valid:
        status = "failed" if status in {"error"} else "success"

    log = KoboSyncLog(kobo_form=form)
    for name, val in (
        ("status", status),
        ("action", action),
        ("message", message),
        ("error_message", message),
        ("details", details),
        ("payload", details),
    ):
        if hasattr(log, name) and val is not None:
            setattr(log, name, val)
    try:
        log.save(user=user)
    except TypeError:
        log.save()
    return log


# ---------------------------------------------------------------------------
# Résolution d'entités liées : Location, User, etc.
# ---------------------------------------------------------------------------

def _resolve_location_from_row(row: Dict[str, Any]) -> Optional[Any]:
    """Tente de retrouver une Location à partir de codes ou libellés présents.
    Retourne None si introuvable. N'assigne rien si le modèle ne possède pas le champ.
    """
    if Location is None:
        return None
    try:
        code_keys = [
            "group_geo/group_prefecture/district_code",
            "group_geo/group_prefecture/sous_prefecture_code",
            "group_geo/group_prefecture/prefecture_code",
            "group_geo/group_prefecture/region_code",
        ]
        for k in code_keys:
            v = row.get(k)
            if v:
                obj = Location.objects.filter(code=str(v)).first()
                if obj:
                    return obj
        name_keys = [
            "group_geo/group_prefecture/district",
            "group_geo/group_prefecture/sous_prefecture",
            "group_geo/group_prefecture/prefecture",
            "group_geo/group_prefecture/region",
        ]
        for k in name_keys:
            v = row.get(k)
            if v:
                obj = Location.objects.filter(name__iexact=str(v)).first()
                if obj:
                    return obj
    except Exception:
        logger.debug("_resolve_location_from_row: pas de Location correspondante")
    return None


# ---------------------------------------------------------------------------
# Affectation conditionnelle / détection de changements
# ---------------------------------------------------------------------------

def _assign_if_changed(instance, field_name: str, new_value) -> bool:
    if not hasattr(instance, field_name):
        return False
    cur = getattr(instance, field_name)
    if cur != new_value:
        setattr(instance, field_name, new_value)
        return True
    return False


# ---------------------------------------------------------------------------
# Règles métier supplémentaires (priority / status / date_of_incident)
# ---------------------------------------------------------------------------
import unicodedata

def _strip_accents_lower(s: str) -> str:
    if not isinstance(s, str):
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    return s.lower()


def _infer_priority(row: Dict[str, Any], category_value: Optional[str]) -> Optional[str]:
    """Déduit la priorité à partir de la catégorie (sensible vs non sensible).
    Retourne 'critical' (sensible), 'normal' (non sensible) ou None si indéterminé.
    """
    v = row.get("group_categorie/categories_plainte") or category_value or ""
    v_norm = _strip_accents_lower(str(v))
    if any(k in v_norm for k in ["cas_non_sensible", "non sensible", "non sensibles"]):
        return "Normal"
    if any(k in v_norm for k in ["cas_sensible", "sensible", "sensibles"]):
        return "Critical"
    return None


def _infer_incident_date(row: Dict[str, Any], fallback_dt: Optional[timezone.datetime]) -> Optional[Any]:
    """Tente d'inférer la date de l'incident. Préférence à un champ dédié si présent,
    sinon retombe sur start/end/_submission_time. Retourne un date() ou None.
    """
    keys = [
        # "groupe_detail_plainte/date_incident",
        # "groupe_detail_survivant/date_incident",
        # "date_incident",
        # "current_datetime",
        "start",
        # "end",
        # "_submission_time",
    ]
    for k in keys:
        val = row.get(k)
        dt = _parse_ts(val) if val else None
        if dt:
            return dt.date()
    if fallback_dt:
        return fallback_dt.date()
    return None


def _maybe_set_resolved(ticket) -> bool:
    """Si une résolution est renseignée, bascule le statut en RESOLVED (sauf CLOSED)."""
    try:
        has_resolution = bool(getattr(ticket, "resolution", None))
        cur = getattr(ticket, "status", None)
        closed = getattr(getattr(Ticket, "TicketStatus", None), "CLOSED", "CLOSED")
        resolved = getattr(getattr(Ticket, "TicketStatus", None), "RESOLVED", "RESOLVED")
        if has_resolution and cur != closed and cur != resolved:
            return _assign_if_changed(ticket, "status", resolved)
    except Exception:
        pass
    return False

# ---------------------------------------------------------------------------
# Recherche / Mapping / Upsert
# ---------------------------------------------------------------------------

def _find_existing_ticket(row: Dict[str, Any], direct_map: Dict[str, str]) -> Optional[Any]:
    """Tente de retrouver un Ticket existant par 'code', puis métadonnées Kobo."""
    ref_value = None
    for kobo_key, target in direct_map.items():
        if target == "code" and kobo_key in row:
            ref_value = row[kobo_key]
            break
    if ref_value:
        try:
            obj = Ticket.objects.filter(code=ref_value).first()
            if obj:
                return obj
        except Exception:
            pass

    candidates = []
    if "_uuid" in row and hasattr(Ticket, "kobo_uuid"):
        candidates.append(("kobo_uuid", row["_uuid"]))
    if "meta/instanceID" in row and hasattr(Ticket, "instance_id"):
        candidates.append(("instance_id", row["meta/instanceID"]))

    for field, value in candidates:
        try:
            obj = Ticket.objects.filter(**{field: value}).first()
            if obj:
                return obj
        except Exception:
            continue
    return None


def _apply_mapping(
    row: Dict[str, Any],
    direct_map: Dict[str, str],
    extras_map: List[Tuple[str, str]],
    ticket: Any,
    resolve_label=None,
) -> Tuple[Dict[str, Any], bool]:
    """Applique le mapping et retourne (json_ext, changed)."""
    model_fields = {f.name for f in ticket._meta.get_fields()}
    original_json = deepcopy(getattr(ticket, "json_ext", {}) or {})
    json_ext: Dict[str, Any] = deepcopy(original_json)
    changed = False

    # Champs directs
    for kobo_key, model_field in direct_map.items():
        if kobo_key not in row or model_field not in model_fields:
            continue
        val = row[kobo_key]
        if resolve_label is not None:
            val = resolve_label(kobo_key, val)

        # Parsing dates
        if model_field.endswith(("_at", "_date", "submitted_at")):
            val = _parse_ts(val)

        # Gestion du type du champ cible
        try:
            field_obj = ticket._meta.get_field(model_field)
        except Exception:
            field_obj = None

        # Si la valeur est une liste mais le champ est un CharField → join
        if isinstance(val, list) and isinstance(field_obj, djm.CharField):
            val = ", ".join(map(str, val))

        # Éviter d'assigner des strings sur des FK
        if getattr(getattr(field_obj, "remote_field", None), "model", None) is not None:
            # Laisse une autre logique résoudre les FK si nécessaire
            continue

        if isinstance(val, str):
            val = val.strip()

        changed |= _assign_if_changed(ticket, model_field, val)

    # Extras -> json_ext (toujours stockés, liste conservée)
    for kobo_key, dotted in extras_map:
        if kobo_key not in row:
            continue
        val = row[kobo_key]
        if resolve_label is not None:
            val = resolve_label(kobo_key, val)
        # Normalisations spécifiques
        if isinstance(val, str) and "," in val and dotted.endswith("responsable_plainte"):
            val = [v.strip() for v in val.split(",") if v.strip()]
        val = _coerce_bool_fr(val)
        path = dotted[9:] if dotted.startswith("json_ext.") else dotted
        _dot_set(json_ext, path, val)

    if json_ext != original_json:
        changed = True

    return json_ext, changed


# ---------------------------------------------------------------------------
# API publique : synchronisations
# ---------------------------------------------------------------------------

def start_sync(kobo_form: KoboForm, user=None, since: Optional[timezone.datetime] = None, dry_run: bool = False) -> None:
    """Lance la synchronisation pour un KoboForm."""
    token = _get_token(kobo_form)
    base_url = (token.url_kobo or "").strip().rstrip("/")
    if not base_url:
        raise ValueError("Le KoboToken associé n'a pas d'URL (url_kobo).")

    uid = _get_uid(kobo_form)
    client = KoboClient(base_url=base_url, token=token.api_key)

    # Résolution des labels (select_one / select_multiple)
    LABEL_LANG = "fr"  # ou "French" selon tes assets
    try:
        resolve_label = _build_choice_resolver(client, uid, lang=LABEL_LANG)
    except Exception as e:
        logger.warning("[Kobo Label] Désactivé: %s", e)
        resolve_label = None

    direct_map, extras_map = _build_mapping(kobo_form)

    # borne temporelle (BACKOFF)
    _since = since or getattr(kobo_form, "last_sync_date", None)
    if _since:
        _since = _since - timedelta(minutes=BACKOFF_MINUTES)

    _save_log(kobo_form, user, status="success", action="start", message=f"Début sync UID={uid}")

    created = updated = skipped = failed = 0
    max_submission_ts: Optional[timezone.datetime] = getattr(kobo_form, "last_sync_date", None)

    try:
        for row in client.iter_submissions(uid):
            # Filtrage côté client
            sub_ts = _parse_ts(row.get("_submission_time") or row.get("end") or row.get("start"))
            if _since and sub_ts and sub_ts <= _since:
                skipped += 1
                continue

            if sub_ts and (max_submission_ts is None or sub_ts > max_submission_ts):
                max_submission_ts = sub_ts

            try:
                with transaction.atomic():
                    ticket = _find_existing_ticket(row, direct_map) or Ticket()

                    # (Optionnel) Liaison d'une Location si le modèle possède un champ 'location'
                    loc = _resolve_location_from_row(row)
                    if loc is not None and hasattr(ticket, "location"):
                        _assign_if_changed(ticket, "location", loc)

                    json_ext, changed = _apply_mapping(row, direct_map, extras_map, ticket, resolve_label=resolve_label)

                    # Règles métier: priority / status / date_of_incident
                    try:
                        prio = _infer_priority(row, getattr(ticket, "category", None))
                        if prio:
                            changed |= _assign_if_changed(ticket, "priority", prio)
                    except Exception:
                        pass

                    try:
                        changed |= _maybe_set_resolved(ticket)
                    except Exception:
                        pass

                    try:
                        incident_date = _infer_incident_date(row, sub_ts)
                        if incident_date is not None:
                            changed |= _assign_if_changed(ticket, "date_of_incident", incident_date)
                    except Exception:
                        pass

                    # Champs génériques facultatifs
                    if hasattr(ticket, "json_ext"):
                        if (getattr(ticket, "json_ext", {}) or {}) != json_ext:
                            ticket.json_ext = json_ext
                            changed = True
                    if hasattr(ticket, "submitted_at") and sub_ts:
                        changed |= _assign_if_changed(ticket, "submitted_at", sub_ts)
                    if hasattr(ticket, "source") and not getattr(ticket, "source", None):
                        changed |= _assign_if_changed(ticket, "source", "KOBO")

                    if dry_run:
                        logger.info("[Dry-Run] Ticket simulé (create/update): %s", getattr(ticket, "code", None))
                    else:
                        is_create = ticket.pk is None
                        if is_create or changed:
                            try:
                                # Fixer max_escalation_level selon la catégorie
                                # sensible peut monter jusqu’au national
                                ticket.max_escalation_level = 4 if ticket.priority == "Critical" else 3
                                ticket.save(user=user)
                                bootstrap_escalation_fields(ticket)
                            except TypeError:
                                ticket.save()
                            if is_create:
                                created += 1
                            else:
                                updated += 1
                        else:
                            skipped += 1  # aucun changement → ne pas sauver pour éviter ValidationError

            except ValidationError as ve:
                # HistoryBusinessModel peut lever si aucun changement; on le compte en 'skipped'
                msg = " ".join(map(str, ve.messages))
                if "no changes" in msg.lower():
                    skipped += 1
                else:
                    failed += 1
                    logger.exception("[Kobo Sync] Validation error")
                    _save_log(kobo_form, user, status="failed", action="row_error", message=str(ve), details=row)
            except Exception as inner:
                failed += 1
                logger.exception("[Kobo Sync] Erreur upsert pour une soumission")
                _save_log(kobo_form, user, status="failed", action="row_error", message=str(inner), details=row)

    except Exception as e:
        _save_log(kobo_form, user, status="failed", action="error", message=str(e))
        logger.exception("[Kobo Sync] Erreur pendant la collecte des soumissions")
        return

    # Mise à jour du pointeur temporel
    setattr(kobo_form, "last_sync_date", max_submission_ts or timezone.now())
    try:
        kobo_form.save(user=user)
    except TypeError:
        kobo_form.save()

    _save_log(
        kobo_form,
        user,
        status="success",
        action="end",
        message=f"Terminé: created={created}, updated={updated}, skipped={skipped}, failed={failed}",
        details={"created": created, "updated": updated, "skipped": skipped, "failed": failed},
    )


def sync_all_kobo_forms() -> None:
    """Appelée par un scheduler : exécute start_sync() pour les formulaires éligibles."""
    for form in KoboForm.objects.all().iterator():
        try:
            if not getattr(form, "auto_sync", False):
                continue
            if not should_sync(form):
                continue
            user = getattr(form, "user", None)
            logger.info(f"[Scheduler] Sync {form} (UID={getattr(form, 'kobo_uid', '?')})")
            start_sync(form, user=user)
        except Exception as e:
            logger.error(f"[Scheduler] Erreur de sync pour {form}: {e}")


# ---------------------------------------------------------------------------
# Fonctions utilitaires supplémentaires
# ---------------------------------------------------------------------------

def sync_since(kobo_form: KoboForm, minutes: int, user=None) -> None:
    """Synchronise uniquement les soumissions des X dernières minutes."""
    since = timezone.now() - timedelta(minutes=minutes)
    start_sync(kobo_form, user=user, since=since)


def sync_one(kobo_form: KoboForm, submission_id: Any, user=None, dry_run: bool = False) -> Optional[Any]:
    """Synchronise UNE soumission par identifiant externe (si l'endpoint unitaire est dispo)."""
    token = _get_token(kobo_form)
    base_url = (token.url_kobo or "").strip().rstrip("/")
    uid = _get_uid(kobo_form)
    client = KoboClient(base_url=base_url, token=token.api_key)

    # Résolution des labels (select_one / select_multiple)
    LABEL_LANG = "fr"
    try:
        resolve_label = _build_choice_resolver(client, uid, lang=LABEL_LANG)
    except Exception as e:
        logger.warning("[Kobo Label] Désactivé: %s", e)
        resolve_label = None

    row = client.get_submission(uid, submission_id)
    if not row:
        logger.warning("[sync_one] Soumission introuvable: %s", submission_id)
        return None

    direct_map, extras_map = _build_mapping(kobo_form)
    sub_ts = _parse_ts(row.get("_submission_time") or row.get("end") or row.get("start"))

    with transaction.atomic():
        ticket = _find_existing_ticket(row, direct_map) or Ticket()
        loc = _resolve_location_from_row(row)
        if loc is not None and hasattr(ticket, "location"):
            _assign_if_changed(ticket, "location", loc)

        json_ext, changed = _apply_mapping(row, direct_map, extras_map, ticket, resolve_label=resolve_label)

        # Règles métier: priority / status / date_of_incident
        try:
            prio = _infer_priority(row, getattr(ticket, "category", None))
            if prio:
                changed |= _assign_if_changed(ticket, "priority", prio)
        except Exception:
            pass

        try:
            changed |= _maybe_set_resolved(ticket)
        except Exception:
            pass

        try:
            incident_date = _infer_incident_date(row, sub_ts)
            if incident_date is not None:
                changed |= _assign_if_changed(ticket, "date_of_incident", incident_date)
        except Exception:
            pass

        if hasattr(ticket, "json_ext"):
            if (getattr(ticket, "json_ext", {}) or {}) != json_ext:
                ticket.json_ext = json_ext
                changed = True
        if hasattr(ticket, "submitted_at") and sub_ts:
            changed |= _assign_if_changed(ticket, "submitted_at", sub_ts)
        if hasattr(ticket, "source") and not getattr(ticket, "source", None):
            changed |= _assign_if_changed(ticket, "source", "KOBO")

        if dry_run:
            logger.info("[Dry-Run] sync_one simulé pour submission_id=%s", submission_id)
            return ticket

        is_create = ticket.pk is None
        if is_create or changed:
            try:
                if not ticket.due_date:
                    ticket.due_date = next_due_date(0)  # SLA du niveau local

                # Fixer max_escalation_level selon la catégorie
                # sensible peut monter jusqu’au national
                # ticket.max_escalation_level = 4 if ticket.priority == "Critical" else 3
                ticket.save(user=user)
                bootstrap_escalation_fields(ticket)
            except TypeError:
                ticket.save()
            # Journaliser la sync unitaire
            _save_log(
                kobo_form,
                user,
                status="success",
                action="single",
                message=f"sync_one: submission={submission_id}, created={is_create}",
                details=row,
            )
        else:
            logger.info("[sync_one] Aucun changement pour submission_id=%s", submission_id)
        return ticket

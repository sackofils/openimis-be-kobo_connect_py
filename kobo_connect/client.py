import json
import logging
import re
import unicodedata
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional

import requests


logger = logging.getLogger(__name__)

API_PREFIX = "/api/v2"
REQUEST_TIMEOUT = 60
PAGE_SIZE = 1000


def _normalize_language(value):
    normalized = unicodedata.normalize("NFKD", value)
    normalized = "".join(
        character for character in normalized
        if not unicodedata.combining(character)
    )
    return re.sub(r"[\s()]+", "", normalized).lower()


def _pick_language(label, preferred_language):
    if isinstance(label, str):
        return label
    if isinstance(label, dict) and label:
        if preferred_language:
            preferred = _normalize_language(preferred_language)
            for language, value in label.items():
                if _normalize_language(language) == preferred:
                    return value
        return next(iter(label.values()))
    if isinstance(label, list) and label:
        first = label[0]
        if isinstance(first, dict):
            if preferred_language:
                preferred = _normalize_language(preferred_language)
                for item in label:
                    if _normalize_language(str(item.get("lang", ""))) == preferred:
                        return item.get("text") or item
            return first.get("text") or first
        return first
    return None


def build_choice_resolver(client, asset_uid, language=None):
    asset = client.get_json(f"assets/{asset_uid}/")
    content = asset.get("content") or {}
    survey = content.get("survey") or []
    raw_choices = content.get("choices") or []

    if isinstance(raw_choices, dict):
        choices_by_list = raw_choices
    else:
        choices_by_list: Dict[str, List[Dict[str, Any]]] = {}
        for choice in raw_choices:
            list_name = choice.get("list_name")
            if list_name:
                choices_by_list.setdefault(list_name, []).append(choice)

    labels_by_list = {}
    for list_name, items in choices_by_list.items():
        labels_by_list[list_name] = {
            str(choice["name"]): str(label)
            for choice in items or []
            if choice.get("name") is not None
            if (label := _pick_language(choice.get("label"), language)) is not None
        }

    field_to_list = {}
    for question in survey:
        question_name = question.get("name")
        question_type = (question.get("type") or "").strip()
        list_name = question.get("select_from_list_name")
        if not list_name and (
            question_type.startswith("select_one")
            or question_type.startswith("select_multiple")
        ):
            parts = question_type.split()
            list_name = parts[1] if len(parts) > 1 else None
        if question_name and list_name:
            field_to_list[question_name] = list_name

    def resolve(kobo_key, raw_value):
        list_name = field_to_list.get(kobo_key.split("/")[-1])
        if not list_name:
            return raw_value
        labels = labels_by_list.get(list_name, {})
        if isinstance(raw_value, list):
            codes = raw_value
        elif isinstance(raw_value, str):
            codes = [code for code in re.split(r"[,\s]+", raw_value.strip()) if code]
        else:
            return labels.get(str(raw_value), raw_value)
        return [labels.get(str(code), code) for code in codes]

    return resolve


def coerce_boolean(value):
    if isinstance(value, str):
        normalized = value.strip().casefold()
        if normalized in ("oui", "yes", "true", "1"):
            return True
        if normalized in ("non", "no", "false", "0"):
            return False
    return value


@dataclass
class KoboClient:
    base_url: str
    token: str

    def _headers(self):
        return {
            "Authorization": f"Token {self.token}",
            "Accept": "application/json",
        }

    def _absolute_url(self, path):
        return f"{self.base_url.rstrip('/')}{API_PREFIX}/{path.lstrip('/')}"

    def get_json(self, path, params=None):
        parameters = dict(params or {})
        parameters.setdefault("format", "json")
        response = requests.get(
            self._absolute_url(path),
            headers=self._headers(),
            params=parameters,
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        try:
            return response.json()
        except ValueError:
            return json.loads(response.text.lstrip("\ufeff").strip())

    def iter_submissions(self, asset_uid, page_size=PAGE_SIZE) -> Iterable[Dict[str, Any]]:
        url = self._absolute_url(f"assets/{asset_uid}/data/")
        params = {"format": "json", "limit": page_size}
        while url:
            response = requests.get(
                url,
                headers=self._headers(),
                params=params,
                timeout=REQUEST_TIMEOUT,
            )
            response.raise_for_status()
            payload = response.json()
            yield from payload.get("results", [])
            url = payload.get("next")
            params = None

    def get_submission(self, asset_uid, submission_id) -> Optional[Dict[str, Any]]:
        response = requests.get(
            self._absolute_url(f"assets/{asset_uid}/data/{submission_id}/"),
            headers=self._headers(),
            params={"format": "json"},
            timeout=REQUEST_TIMEOUT,
        )
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return response.json()

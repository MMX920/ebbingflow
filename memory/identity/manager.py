"""Persona manager backed by Neo4j."""
import json
import logging
import re
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from neo4j import AsyncGraphDatabase

from config import identity_config, neo4j_config
from memory.event.slots import ObservationCategory, PersonaObservation
from memory.identity.canonical import canonicalize_entity
from memory.identity.schema import UserProfile

logger = logging.getLogger(__name__)

ASSISTANT_ROLE_REWRITE_FIELDS = {
    "name",
    "role",
    "age",
    "gender",
    "persona",
    "relationship_to_user",
}

ASSISTANT_ROLE_CANDIDATES = [
    "secretary",
    "butler",
    "assistant",
    "companion",
    "\u79c1\u4eba\u52a9\u7406",
    "\u52a9\u7406",
    "\u7ba1\u5bb6",
    "\u79d8\u4e66",
]


ASSISTANT_STYLE_HINTS = [
    "gentle",
    "calm",
    "charming",
    "warm",
]

USER_ROLE_CANDIDATES = [
    "engineer",
    "developer",
    "designer",
    "product manager",
    "student",
    "teacher",
    "manager",
]

USER_PERSONALITY_MARKERS = ["I", "E"]

VALID_MBTI_LABELS = {
    "INTJ", "INTP", "INFJ", "INFP", "ISTJ", "ISTP", "ISFJ", "ISFP",
    "ENTJ", "ENTP", "ENFJ", "ENFP", "ESTJ", "ESTP", "ESFJ", "ESFP",
    "I", "E",
}

PERSONALITY_SOURCE_WEIGHTS = {
    "self_report": 1.2,
    "direct_user": 1.15,
    "behavior_observation": 1.08,
    "observation_extractor": 1.0,
    "episode_rollup": 0.95,
    "reported_by_others": 0.82,
    "fallback": 0.72,
}

NOISE_TEXT_HINTS_STRONG = (
    "just kidding",
    "kidding",
    "joke",
    "sarcasm",
    "\u5f00\u73a9\u7b11",
    "\u73a9\u7b11",
    "\u6545\u610f",
    "\u8bd5\u63a2",
    "\u53cd\u8bdd",
)

NOISE_TEXT_HINTS_WEAK = (
    "maybe",
    "probably",
    "guess",
    "\u53ef\u80fd",
    "\u5927\u6982",
    "\u4e5f\u8bb8",
    "\u731c",
)


def _normalize_relationship_to_user(value: str, role_hint: str = "") -> str:
    raw = str(value or "").strip().lower()
    if not raw:
        return "assistant_to_user" if role_hint else ""
    # Backward-compat: legacy values like user_assistant / user_私人助理.
    if raw.startswith("user_") or raw in {"userassistant", "user_assistant"}:
        return "assistant_to_user"
    if raw in {"assistant_to_user", "assistant-user", "assistanttouser"}:
        return "assistant_to_user"
    return "assistant_to_user"


def _node_primary_name(node, fallback: str = "") -> str:
    if not node:
        return fallback
    return (
        node.get("primary_name")
        or node.get("name")
        or fallback
    )


def _extract_demographic_updates(text: str) -> dict:
    if not text:
        return {}

    raw = str(text).strip()
    updates = {}

    for pattern in [r"age[:\s]*(\d{1,3})", r"(\d{1,3})\s*years?\s*old"]:
        match = re.search(pattern, raw, re.IGNORECASE)
        if match:
            updates["age"] = match.group(1)
            break

    gender_map = {"female": "female", "woman": "female", "male": "male", "man": "male"}
    for token, normalized in gender_map.items():
        if re.search(rf"\b{re.escape(token)}\b", raw, re.IGNORECASE):
            updates["gender"] = normalized
            break

    for pattern in [
        r"(?:occupation|job|role)[:\s]+([A-Za-z][A-Za-z0-9 _-]{1,40})",
        r"(?:i am|i'm|working as)\s+(?:a|an)?\s*([A-Za-z][A-Za-z0-9 _-]{1,40})",
    ]:
        match = re.search(pattern, raw, re.IGNORECASE)
        if match:
            value = match.group(1).strip()
            if value and value.upper() not in {"I", "E"}:
                updates["occupation"] = value
                break

    return updates


def _extract_user_biography_hint(text: str, occupation: str = "") -> str:
    raw = str(text or "").strip()
    if not raw:
        return ""

    raw = str(text or "").strip()
    if not raw:
        return ""

    if occupation:
        value = str(occupation).strip()
        if value in USER_PERSONALITY_MARKERS:
            return ""
        return value

    patterns = [
        r"(?:i am|i'm)\s+(?:a|an)?\s*([A-Za-z][A-Za-z0-9 _-]{2,40})",
        r"(?:my occupation is|my job is|my role is)\s*([A-Za-z][A-Za-z0-9 _-]{2,40})",
    ]
    for pattern in patterns:
        match = re.search(pattern, raw, re.IGNORECASE)
        if match:
            value = str(match.group(1) or "").strip().strip(",.;!? ")
            if value and value not in USER_PERSONALITY_MARKERS:
                return value
    return ""


def _extract_user_personality_marker(text: str) -> dict:
    raw = str(text or "").strip()
    if not raw:
        return {}

    marker = ""
    upper = raw.upper()
    for item in USER_PERSONALITY_MARKERS:
        if item in upper:
            marker = item
            break
    if not marker:
        return {}

    by_others = bool(re.search(r"\b(they say|others say|people say)\b", raw, re.IGNORECASE))
    mbti_label = "I" if marker == "I" else "E"
    if by_others:
        extraversion = 0.45 if marker == "I" else 0.55
    else:
        extraversion = 0.32 if marker == "I" else 0.68
    return {
        "confidence": 0.72 if by_others else 0.88,
        "evidence": "reported_by_others" if by_others else "self_report",
        "mbti_label": mbti_label,
        "big_five": {"extraversion": extraversion},
    }


def _extract_user_social_personality_hint(text: str) -> dict:
    raw = str(text or "").strip()
    if not raw:
        return {}

    introvert_patterns = [
        r"\bintrovert\b",
        r"\bquiet\b",
        r"\bprefer to be alone\b",
        r"\bdo not like socializing\b",
    ]
    extrovert_patterns = [
        r"\bextrovert\b",
        r"\boutgoing\b",
        r"\blike socializing\b",
        r"\blove socializing\b",
    ]

    matched_i = any(re.search(p, raw) for p in introvert_patterns)
    matched_e = any(re.search(p, raw) for p in extrovert_patterns)
    if not matched_i and not matched_e:
        return {}

    # Conflicting sentence fallback: do nothing.
    if matched_i and matched_e:
        return {}

    if matched_i:
        return {
            "confidence": 0.84,
            "evidence": "self_report",
            "mbti_label": "I",
            "big_five": {"extraversion": 0.30},
        }

    return {
        "confidence": 0.84,
        "evidence": "self_report",
        "mbti_label": "E",
        "big_five": {"extraversion": 0.70},
    }


def _infer_user_role(*texts: str) -> str:
    for raw in texts:
        text = str(raw or "").strip()
        if not text:
            continue
        for role in USER_ROLE_CANDIDATES:
            if role in text:
                return role
    return ""


def _normalize_assistant_gender(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""

    female_tokens = ["female", "woman", "girl"]
    male_tokens = ["male", "man", "boy"]

    for token in female_tokens:
        if re.search(rf"\b{re.escape(token)}\b", raw, re.IGNORECASE):
            return "female"
    for token in male_tokens:
        if re.search(rf"\b{re.escape(token)}\b", raw, re.IGNORECASE):
            return "male"
    return ""


def _infer_assistant_role(*texts: str) -> str:
    zh_role_map = {
        "\u79c1\u4eba\u52a9\u7406": "\u79c1\u4eba\u52a9\u7406",
        "\u52a9\u7406": "\u52a9\u7406",
        "\u7ba1\u5bb6": "\u7ba1\u5bb6",
        "\u79d8\u4e66": "\u79d8\u4e66",
        "\u966a\u4f34": "\u966a\u4f34",
    }
    for raw in texts:
        text = str(raw or "").strip()
        if not text:
            continue
        for token, role in zh_role_map.items():
            if token in text:
                return role
        for role in ASSISTANT_ROLE_CANDIDATES:
            if role in text:
                return role
    return ""


def _looks_like_role_label(text: str) -> bool:
    raw = str(text or "").strip()
    if not raw:
        return False
    if _infer_assistant_role(raw):
        return True
    return raw.lower() in {"assistant", "ai", "secretary", "butler", "companion"}


def _looks_like_style_descriptor(text: str) -> bool:
    raw = str(text or "").strip()
    if not raw:
        return False
    if any(hint in raw for hint in ASSISTANT_STYLE_HINTS):
        return True
    if raw.lower().endswith("style"):
        return True
    return False


def _sanitize_assistant_role_updates(raw_updates: dict) -> dict:
    if not raw_updates:
        return {}

    alias_map = {
        "persona_summary": "persona",
        "personality_summary": "persona",
        "profile": "persona",
        "bio": "persona",
    }
    updates = {}
    for key, value in (raw_updates or {}).items():
        if value in (None, ""):
            continue
        normalized_key = alias_map.get(str(key).strip(), str(key).strip())
        if normalized_key not in ASSISTANT_ROLE_REWRITE_FIELDS:
            continue
        text = str(value).strip().strip(",.;!?")
        if not text:
            continue
        if normalized_key == "age":
            match = re.search(r"(\d{1,3})", text)
            if match:
                updates["age"] = match.group(1)
            continue
        if normalized_key == "gender":
            gender = _normalize_assistant_gender(text)
            if gender:
                updates["gender"] = gender
            continue
        if normalized_key == "role":
            text = re.sub(r"^(?:my|be my|as my|become my)\s+", "", text, flags=re.IGNORECASE).strip()
        updates[normalized_key] = text

    inferred_role = updates.get("role") or _infer_assistant_role(
        updates.get("relationship_to_user"),
        updates.get("persona"),
    )
    if inferred_role:
        updates["role"] = inferred_role

    if updates.get("role") and not updates.get("persona"):
        updates["persona"] = updates["role"]
    if updates.get("relationship_to_user") or updates.get("role"):
        updates["relationship_to_user"] = _normalize_relationship_to_user(
            updates.get("relationship_to_user"),
            updates.get("role", ""),
        )

    return updates


def extract_assistant_role_rewrite(text: str) -> dict:
    raw = str(text or "").strip()
    if not raw:
        return {}

    updates: dict = {}

    name_match = re.search(r"(?:your name is|call yourself)\s+([A-Za-z][A-Za-z0-9_-]{1,20})", raw, re.IGNORECASE)
    if name_match:
        updates["name"] = name_match.group(1)

    age_match = re.search(r"\b(\d{1,3})\s*years?\s*old\b", raw, re.IGNORECASE)
    if age_match:
        updates["age"] = age_match.group(1)

    gender = _normalize_assistant_gender(raw)
    if gender:
        updates["gender"] = gender

    detected_role = _infer_assistant_role(raw)
    if detected_role:
        updates["role"] = detected_role
        updates.setdefault("persona", detected_role)
        updates.setdefault("relationship_to_user", "assistant_to_user")

    # 中文角色改写：如“以后你是我的私人助理 / 你当我的管家”
    zh_role_match = re.search(
        r"(?:\u4f60\u662f|\u4f60\u5f53|\u4ee5\u540e\u4f60\u662f\u6211\u7684?|\u4ece\u4eca\u5f80\u540e\u4f60\u662f\u6211\u7684?)\s*([^\s\uff0c\u3002?!\uff01\uff1f]{1,12})",
        raw,
    )
    if zh_role_match:
        role_text = str(zh_role_match.group(1) or "").strip()
        normalized = _infer_assistant_role(role_text) or role_text
        if normalized:
            updates["role"] = normalized
            updates.setdefault("persona", normalized)
            updates.setdefault("relationship_to_user", "assistant_to_user")

    return _sanitize_assistant_role_updates(updates)


def extract_user_self_name(text: str) -> str:
    raw = str(text or "").strip()
    if not raw:
        return ""
    patterns = [
        r"(?:my name is|i am)\s+([A-Za-z][A-Za-z0-9_-]{1,24})",
    ]
    for pattern in patterns:
        match = re.search(pattern, raw, re.IGNORECASE)
        if match:
            return str(match.group(1) or "").strip()
    return ""


def extract_user_profile_rewrite(text: str) -> dict:
    raw = str(text or "").strip()
    if not raw or not _is_user_self_profile_statement(raw):
        return {}

    updates = _extract_demographic_updates(raw)
    user_name = extract_user_self_name(raw)
    if user_name:
        updates["name"] = user_name
        updates["primary_name"] = user_name

    inferred_role = _infer_user_role(raw, updates.get("role"))
    if inferred_role:
        updates["role"] = inferred_role

    biography = _extract_user_biography_hint(raw, updates.get("occupation") or "")
    if biography:
        updates["biography"] = biography

    personality_hint = _extract_user_personality_marker(raw) or _extract_user_social_personality_hint(raw)
    if personality_hint:
        if personality_hint.get("mbti_label"):
            updates["mbti_label"] = personality_hint.get("mbti_label")
        if personality_hint.get("big_five"):
            updates["big_five"] = personality_hint.get("big_five")

    return updates


def _is_user_self_profile_statement(text: str) -> bool:
    """Return True when text looks like a self-profile statement."""
    raw = str(text or "").strip()
    if not raw:
        return False
    patterns = [
        r"\b(i am|i'm|my name is|my job is|my role is|age)\b",
        r"(\u6211\u53eb|\u6211\u662f|\u6211\u540d\u5b57\u662f|\u6211\u7684\u804c\u4e1a\u662f|\u6211\u4eca\u5e74)",
    ]
    return any(re.search(p, raw, re.IGNORECASE) for p in patterns)




def extract_external_entity_names(text: str) -> List[str]:
    raw = str(text or "").strip()
    if not raw:
        return []

    patterns = [
        r"(?:my friend|my colleague|my boss)\s+(?:is|named)\s+([A-Za-z][A-Za-z0-9_-]{1,24})",
    ]
    names: List[str] = []
    for pattern in patterns:
        for match in re.finditer(pattern, raw, re.IGNORECASE):
            candidate = str(match.group(1) or "").strip()
            if candidate and candidate not in names:
                names.append(candidate)
    return names


class PersonaManager:
    def __init__(self):
        self._driver = AsyncGraphDatabase.driver(
            uri=neo4j_config.uri,
            auth=(neo4j_config.username, neo4j_config.password),
        )
        self.database = neo4j_config.database

    async def close(self):
        await self._driver.close()

    def _canonicalize(self, name: str) -> str:
        return canonicalize_entity(name)

    async def _merge_name_update(self, session, match_clause: str, params: dict, new_name: str, entity_key: str = "e") -> dict:
        clean_name = str(new_name or "").strip()
        if not clean_name:
            return {}
        res = await session.run(
            f"MATCH ({entity_key}:Entity {{ {match_clause} }}) RETURN COALESCE({entity_key}.primary_name, {entity_key}.name) AS current_name, COALESCE({entity_key}.aliases, []) AS aliases",
            **params,
        )
        record = await res.single()
        current_name = str((record or {}).get("current_name") or "").strip()
        aliases = list((record or {}).get("aliases") or [])
        if current_name and current_name != clean_name and current_name not in aliases:
            aliases.append(current_name)
        return {
            "name": clean_name,
            "primary_name": clean_name,
            "aliases": aliases,
        }

    async def apply_user_profile_rewrite(self, uid: str, updates: dict) -> dict:
        safe_updates = dict(updates or {})
        if not safe_updates:
            return {}

        await self.bootstrap_genesis_identities(uid)
        persisted = {}

        async with self._driver.session(database=self.database) as session:
            if safe_updates.get("name"):
                name_updates = await self._merge_name_update(
                    session,
                    "entity_id: $uid, owner_id: $uid",
                    {"uid": uid},
                    safe_updates["name"],
                    entity_key="u",
                )
                persisted.update(name_updates)

            for key in ("age", "gender", "role"):
                value = str(safe_updates.get(key) or "").strip()
                if value:
                    persisted[key] = value

            occupation = str(safe_updates.get("occupation") or "").strip()
            if occupation:
                persisted["occupation"] = occupation

            biography = str(safe_updates.get("biography") or "").strip()
            if biography:
                persisted["biography"] = biography

            mbti_label = str(safe_updates.get("mbti_label") or "").strip().upper()
            if mbti_label in VALID_MBTI_LABELS:
                persisted["mbti_label"] = mbti_label

            if not persisted:
                persisted = {}

            now = datetime.now().isoformat()
            if persisted:
                set_clause = ", ".join([f"u.{key} = ${key}" for key in persisted.keys()])
                await session.run(
                    f"MATCH (u:Entity {{entity_id: $uid, owner_id: $uid}}) SET {set_clause}, u.persona_updated_at = $now",
                    uid=uid,
                    now=now,
                    **persisted,
                )

            # Cleanup legacy dirty bio values that should live in MBTI, not biography.
            if safe_updates.get("mbti_label") and not safe_updates.get("biography"):
                await session.run(
                    """
                    MATCH (u:Entity {entity_id: $uid, owner_id: $uid})
                    WHERE u.biography IN ['I', 'E']
                    SET u.biography = '', u.persona_updated_at = $now
                    """,
                    uid=uid,
                    now=now,
                )

            big_five = safe_updates.get("big_five") or {}
            if isinstance(big_five, dict):
                ext = big_five.get("extraversion")
                if ext is not None:
                    try:
                        ext_value = max(0.0, min(1.0, float(ext)))
                        await session.run(
                            """
                            MATCH (u:Entity {entity_id: $uid, owner_id: $uid})
                            SET u.big_five_extraversion = $ext, u.persona_updated_at = $now
                            """,
                            uid=uid,
                            ext=ext_value,
                            now=now,
                        )
                    except (TypeError, ValueError):
                        pass

        return persisted

    @staticmethod
    def _clamp01(value: Any, default: float = 0.5) -> float:
        try:
            return max(0.0, min(1.0, float(value)))
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _parse_dt(value: Any) -> Optional[datetime]:
        text = str(value or "").strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            return datetime.fromisoformat(text)
        except ValueError:
            pass
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                return datetime.strptime(text, fmt)
            except ValueError:
                continue
        return None

    @staticmethod
    def _normalize_discrete_value(value: Any) -> str:
        text = str(value or "").strip().lower()
        return re.sub(r"\s+", " ", text)

    @staticmethod
    def _is_short_term_behavior_category(value: Any) -> bool:
        if isinstance(value, ObservationCategory):
            category = value.value
        else:
            category = str(value or "").strip().lower()
        return category == ObservationCategory.SHORT_TERM_EFSTB.value

    @staticmethod
    def _is_numeric_predicate(predicate: str) -> bool:
        p = str(predicate or "").strip().lower()
        return p.startswith("big_five_") or p.startswith("efstb_")

    @classmethod
    def _estimate_noise_penalty(cls, evidence_text: str) -> float:
        text = str(evidence_text or "").strip().lower()
        if not text:
            return 1.0
        if any(hint in text for hint in NOISE_TEXT_HINTS_STRONG):
            return 0.55
        if any(hint in text for hint in NOISE_TEXT_HINTS_WEAK):
            return 0.82
        return 1.0

    @classmethod
    def _estimate_source_weight(cls, source: str, evidence_text: str) -> float:
        src = str(source or "").strip().lower()
        if not src:
            txt = str(evidence_text or "").lower()
            if "reported_by_others" in txt:
                src = "reported_by_others"
            elif "self_report" in txt:
                src = "self_report"
        return PERSONALITY_SOURCE_WEIGHTS.get(src, 1.0)

    @classmethod
    def _score_evidence_row(
        cls,
        row: Dict[str, Any],
        now: Optional[datetime] = None,
        half_life_days: float = 45.0,
    ) -> Dict[str, Any]:
        now_dt = now or datetime.now()
        confidence = cls._clamp01(row.get("confidence"), default=0.5)
        source_weight = cls._estimate_source_weight(row.get("source"), row.get("evidence"))
        noise_penalty = cls._estimate_noise_penalty(row.get("evidence"))
        created_dt = cls._parse_dt(row.get("created_at") or row.get("updated_at") or row.get("time"))

        if created_dt is None:
            time_decay = 0.9
        else:
            age_days = max(0.0, (now_dt - created_dt).total_seconds() / 86400.0)
            hl = max(1.0, float(half_life_days or 45.0))
            time_decay = 0.5 ** (age_days / hl)

        base_score = confidence * source_weight * time_decay * noise_penalty
        scored = dict(row)
        scored["confidence"] = confidence
        scored["source_weight"] = round(source_weight, 4)
        scored["time_decay"] = round(time_decay, 4)
        scored["noise_penalty"] = round(noise_penalty, 4)
        scored["conflict_penalty"] = 1.0
        scored["base_score"] = round(base_score, 6)
        scored["effective_score"] = round(base_score, 6)
        return scored

    @classmethod
    def _apply_conflict_denoise(cls, scored_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        groups: Dict[str, List[Dict[str, Any]]] = {}
        for row in scored_rows:
            predicate = str(row.get("predicate") or "").strip().lower()
            if not predicate:
                continue
            groups.setdefault(predicate, []).append(row)

        conflict_groups = 0
        disputed_predicates: List[str] = []

        for predicate, rows in groups.items():
            if len(rows) <= 1:
                continue

            numeric_mode = cls._is_numeric_predicate(predicate)
            if numeric_mode:
                numeric_items: List[tuple[Dict[str, Any], float]] = []
                for row in rows:
                    try:
                        numeric_items.append((row, float(row.get("object"))))
                    except (TypeError, ValueError):
                        continue
                if len(numeric_items) >= 2:
                    total_weight = sum(max(float(item[0].get("base_score") or 0.0), 1e-6) for item in numeric_items)
                    center = sum(
                        value * max(float(item.get("base_score") or 0.0), 1e-6)
                        for item, value in numeric_items
                    ) / max(total_weight, 1e-6)
                    spread = max(value for _, value in numeric_items) - min(value for _, value in numeric_items)
                    if spread >= 0.25:
                        conflict_groups += 1
                        disputed_predicates.append(predicate)
                    for row, value in numeric_items:
                        distance = abs(value - center)
                        if distance <= 0.12:
                            penalty = 1.0
                        elif distance <= 0.28:
                            penalty = 0.85
                        elif distance <= 0.45:
                            penalty = 0.65
                        else:
                            penalty = 0.45
                        row["conflict_penalty"] = min(float(row.get("conflict_penalty", 1.0)), penalty)
                continue

            bucket_scores: Dict[str, float] = {}
            row_bucket_keys: Dict[int, str] = {}
            for row in rows:
                key = cls._normalize_discrete_value(row.get("object"))
                row_bucket_keys[id(row)] = key
                bucket_scores[key] = bucket_scores.get(key, 0.0) + float(row.get("base_score") or 0.0)

            if len(bucket_scores) <= 1:
                continue

            conflict_groups += 1
            disputed_predicates.append(predicate)
            winner_key = max(bucket_scores.items(), key=lambda item: item[1])[0]

            for row in rows:
                key = row_bucket_keys.get(id(row), "")
                if key == winner_key:
                    continue
                penalty = 0.45 if predicate == "mbti_label" else 0.55
                row["conflict_penalty"] = min(float(row.get("conflict_penalty", 1.0)), penalty)

        for row in scored_rows:
            effective = float(row.get("base_score") or 0.0) * float(row.get("conflict_penalty") or 1.0)
            row["effective_score"] = round(max(0.0, min(1.0, effective)), 6)

        return {
            "conflict_groups": conflict_groups,
            "disputed_predicates": disputed_predicates[:12],
            "group_count": len(groups),
        }

    @classmethod
    def _settle_rows(cls, rows: List[Dict[str, Any]], lookback_days: int = 30) -> Dict[str, Any]:
        now_dt = datetime.now()
        lb_days = max(1, int(lookback_days or 30))
        cutoff = now_dt - timedelta(days=lb_days)

        scored_rows = [cls._score_evidence_row(row, now=now_dt) for row in rows]
        considered = [
            row
            for row in scored_rows
            if str(row.get("predicate") or "").strip()
                    and not cls._is_short_term_behavior_category(row.get("category"))
                    and not str(row.get("predicate") or "").strip().lower().startswith("efstb_")
        ]
        if not considered:
            considered = scored_rows

        conflict_meta = cls._apply_conflict_denoise(considered)

        sorted_rows = sorted(
            considered,
            key=lambda row: (
                float(row.get("effective_score") or 0.0),
                float(row.get("confidence") or 0.0),
                str(row.get("created_at") or ""),
            ),
            reverse=True,
        )

        selected_rows: List[Dict[str, Any]] = []
        per_predicate_count: Dict[str, int] = {}
        for row in sorted_rows:
            predicate = str(row.get("predicate") or "").strip().lower() or "_unknown"
            if per_predicate_count.get(predicate, 0) >= 3:
                continue
            if float(row.get("effective_score") or 0.0) < 0.32:
                continue
            selected_rows.append(row)
            per_predicate_count[predicate] = per_predicate_count.get(predicate, 0) + 1
            if len(selected_rows) >= 12:
                break

        if not selected_rows and sorted_rows:
            selected_rows = sorted_rows[: min(5, len(sorted_rows))]

        selected_refs = [str(row.get("evidence_id") or "").strip() for row in selected_rows if str(row.get("evidence_id") or "").strip()]
        total = len(scored_rows)
        recent_count = 0
        for row in scored_rows:
            dt = cls._parse_dt(row.get("created_at") or row.get("updated_at") or row.get("time"))
            if dt and dt >= cutoff:
                recent_count += 1

        avg_confidence = (
            sum(float(row.get("confidence") or 0.0) for row in scored_rows) / max(total, 1)
            if scored_rows
            else 0.0
        )
        avg_effective = (
            sum(float(row.get("effective_score") or 0.0) for row in considered) / max(len(considered), 1)
            if considered
            else 0.0
        )
        weak_count = sum(1 for row in considered if float(row.get("effective_score") or 0.0) < 0.35)

        top_evidence = []
        for row in selected_rows[:8]:
            top_evidence.append(
                {
                    "id": row.get("evidence_id"),
                    "predicate": row.get("predicate"),
                    "object": row.get("object"),
                    "score": round(float(row.get("effective_score") or 0.0), 4),
                    "confidence": round(float(row.get("confidence") or 0.0), 4),
                    "source": row.get("source") or "",
                    "created_at": row.get("created_at") or row.get("updated_at") or "",
                }
            )

        quality_index = round(max(0.0, min(1.0, avg_effective)), 4)
        return {
            "total": total,
            "considered_count": len(considered),
            "recent_count": recent_count,
            "weak_count": weak_count,
            "avg_confidence": round(avg_confidence, 4),
            "avg_effective": round(avg_effective, 4),
            "quality_index": quality_index,
            "conflict_groups": int(conflict_meta.get("conflict_groups") or 0),
            "disputed_predicates": list(conflict_meta.get("disputed_predicates") or []),
            "selected_count": len(selected_refs),
            "selected_refs": selected_refs,
            "top_evidence": top_evidence,
            "lookback_days": lb_days,
        }

    @staticmethod
    def _build_personality_evidence_rows(obs: PersonaObservation) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        now = datetime.now().isoformat()
        category = str(obs.category.value if hasattr(obs.category, "value") else obs.category or "").strip()
        source = "observation_extractor"
        confidence = float(obs.confidence or 0.8)
        evidence_text = str(obs.evidence_summary or obs.content or "").strip()

        if obs.category == ObservationCategory.LONG_TERM_PERSONA:
            for key, value in (obs.big_five_update or {}).items():
                if value is None:
                    continue
                dim = str(key or "").strip().lower()
                if not dim:
                    continue
                try:
                    normalized_value = max(0.0, min(1.0, float(value)))
                except (TypeError, ValueError):
                    continue
                rows.append(
                    {
                        "predicate": f"big_five_{dim}",
                        "object": str(round(normalized_value, 4)),
                        "evidence": evidence_text or f"Observed long-term persona shift: {dim}",
                        "confidence": confidence,
                        "category": category or ObservationCategory.LONG_TERM_PERSONA.value,
                        "source": source,
                        "source_msg_id": obs.source_msg_id,
                        "created_at": now,
                    }
                )
            return rows

        if PersonaManager._is_short_term_behavior_category(obs.category) and obs.efstb_update:
            efstb = obs.efstb_update
            mappings = {
                "efstb_urgency_level": efstb.urgency_level,
                "efstb_instruction_compliance": efstb.instruction_compliance,
                "efstb_logic_vs_emotion": efstb.logic_vs_emotion,
                "efstb_granularity_preference": str(efstb.granularity_preference),
            }
            for predicate, value in mappings.items():
                rows.append(
                    {
                        "predicate": predicate,
                        "object": str(value),
                        "evidence": evidence_text or f"Observed short-term EFSTB state: {predicate}",
                        "confidence": confidence,
                        "category": category or ObservationCategory.SHORT_TERM_EFSTB.value,
                        "source": source,
                        "source_msg_id": obs.source_msg_id,
                        "created_at": now,
                    }
                )
            return rows
        return rows

    async def _persist_personality_evidence(
        self,
        session,
        uid: str,
        anchor_entity_id: str,
        obs: PersonaObservation,
    ) -> int:
        rows = self._build_personality_evidence_rows(obs)
        if not rows:
            return 0

        created = 0
        for row in rows:
            await session.run(
                """
                MATCH (e:Entity {entity_id: $eid, owner_id: $uid})
                CREATE (f:Fact:PersonalityEvidence {
                    owner_id: $uid,
                    subject_id: $eid,
                    predicate: $predicate,
                    object: $object,
                    evidence: $evidence,
                    confidence: $confidence,
                    category: $category,
                    source: $source,
                    source_msg_id: $source_msg_id,
                    created_at: $created_at,
                    updated_at: $created_at
                })
                CREATE (e)-[:HAS_FACT]->(f)
                """,
                uid=uid,
                eid=anchor_entity_id,
                predicate=row["predicate"],
                object=row["object"],
                evidence=row["evidence"],
                confidence=self._clamp01(row.get("confidence"), default=0.8),
                category=row["category"],
                source=row["source"],
                source_msg_id=row.get("source_msg_id"),
                created_at=row["created_at"],
            )
            created += 1
        return created

    async def apply_observations(self, session: "ChatSession", observations: List[PersonaObservation]):
        if not observations:
            return

        user_id = session.user_id
        user_ids = {"user", "user_001", identity_config.user_id}
        asst_ids = {"assistant", "assistant_001", "andrew", identity_config.assistant_id}
        user_aliases = {
            self._canonicalize(str(x or ""))
            for x in [
                session.context_canvas.get("user_real_name"),
                *(session.context_canvas.get("user_aliases") or []),
                "user",
                identity_config.user_id,
            ]
            if str(x or "").strip()
        }
        asst_aliases = {
            self._canonicalize(str(x or ""))
            for x in [
                session.context_canvas.get("assistant_real_name"),
                *(session.context_canvas.get("assistant_aliases") or []),
                "andrew",
                identity_config.assistant_id,
            ]
            if str(x or "").strip()
        }

        async with self._driver.session(database=self.database) as db_session:
            for obs in observations:
                raw_target = str(obs.target or "").strip()
                target_type = self._canonicalize(raw_target)
                is_user = (
                    target_type == "user"
                    or raw_target in user_ids
                    or target_type in user_aliases
                )
                is_assistant = (
                    target_type == "andrew"
                    or raw_target in asst_ids
                    or target_type in asst_aliases
                )
                if is_user:
                    await self._update_user_persona(db_session, user_id, obs)
                    await self._persist_personality_evidence(
                        db_session,
                        user_id,
                        identity_config.user_id,
                        obs,
                    )
                elif is_assistant:
                    await self._update_assistant_persona(db_session, user_id, obs)
                    await self._persist_personality_evidence(
                        db_session,
                        user_id,
                        identity_config.assistant_id,
                        obs,
                    )

    async def apply_assistant_role_rewrite(self, uid: str, updates: dict) -> dict:
        safe_updates = _sanitize_assistant_role_updates(dict(updates or {}))
        if not safe_updates:
            return {}

        await self.bootstrap_genesis_identities(uid)
        persisted: Dict[str, str] = {}

        if safe_updates.get("name"):
            async with self._driver.session(database=self.database) as session:
                name_updates = await self._merge_name_update(
                    session,
                    "entity_id: $aid, owner_id: $uid",
                    {"aid": identity_config.assistant_id, "uid": uid},
                    safe_updates["name"],
                    entity_key="a",
                )
                persisted.update(name_updates)

        for key in ("role", "age", "gender", "persona", "relationship_to_user"):
            value = str(safe_updates.get(key) or "").strip()
            if value:
                persisted[key] = value

        if persisted.get("role"):
            persisted["current_role"] = persisted["role"]
        if persisted.get("persona"):
            persisted["personality_summary"] = persisted["persona"]

        if not persisted:
            return {}

        now = datetime.now().isoformat()
        set_clause = ", ".join([f"a.{key} = ${key}" for key in persisted.keys()])
        async with self._driver.session(database=self.database) as session:
            await session.run(
                f"MATCH (a:Entity {{entity_id: $aid, owner_id: $uid}}) SET {set_clause}, a.persona_updated_at = $now",
                aid=identity_config.assistant_id,
                uid=uid,
                now=now,
                **persisted,
            )
        return persisted

    async def _update_user_persona(self, session, uid: str, obs: PersonaObservation):
        """
        [v3 认知处理引擎]：分层更新慢变量人格与快变量行为态
        """
        now = datetime.now().isoformat()
        await self.bootstrap_genesis_identities(uid)

        if obs.category == ObservationCategory.LONG_TERM_PERSONA:
            filtered = {}
            for key, value in (obs.big_five_update or {}).items():
                if value is None:
                    continue
                try:
                    filtered[key] = max(0.0, min(1.0, float(value)))
                except (TypeError, ValueError):
                    continue
            if filtered:
                await self.update_big_five_momentum(uid, filtered)

        elif self._is_short_term_behavior_category(obs.category) and obs.efstb_update:
            efstb = obs.efstb_update
            await session.run(
                """
                MATCH (u:Entity {entity_id: $uid, owner_id: $uid})
                SET u.efstb_urgency = $urgency,
                    u.efstb_granularity = $granularity,
                    u.efstb_instruction_compliance = $instruction,
                    u.efstb_logic_vs_emotion = $logic,
                    u.persona_updated_at = $now
                """,
                uid=uid,
                urgency=float(efstb.urgency_level),
                granularity=str(efstb.granularity_preference),
                instruction=float(efstb.instruction_compliance),
                logic=float(efstb.logic_vs_emotion),
                now=now,
            )

    async def _update_assistant_persona(self, session, uid: str, obs: PersonaObservation):
        now = datetime.now().isoformat()
        await self.bootstrap_genesis_identities(uid)

        if obs.category == ObservationCategory.LONG_TERM_PERSONA:
            filtered = {}
            for key, value in (obs.big_five_update or {}).items():
                if value is None:
                    continue
                try:
                    filtered[key] = max(0.0, min(1.0, float(value)))
                except (TypeError, ValueError):
                    continue
            if filtered:
                set_clause = ", ".join([f"a.big_five_{k} = ${k}" for k in filtered.keys()])
                await session.run(
                    f"MATCH (a:Entity {{entity_id: $aid, owner_id: $uid}}) SET {set_clause}, a.persona_updated_at = $now",
                    aid=identity_config.assistant_id,
                    uid=uid,
                    now=now,
                    **filtered,
                )
        elif self._is_short_term_behavior_category(obs.category) and obs.efstb_update:
            efstb = obs.efstb_update
            await session.run(
                """
                MATCH (a:Entity {entity_id: $aid, owner_id: $uid})
                SET a.efstb_urgency = $urgency,
                    a.efstb_granularity = $granularity,
                    a.efstb_instruction_compliance = $instruction,
                    a.efstb_logic_vs_emotion = $logic,
                    a.persona_updated_at = $now
                """,
                aid=identity_config.assistant_id,
                uid=uid,
                urgency=float(efstb.urgency_level),
                granularity=str(efstb.granularity_preference),
                instruction=float(efstb.instruction_compliance),
                logic=float(efstb.logic_vs_emotion),
                now=now,
            )

    async def get_user_profile(self, uid: str) -> UserProfile:
        async with self._driver.session(database=self.database) as session:
            res = await session.run("MATCH (e:Entity {entity_id: $uid, owner_id: $uid}) RETURN e", uid=uid)
            record = await res.single()
            if not record:
                await self.bootstrap_genesis_identities(uid)
                res = await session.run("MATCH (e:Entity {entity_id: $uid, owner_id: $uid}) RETURN e", uid=uid)
                record = await res.single()
                if not record: return UserProfile(profile_id=uid)
            user_node = record["e"]
            state_demo = _extract_demographic_updates(user_node.get("current_state", "normal"))
            return UserProfile(
                profile_id=uid,
                identity={
                    "name": _node_primary_name(user_node, "unknown"),
                    "primary_name": _node_primary_name(user_node, "unknown"),
                    "aliases": list(user_node.get("aliases") or []),
                    "age": user_node.get("age") or state_demo.get("age") or "",
                    "gender": user_node.get("gender") or state_demo.get("gender") or "",
                    "role": user_node.get("role") or "",
                    "personality_summary": user_node.get("personality_summary") or "",
                    "biography": user_node.get("biography") or "",
                    "core_values": list(user_node.get("core_values") or []),
                    "current_state": user_node.get("current_state") or "",
                    "mbti_label": user_node.get("mbti_label") or "",
                    "big_five_openness": user_node.get("big_five_openness"),
                    "big_five_conscientiousness": user_node.get("big_five_conscientiousness"),
                    "big_five_extraversion": user_node.get("big_five_extraversion"),
                    "big_five_agreeableness": user_node.get("big_five_agreeableness"),
                    "big_five_neuroticism": user_node.get("big_five_neuroticism"),
                    "inference_reasoning": user_node.get("personality_inference_reasoning") or "",
                },
                updated_at=datetime.fromisoformat(user_node.get("persona_updated_at", datetime.now().isoformat())),
            )

    def serialize_user_profile(self, profile: UserProfile) -> dict:
        ident = profile.identity or {}
        def _bf(key):
            v = ident.get(key)
            return round(float(v), 3) if v is not None else None
        big_five = {
            "openness": _bf("big_five_openness"), "conscientiousness": _bf("big_five_conscientiousness"),
            "extraversion": _bf("big_five_extraversion"), "agreeableness": _bf("big_five_agreeableness"), "neuroticism": _bf("big_five_neuroticism"),
        }
        has_big_five = any(v is not None for v in big_five.values())
        return {
            "name": ident.get("name") or ident.get("primary_name") or "user",
            "aliases": list(ident.get("aliases") or []),
            "age": ident.get("age") or "",
            "gender": ident.get("gender") or "",
            "role": ident.get("role") or "",
            "state": ident.get("current_state") or "",
            "values": list(ident.get("core_values") or []),
            "bio": ident.get("personality_summary") or ident.get("biography") or "",
            "mbti_label": ident.get("mbti_label") or "",
            "big_five": big_five if has_big_five else {},
            "inference_reasoning": ident.get("inference_reasoning") or "",
        }

    async def get_user_profile_struct(self, uid: str) -> dict:
        return self.serialize_user_profile(await self.get_user_profile(uid))

    async def get_assistant_profile_struct(self, uid: str) -> dict:
        aid = identity_config.assistant_id
        async with self._driver.session(database=self.database) as session:
            res = await session.run(
                """
                MATCH (a:Entity {entity_id: $aid, owner_id: $uid})
                RETURN a
                """,
                aid=aid,
                uid=uid,
            )
            record = await res.single()
            if not record:
                return {
                    "name": identity_config.default_asst_name,
                    "primary_name": identity_config.default_asst_name,
                    "persona": identity_config.default_asst_persona,
                    "role": "assistant",
                    "age": "28",
                    "gender": "male",
                    "relationship_to_user": "assistant_to_user",
                    "aliases": [identity_config.default_asst_name],
                    "state": "Active",
                    "values": [],
                    "bio": identity_config.default_asst_persona,
                    "mbti_label": "",
                    "big_five": {},
                    "efstb": {},
                }

            a = record["a"]
            big_five = {
                "openness": a.get("big_five_openness"),
                "conscientiousness": a.get("big_five_conscientiousness"),
                "extraversion": a.get("big_five_extraversion"),
                "agreeableness": a.get("big_five_agreeableness"),
                "neuroticism": a.get("big_five_neuroticism"),
            }
            big_five = {k: v for k, v in big_five.items() if v is not None}
            efstb = {
                "urgency_level": a.get("efstb_urgency"),
                "granularity_preference": a.get("efstb_granularity"),
                "instruction_compliance": a.get("efstb_instruction_compliance"),
                "logic_vs_emotion": a.get("efstb_logic_vs_emotion"),
            }
            efstb = {k: v for k, v in efstb.items() if v is not None}
            return {
                "name": _node_primary_name(a, ""),
                "primary_name": _node_primary_name(a, ""),
                "persona": a.get("persona") or a.get("personality_summary") or "",
                "role": a.get("role") or a.get("current_role") or "",
                "age": a.get("age") or "",
                "gender": a.get("gender") or "",
                "relationship_to_user": _normalize_relationship_to_user(
                    a.get("relationship_to_user") or "",
                    a.get("role") or a.get("current_role") or "",
                ),
                "aliases": list(a.get("aliases") or []),
                "state": a.get("current_state") or "",
                "values": [],
                "bio": a.get("persona") or a.get("personality_summary") or a.get("summary") or a.get("description") or "",
                "mbti_label": a.get("mbti_label") or "",
                "big_five": big_five,
                "efstb": efstb,
            }

    async def bootstrap_genesis_identities(self, user_id: str):
        now = datetime.now().isoformat()
        async with self._driver.session(database=self.database) as session:
            await session.run(
                """
                MERGE (u:Entity {entity_id: $uid})
                ON CREATE SET
                    u.primary_name = $uname,
                    u.name = $uname,
                    u.aliases = [$uname],
                    u.owner_id = $uid,
                    u.role = 'user',
                    u.age = 'unknown',
                    u.gender = 'unknown',
                    u.current_state = 'Normal',
                    u.persona_updated_at = $now
                """,
                uid=user_id,
                uname="user",
                now=now,
            )
            aid = identity_config.assistant_id
            await session.run(
                """
                MERGE (a:Entity {entity_id: $aid, owner_id: $uid})
                ON CREATE SET
                    a.primary_name = $aname,
                    a.name = $aname,
                    a.aliases = [$aname],
                    a.persona = $asum,
                    a.personality_summary = $asum,
                    a.role = 'assistant',
                    a.current_role = 'assistant',
                    a.relationship_to_user = 'assistant_to_user',
                    a.age = '28',
                    a.gender = 'male',
                    a.current_state = 'Active',
                    a.persona_updated_at = $now
                ON MATCH SET a.owner_id = $uid
                """,
                aid=aid,
                uid=user_id,
                aname=identity_config.default_asst_name,
                asum=identity_config.default_asst_persona,
                now=now,
            )
            await session.run(
                """
                MATCH (u:Entity {entity_id: $uid}), (a:Entity {entity_id: $aid})
                MERGE (a)-[r:RELATION {owner_id: $uid}]->(u)
                ON CREATE SET r.type = 'loyal', r.created_at = $now
                MERGE (u)-[r2:RELATION {owner_id: $uid}]->(a)
                ON CREATE SET r2.type = 'owns', r2.created_at = $now
                """,
                uid=user_id,
                aid=aid,
                now=now,
            )

    async def update_big_five_momentum(self, uid: str, observed_vector: dict, learning_rate: float = 0.1, threshold: float = 0.05):
        async with self._driver.session(database=self.database) as session:
            res = await session.run(
                """
                MATCH (u:Entity {entity_id: $uid})
                RETURN u.big_five_openness AS o,
                       u.big_five_conscientiousness AS c,
                       u.big_five_extraversion AS e,
                       u.big_five_agreeableness AS a,
                       u.big_five_neuroticism AS n
                """,
                uid=uid,
            )
            record = await res.single()
            if not record:
                return False

            keys = ["o", "c", "e", "a", "n"]
            full_keys = ["openness", "conscientiousness", "extraversion", "agreeableness", "neuroticism"]
            updates = {}
            for short_key, full_key in zip(keys, full_keys):
                old_val = record[short_key] if record[short_key] is not None else 0.5
                new_obs = observed_vector.get(full_key)
                if new_obs is None:
                    continue
                smoothed_val = (1 - learning_rate) * old_val + learning_rate * new_obs
                if abs(smoothed_val - old_val) >= threshold:
                    updates[f"u.big_five_{full_key}"] = smoothed_val

            if not updates:
                logger.info("[PersonaManager] User %s BigFive shift < %s, skipped DB write.", uid, threshold)
                return False

            set_clauses = ", ".join([f"{k} = ${k.replace('u.big_five_', '')}" for k in updates.keys()])
            params = {k.replace("u.big_five_", ""): v for k, v in updates.items()}
            params["uid"] = uid
            params["now"] = datetime.now().isoformat()
            await session.run(f"MATCH (u:Entity {{entity_id: $uid}}) SET {set_clauses}, u.persona_updated_at = $now", **params)
            logger.info("[PersonaManager] User %s BigFive Momentum Updated: %s", uid, params)
            return True

    async def apply_long_term_persona_hints(
        self,
        uid: str,
        mbti_label: Optional[str] = None,
        big_five: Optional[Dict[str, float]] = None,
        core_values: Optional[List[str]] = None,
    ) -> bool:
        changed = False
        now = datetime.now().isoformat()

        filtered = {}
        for key, value in (big_five or {}).items():
            if value is None:
                continue
            try:
                filtered[key] = max(0.0, min(1.0, float(value)))
            except (TypeError, ValueError):
                continue
        if filtered:
            changed = await self.update_big_five_momentum(uid, filtered) or changed

        async with self._driver.session(database=self.database) as session:
            if mbti_label:
                mbti = str(mbti_label).strip().upper()
                if 2 <= len(mbti) <= 6:
                    await session.run(
                        "MATCH (u:Entity {entity_id: $uid}) SET u.mbti_label = $mbti, u.persona_updated_at = $now",
                        uid=uid,
                        mbti=mbti,
                        now=now,
                    )
                    changed = True

            clean_values = []
            for item in core_values or []:
                text = str(item).strip()
                if text and text not in clean_values:
                    clean_values.append(text)
            if clean_values:
                res = await session.run("MATCH (u:Entity {entity_id: $uid}) RETURN u.core_values AS vals", uid=uid)
                record = await res.single()
                current = list(record["vals"] or []) if record else []
                merged = current[:]
                for value in clean_values:
                    if value not in merged:
                        merged.append(value)
                if merged != current:
                    await session.run(
                        "MATCH (u:Entity {entity_id: $uid}) SET u.core_values = $vals, u.persona_updated_at = $now",
                        uid=uid,
                        vals=merged[:8],
                        now=now,
                    )
                    changed = True

        return changed

    async def settle_personality_evidence(self, uid: str, lookback_days: int = 30) -> dict:
        """
        Step-13 audit settlement (v1.1):
        aggregate + denoise personality evidence before re-inference.
        """
        lb_days = max(1, int(lookback_days))
        async with self._driver.session(database=self.database) as session:
            result = await session.run(
                """
                MATCH (u:Entity {entity_id: $uid})-[:HAS_FACT]->(f:PersonalityEvidence)
                RETURN
                    elementId(f) AS evidence_id,
                    f.predicate AS predicate,
                    f.object AS object,
                    f.evidence AS evidence,
                    coalesce(f.confidence, 0.5) AS confidence,
                    coalesce(f.updated_at, f.created_at, '') AS created_at,
                    coalesce(f.source, '') AS source,
                    coalesce(f.category, '') AS category,
                    f.source_msg_id AS source_msg_id
                ORDER BY coalesce(f.updated_at, f.created_at) DESC
                LIMIT 500
                """,
                uid=uid,
            )
            rows = [record.data() async for record in result]

        settlement = self._settle_rows(rows, lookback_days=lb_days)
        settlement["cutoff_time"] = (datetime.now() - timedelta(days=lb_days)).isoformat()

        # Persist audit summary for monitor/debug visibility.
        async with self._driver.session(database=self.database) as session:
            await session.run(
                """
                MATCH (u:Entity {entity_id: $uid})
                SET u.personality_audit_summary = $summary_json,
                    u.personality_audit_quality = $quality_index,
                    u.personality_audit_updated_at = $now
                """,
                uid=uid,
                summary_json=json.dumps(settlement, ensure_ascii=False),
                quality_index=float(settlement.get("quality_index", 0.0)),
                now=datetime.now().isoformat(),
            )

        return settlement

    async def re_infer_identity(
        self,
        uid: str,
        evidence_refs: Optional[List[str]] = None,
        audit_summary: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """
        Trigger a system judgment to re-infer MBTI and BigFive
        from existing personality evidence in the graph.
        """
        try:
            from memory.identity.inference import PersonaInferenceService
            inference_service = PersonaInferenceService(self._driver, self.database)
            judgment = await inference_service.run_judgment(
                uid,
                evidence_refs=evidence_refs,
                audit_summary=audit_summary,
            )
            if not judgment:
                return False
            
            now = datetime.now().isoformat()
            async with self._driver.session(database=self.database) as session:
                # 1) Update core fields and persist reasoning text.
                # 2) Rebuild reference links to supporting facts.
                await session.run(
                    """
                    MATCH (u:Entity {entity_id: $uid})
                    SET u.mbti_label = $mbti,
                        u.big_five_openness = $o,
                        u.big_five_conscientiousness = $c,
                        u.big_five_extraversion = $e,
                        u.big_five_agreeableness = $a,
                        u.big_five_neuroticism = $n,
                        u.personality_inference_reasoning = $reasoning,
                        u.persona_updated_at = $now
                    
                    WITH u
                    OPTIONAL MATCH (u)-[r:BASED_ON {category: 'personality_inference'}]->(:Fact)
                    DELETE r
                    
                    WITH u
                    UNWIND $refs AS ref_id
                    MATCH (f:Fact) WHERE elementId(f) = ref_id
                    CREATE (u)-[:BASED_ON {category: 'personality_inference'}]->(f)
                    """,
                    uid=uid,
                    mbti=judgment.mbti_label,
                    o=judgment.big_five.get("openness", 0.5),
                    c=judgment.big_five.get("conscientiousness", 0.5),
                    e=judgment.big_five.get("extraversion", 0.5),
                    a=judgment.big_five.get("agreeableness", 0.5),
                    n=judgment.big_five.get("neuroticism", 0.5),
                    reasoning=judgment.reasoning,
                    refs=judgment.references,
                    now=now
                )
                logger.info("[PersonaManager] System Judgment Re-Infer completed for user %s: %s", uid, judgment.mbti_label)
                return True
        except Exception as e:
            logger.error("[PersonaManager] Identity Inference Failed: %s", e)
            return False





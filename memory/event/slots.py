"""Schemas for event extraction and v3 persona observations."""

from enum import Enum
from typing import Any, Dict, List, Optional, Union
from decimal import Decimal

from pydantic import AliasChoices, BaseModel, Field


class ActionType(str, Enum):
    STATE_CHANGE = "STATE_CHANGE"
    INTERACTION = "INTERACTION"
    CONSUMPTION = "CONSUMPTION"
    PLAN = "PLAN"
    OPINION = "OPINION"
    ACHIEVEMENT = "ACHIEVEMENT"
    RELATIONSHIP = "RELATIONSHIP"
    OTHER = "OTHER"


class EmotionLabel(str, Enum):
    JOY = "joy"
    ANGER = "anger"
    SADNESS = "sadness"
    FEAR = "fear"
    SURPRISE = "surprise"
    DISGUST = "disgust"
    NEUTRAL = "neutral"


class MainEventType(str, Enum):
    FINANCE = "FINANCE"
    HEALTH = "HEALTH"
    WORK = "WORK"
    SOCIAL = "SOCIAL"
    CONSUMPTION = "CONSUMPTION"
    LOCATION = "LOCATION"
    EMOTION = "EMOTION"
    PROPERTY = "PROPERTY"
    GOAL = "GOAL"
    TASK = "TASK"
    SCHEDULE = "SCHEDULE"
    PLAN = "PLAN"
    OPINION = "OPINION"
    LEGAL = "LEGAL"
    SYSTEM = "SYSTEM"


class TypedPayload(BaseModel):
    model_config = {"extra": "allow"}
    
    quantity: Optional[Decimal] = None
    quantity_unit: Optional[str] = None
    amount: Optional[Decimal] = None
    currency: Optional[str] = None
    currency_source: Optional[str] = None
    original_text: Optional[str] = None


class NormalizationMeta(BaseModel):
    confidence: float = Field(default=1.0)
    method: str = "rule" # rule, llm
    rules_applied: List[str] = Field(default_factory=list)
    needs_confirmation: bool = False


class EventEnvelope(BaseModel):
    """Structured event envelope for extraction and normalization."""
    event_id: Optional[str] = None # UUID string
    main_type: MainEventType
    subtype: Optional[str] = None
    event_time: Optional[str] = None # ISO format or natural language before normalization
    subject: str
    predicate: str
    object: Optional[str] = None
    payload: TypedPayload = Field(default_factory=TypedPayload)
    normalization: NormalizationMeta = Field(default_factory=NormalizationMeta)
    confidence: float = Field(default=1.0, ge=0.0, le=1.0)
    source_msg_id: Optional[int] = None
    needs_confirmation: bool = False
    metadata: Dict[str, Any] = Field(default_factory=dict)


class MemoryEvent(BaseModel):
    # Keep legacy for compatibility during migration
    model_config = {"extra": "allow"}

    subject: str
    object: Optional[str] = None
    predicate: str
    action_type: ActionType
    context: Optional[str] = Field(default="unspecified")
    timestamp_reference: Optional[str] = None
    duration: Optional[str] = None
    emotion_label: str = Field(default=EmotionLabel.NEUTRAL.value)
    impact_score: Optional[int] = Field(default=5, ge=1, le=10)
    confidence: Optional[float] = Field(default=1.0, ge=0.0, le=1.0)
    source_entity: Optional[str] = None
    trust_score: Optional[float] = Field(default=0.8, ge=0.0, le=1.0)
    event_metadata: Optional[dict] = Field(default_factory=dict)
    source_msg_id: Optional[int] = None
    event_time: Optional[str] = None


class EntityRelation(BaseModel):
    model_config = {"extra": "allow"}

    from_entity: str
    relation_type: str
    to_entity: str
    confidence: float = Field(default=1.0, ge=0.0, le=1.0)
    source_entity: Optional[str] = None
    source_msg_id: Optional[int] = None
    inferred: bool = False
    inference_rule: Optional[str] = None
    confirmed: bool = False


class MemoryEpisode(BaseModel):
    episode_id: Optional[str] = None
    name: str
    summary: str
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    associated_events: List[str] = Field(default_factory=list)
    evidence_msg_ids: List[int] = Field(default_factory=list)
    efstb_tags: Optional[Dict] = Field(default_factory=dict)
    mbti_hint: Optional[str] = None
    big_five_observed: Optional[Dict[str, float]] = Field(default_factory=dict)
    core_values_hint: List[str] = Field(default_factory=list)


class MemorySaga(BaseModel):
    saga_id: Optional[str] = None
    title: str
    description: str
    start_time: Optional[str] = None
    last_active: Optional[str] = None
    associated_episode_ids: List[str] = Field(default_factory=list)


class ObservationCategory(str, Enum):
    LONG_TERM_PERSONA = "long_term_persona"
    SHORT_TERM_EFSTB = "short_term_efstb"


class ProfileProperties(BaseModel):
    """Minimal profile whitelist; extra fields pass through as facts."""

    model_config = {"extra": "allow"}

    name: Optional[str] = None
    age: Optional[str] = None
    gender: Optional[str] = None
    mbti_label: Optional[str] = None
    big_five_openness: Optional[float] = None
    big_five_conscientiousness: Optional[float] = None
    big_five_extraversion: Optional[float] = None
    big_five_agreeableness: Optional[float] = None
    big_five_neuroticism: Optional[float] = None


class EfstbBehavioralTags(BaseModel):
    urgency_level: float = Field(default=0.5, ge=0.0, le=1.0)
    granularity_preference: str = Field(default="medium")
    logic_vs_emotion: float = Field(default=0.5, ge=0.0, le=1.0)
    instruction_compliance: float = Field(default=0.5, ge=0.0, le=1.0)


class PersonaObservation(BaseModel):
    model_config = {"extra": "allow", "populate_by_name": True}

    target: str
    category: ObservationCategory
    content: str = Field(..., description="Observation summary")
    confidence: float = Field(default=0.8, ge=0.0, le=1.0)
    evidence_summary: Optional[str] = None
    big_five_update: Optional[Dict[str, float]] = Field(
        default=None,
        description="Observed shift in Big Five dimensions",
    )
    efstb_update: Optional[EfstbBehavioralTags] = Field(
        default=None,
        validation_alias=AliasChoices("efstb_update", "efstb_update"),
        description="Observed short-term EFSTB state",
    )
    source_msg_id: Optional[int] = None


class FullExtractionResult(BaseModel):
    events: List[MemoryEvent] = Field(default_factory=list)
    event_envelopes: List[EventEnvelope] = Field(default_factory=list) # New structured format
    relations: List[EntityRelation] = Field(default_factory=list)
    observations: List[PersonaObservation] = Field(default_factory=list)


class EventExtractionResult(BaseModel):
    events: List[MemoryEvent] = Field(default_factory=list)
    event_envelopes: List[EventEnvelope] = Field(default_factory=list)

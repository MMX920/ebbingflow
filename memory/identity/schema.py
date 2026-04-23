"""Unified identity schema (v3)."""

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class UserProfile(BaseModel):
    """Runtime user profile envelope for prompt injection and sync payloads.
    
    Legacy shim for compatibility with v1/v2 manager code.
    """

    profile_id: str = Field(default="user")
    identity: Dict[str, Any] = Field(default_factory=dict)
    constraints: List[str] = Field(default_factory=list)
    state: Dict[str, str] = Field(default_factory=dict)
    updated_at: datetime = Field(default_factory=datetime.now)


class BigFiveVector(BaseModel):
    openness: float = Field(default=0.5, ge=0.0, le=1.0)
    conscientiousness: float = Field(default=0.5, ge=0.0, le=1.0)
    extraversion: float = Field(default=0.5, ge=0.0, le=1.0)
    agreeableness: float = Field(default=0.5, ge=0.0, le=1.0)
    neuroticism: float = Field(default=0.5, ge=0.0, le=1.0)


class LongTermPersona(BaseModel):
    mbti_label: Optional[str] = None
    big_five: BigFiveVector = Field(default_factory=BigFiveVector)
    core_values: List[str] = Field(default_factory=list)
    last_updated: datetime = Field(default_factory=datetime.now)


class EfstbBehavioralTags(BaseModel):
    urgency_level: float = Field(default=0.5, ge=0.0, le=1.0)
    granularity_preference: str = Field(default="medium")
    instruction_compliance: float = Field(default=0.5, ge=0.0, le=1.0)
    logic_vs_emotion: float = Field(default=0.5, ge=0.0, le=1.0)
    volatility: float = Field(default=0.0, ge=0.0, le=1.0)
    decay_timestamp: datetime = Field(default_factory=datetime.now)


class DualLayerProfile(BaseModel):
    profile_id: str
    long_term: LongTermPersona = Field(default_factory=LongTermPersona)
    short_term: EfstbBehavioralTags = Field(default_factory=EfstbBehavioralTags)

    def generate_prompt_injection(self) -> str:
        mbti = (self.long_term.mbti_label or "").strip().upper()
        mbti_part = f" MBTI={mbti}." if mbti else ""

        bf = self.long_term.big_five
        big_five_part = (
            f" BigFive(openness={bf.openness:.3f}, conscientiousness={bf.conscientiousness:.3f}, "
            f"extraversion={bf.extraversion:.3f}, agreeableness={bf.agreeableness:.3f}, "
            f"neuroticism={bf.neuroticism:.3f})."
        )

        values_part = ""
        if self.long_term.core_values:
            values_part = " CoreValues=" + ", ".join(self.long_term.core_values[:3]) + "."

        s = self.short_term
        efstb_part = (
            f" EFSTB(urgency={s.urgency_level:.3f}, granularity={s.granularity_preference}, "
            f"instruction={s.instruction_compliance:.3f}, logic={s.logic_vs_emotion:.3f}, "
            f"volatility={s.volatility:.3f})."
        )

        return ("Dual-layer user profile." + mbti_part + big_five_part + values_part + efstb_part).strip()

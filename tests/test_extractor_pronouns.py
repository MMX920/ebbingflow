from memory.event.extractor import EventExtractor
from memory.event.slots import ActionType, FullExtractionResult, MemoryEvent


def test_detect_reported_target_chinese():
    extractor = EventExtractor()
    assert extractor.detect_reported_target("我跟王哥说：你真的很棒") == "王哥"
    assert extractor.detect_reported_target("我对小李说你别迟到") == "小李"


def test_bind_reported_you_to_object_named_target():
    extractor = EventExtractor()
    result = FullExtractionResult(
        events=[
            MemoryEvent(subject="我", predicate="评价", object="你", action_type=ActionType.OTHER),
            MemoryEvent(subject="你", predicate="迟到", object="", action_type=ActionType.OTHER),
        ],
        relations=[],
        observations=[],
    )

    extractor._bind_reported_you_to_object("我跟王哥说你别迟到", result)

    assert result.events[0].object == "王哥"
    assert result.events[1].subject == "王哥"


def test_bind_reported_you_to_object_assistant_fallback():
    extractor = EventExtractor()
    result = FullExtractionResult(
        events=[
            MemoryEvent(subject="我", predicate="评价", object="你", action_type=ActionType.OTHER),
        ],
        relations=[],
        observations=[],
    )

    extractor._bind_reported_you_to_object("我对你说你很专业", result)

    assert result.events[0].object == "assistant"

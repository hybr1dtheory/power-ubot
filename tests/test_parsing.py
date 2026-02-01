from helpers import parse_message


def test_on_message_parsing():
    text = (
        "🟢 08:31 Світло з'явилося\n"
        "🕓 Його не було 3год 0хв\n"
        "🗓 Наступне планове: 13:00 - 18:00"
    )
    result = parse_message(text)

    assert result["type"] == "ON"
    assert result["time"] == "08:31"
    assert result["duration"].total_seconds() == 3 * 3600

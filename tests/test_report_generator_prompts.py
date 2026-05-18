from src.reporting.generator import ReportGenerator


def test_build_template_prompt_auto():
    generator = ReportGenerator()
    validation = {"available_collectors": ["steam", "qimai"]}

    prompt = generator._build_template_prompt(
        "Analyze this game", "auto", validation, custom_prompt="Focus on CN market"
    )

    assert "Analyze this game" in prompt
    assert "Available data sources: steam, qimai" in prompt
    assert "Focus on CN market" in prompt
    assert "Dynamically structure" in prompt

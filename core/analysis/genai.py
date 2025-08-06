import os
from typing import Literal

from google import genai
from google.genai.types import (
    Content,
    FileData,
    GenerateContentConfig,
    HarmBlockThreshold,
    HarmCategory,
    HttpOptions,
    Part,
    SafetySetting,
)
from pydantic import BaseModel

client = genai.Client(
    vertexai=True,
    project=os.environ["GEMINI_PROJECT"],
    location=os.environ["GEMINI_LOCATION"],
    http_options=HttpOptions(api_version="v1"),
)


DEFAULT_SAFETY_SETTINGS = [
    SafetySetting(
        category=HarmCategory("HARM_CATEGORY_HATE_SPEECH"),
        threshold=HarmBlockThreshold("BLOCK_NONE"),
    ),
    SafetySetting(
        category=HarmCategory("HARM_CATEGORY_DANGEROUS_CONTENT"),
        threshold=HarmBlockThreshold("BLOCK_NONE"),
    ),
    SafetySetting(
        category=HarmCategory("HARM_CATEGORY_SEXUALLY_EXPLICIT"),
        threshold=HarmBlockThreshold("BLOCK_NONE"),
    ),
    SafetySetting(
        category=HarmCategory("HARM_CATEGORY_HARASSMENT"),
        threshold=HarmBlockThreshold("BLOCK_NONE"),
    ),
]


class Sentence(BaseModel):
    text: str
    start_time_s: float
    source: Literal["audio"] | Literal["screen"]
    is_claim: bool


async def generate_transcript(video_url: str) -> list[Sentence]:
    prompt = """
    Transcribe the spoken audio, and extract any text displayed on the screen into complete sentences.
    For audio, specify the "source" as "audio", and for text from the screen specify the source as "screen".
    If a sentence appears to be a claim, you must mark it as so by setting `is_claim` to True.
    Each sentence must be separated naturally.
    The Timestamps for each sentence must be provided as SS (seconds only) using the `start_time_s` field.
    """

    response = await client.aio.models.generate_content(
        model=os.environ["GEMINI_MODEL"],
        config=GenerateContentConfig(
            safety_settings=DEFAULT_SAFETY_SETTINGS,
            audio_timestamp=True,
            response_mime_type="application/json",
            response_schema=list[Sentence],
        ),
        contents=Content(
            role="user",
            parts=[
                Part(
                    file_data=FileData(
                        file_uri=video_url,
                        mime_type="video/mp4",
                    )
                ),
                Part(text=prompt),
            ],
        ),
    )

    if not response.parsed or not isinstance(response.parsed, list):
        raise ValueError(
            f"Did not get expected response from Gemini: {response.parsed}"
        )

    return response.parsed

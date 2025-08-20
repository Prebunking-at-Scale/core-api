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


async def generate_transcript(video_url: str) -> list[Sentence]:
    prompt = """Transcribe the provided video.
Split the transcript into complete sentences, separated naturally.
Only return individual sentences.
For each sentence, provide a timestamp formatted as SS (seconds only) using the `start_time_s` field.
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

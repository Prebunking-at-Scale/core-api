import asyncio
import os
import random
import logging
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
from pydantic import BaseModel, Field

log = logging.getLogger(__name__)

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
    text: str = Field(
        description=(
            "A complete sentence. " "Do not translate from its original language."
        )
    )
    source: Literal["audio", "video"] = Field(
        description=(
            "The source of the sentence."
            "'audio' if it's from the audio."
            "'video' if it was text on the video."
        )
    )
    start_time_s: float = Field(
        description="Timestamp for when sentence starts, formatted as SS (seconds only)"
    )
    language: str = Field(
        description="Language of the sentence as a two letter ISO language code (e.g. 'en', 'es')"
    )


class RetriesExceededError(Exception):
    pass


async def generate_transcript(video_url: str, retries: int = 3) -> list[Sentence]:
    prompt = """
Transcribe the video into sentences by following these steps.

1. Transcribe the audio from the video as a block of text.
2. Split this text into complete sentences. The "source" value of these sentences should be "audio".
3. Save any text in the video, and make sentences out of them. The source for these sentences should be "video". Ignore subtitles if the audio is already saved.
4. Check for any duplicate sentences which appear as both "audio" and "video" sentences. If there are, only keep the "audio" version.
5. Check that all the sentences are as complete as possible, and merge any partial sentences together.
6. Sort the sentences so they appear in the same order as in the video.

Do not transcribe the lyrics of background music in the videos.

Only include content from inside the video. Do not create any new content.

Each sentence should be in the same language as it appears or is spoken in the video. Do not translate the transcript.
"""
    for _ in range(retries):
        try:
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

        except Exception as exc:
            log.info("Encountered an error during transcription: " + repr(exc))
            await asyncio.sleep(random.randint(5, 30))
            continue

    raise RetriesExceededError(f"failed to get transcript after {retries} attempts")

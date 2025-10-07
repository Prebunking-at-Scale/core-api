from pydantic import BaseModel

class LanguageWithVideoCount(BaseModel):
    language: str
    count: int

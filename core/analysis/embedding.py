from sentence_transformers import SentenceTransformer
from torch import Tensor

model: SentenceTransformer | None = None


def encode(sentence: str) -> Tensor:
    global model
    if not model:
        model = SentenceTransformer(
            "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2",
            device="cpu",
        )
    return model.encode(sentence, show_progress_bar=False)

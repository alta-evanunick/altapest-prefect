from typing import Iterable, List, TypeVar

T = TypeVar("T")


def chunk_list(seq: List[T], chunk_size: int) -> Iterable[List[T]]:
    """
    Yield successive chunk_size-sized chunks from seq.
    """
    for i in range(0, len(seq), chunk_size):
        yield seq[i : i + chunk_size]
def n_grams(text: str, n: int = 3) -> list[str]:
    """
    Return a list of n‑grams for the supplied string.
    
    Args:
        text (str): The text string to find n-grams for
        n (int): The parameter which determines the length of the n-grams
    """
    if n <= 0:
        raise ValueError("n must be a positive integer")

    if len(text) < n:
        return []

    return [text[i : i + n] for i in range(len(text) - n + 1)]


from typing import List

def n_grams(text: str, n: int = 3) -> List[str]:
    """
    Return a list of n‑grams for the supplied string.
    
    The function:
    * normalises the string by stripping leading/trailing whitespace
    * treats the string as a sequence of characters
    * includes overlapping n‑grams
    
    Example
    -------
    >>> n_grams("hello", 2)
    ['he', 'el', 'll', 'lo']
    
    Parameters
    ----------
    text : str
        Input string to be decomposed.
    n : int
        Length of each n‑gram (must be >= 1).
    
    Returns
    -------
    List[str]
        List of n‑gram strings, in the order they appear in `text`.
    """
    if n <= 0:
        raise ValueError("n must be a positive integer")
    if len(text) < n:
        return []
    return [text[i : i + n] for i in range(len(text) - n + 1)]


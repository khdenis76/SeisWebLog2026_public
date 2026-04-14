from pathlib import Path


from pathlib import Path


def tex_escape(text: str) -> str:
    if text is None:
        return ""

    text = str(text)

    # first normalize Windows paths only if raw backslashes are present
    # do this BEFORE adding LaTeX escapes
    text = text.replace("\\", "/")

    replacements = {
        "&": r"\&",
        "%": r"\%",
        "$": r"\$",
        "#": r"\#",
        "_": r"\_",
        "{": r"\{",
        "}": r"\}",
        "~": r"\textasciitilde{}",
        "^": r"\^{}",
    }

    for k, v in replacements.items():
        text = text.replace(k, v)

    # remove non-printable garbage characters
    text = "".join(c for c in text if c.isprintable())

    return text


def tex_path(path) -> str:
    if not path:
        return ""
    return str(Path(path)).replace("\\", "/")


def latex_bool(value) -> bool:
    return bool(value)


def tex_path(path) -> str:
    """
    Convert Windows path to LaTeX-safe forward-slash path.
    """
    if not path:
        return ""
    return str(Path(path)).replace("\\", "/")


def latex_bool(value) -> bool:
    return bool(value)
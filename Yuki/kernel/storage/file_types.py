"""Classification of job output files into 'plot' vs 'data', plus a
selection-spec predicate builder. Single source of truth for what counts
as a plot, shared by collect filtering, booking filtering, and status views.
"""
import fnmatch
import os

PLOT_EXTENSIONS = {".png", ".jpg", ".jpeg", ".gif", ".pdf", ".svg", ".webp", ".eps"}

INLINE_IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg"}


def is_plot(filename):
    """True when the file's extension marks it as a plot/image."""
    return os.path.splitext(filename)[1].lower() in PLOT_EXTENSIONS


def is_inline_image(filename):
    """True when a browser can render the file inline as an <img> (raster or
    SVG). Narrower than is_plot: excludes .pdf/.eps, which are plots but not
    inline-renderable images."""
    return os.path.splitext(filename)[1].lower() in INLINE_IMAGE_EXTENSIONS


def classify(filename):
    """Return 'plot' or 'data' for a stageout filename."""
    return "plot" if is_plot(filename) else "data"


def make_predicate(spec):
    """Build a name predicate from a selection spec.

    spec is one of: 'plots', 'data', 'all', a glob pattern, or a literal
    filename/path. Returns a function (name) -> bool where name may be a
    relative path such as 'subdir/file.root' or a bare filename.

    Semantics:
      - type keywords classify by basename extension.
      - globs without '/' match the basename.
      - globs with '/' match the full relative path.
      - literal names match either the exact relative path or the basename.
    """
    if spec in ("all", "", None):
        return lambda name: True
    if spec == "plots":
        return lambda name: is_plot(os.path.basename(name))
    if spec == "data":
        return lambda name: not is_plot(os.path.basename(name))
    if any(ch in spec for ch in "*?["):
        if "/" in spec:
            return lambda name: fnmatch.fnmatch(name, spec)
        return lambda name: fnmatch.fnmatch(os.path.basename(name), spec)
    return lambda name: name == spec or os.path.basename(name) == spec

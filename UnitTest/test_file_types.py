"""Tests for the file_types classification helpers."""
from Yuki.kernel.storage import file_types


def test_is_plot_by_extension():
    """is_plot matches image extensions case-insensitively."""
    assert file_types.is_plot("mass.png") is True
    assert file_types.is_plot("fit.PDF") is True          # case-insensitive
    assert file_types.is_plot("ntuple.root") is False


def test_classify():
    """classify maps images to plot and everything else to data."""
    assert file_types.classify("mass.png") == "plot"
    assert file_types.classify("ntuple.root") == "data"


def test_make_predicate_type_keywords():
    """Type keywords plots/data/all select matching files."""
    plots = file_types.make_predicate("plots")
    data = file_types.make_predicate("data")
    every = file_types.make_predicate("all")
    assert plots("mass.png") and not plots("ntuple.root")
    assert data("ntuple.root") and not data("mass.png")
    assert every("anything.xyz") is True


def test_make_predicate_glob_and_name():
    """Glob patterns and literal names match by extension or basename."""
    glob = file_types.make_predicate("*.root")
    name = file_types.make_predicate("mass.png")
    assert glob("ntuple.root") and not glob("mass.png")
    assert name("mass.png") and not name("other.png")


def test_is_plot_covers_legacy_image_set():
    """The legacy status.py image set must still classify as plot."""
    # The old status.py inline set: png/jpg/jpeg/gif must still classify as plot.
    for ext in (".png", ".jpg", ".jpeg", ".gif"):
        assert file_types.is_plot("x" + ext) is True


def test_make_predicate_relative_path():
    """A relative path predicate matches only that exact path."""
    pred = file_types.make_predicate("plots/mass.png")
    assert pred("plots/mass.png")
    assert not pred("mass.png")
    assert not pred("plots/other.png")


def test_make_predicate_basename_glob_matches_nested():
    """A basename glob matches files in nested directories."""
    pred = file_types.make_predicate("*.png")
    assert pred("mass.png")
    assert pred("plots/mass.png")
    assert not pred("mass.root")


def test_make_predicate_path_glob():
    """A path glob matches only paths under the globbed directory."""
    pred = file_types.make_predicate("plots/*.png")
    assert pred("plots/mass.png")
    assert not pred("mass.png")
    assert not pred("data/mass.png")


def test_make_predicate_literal_matches_basename_or_path():
    """A literal name matches its basename or full path."""
    pred = file_types.make_predicate("mass.png")
    assert pred("mass.png")
    assert pred("plots/mass.png")
    assert not pred("other.png")

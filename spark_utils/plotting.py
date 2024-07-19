import math
import warnings
from types import ModuleType
from typing import Optional, Tuple, Union

from IPython.display import Image  # type: ignore
from IPython.display import display as display_img  # type: ignore

rabo_colors = ["#000099", "#FD6400", "#80BA27", "#C8009C", "#D6083B", "#FFC200", "#90D1E3", "#A19469"]


class PlotSaver:
    """Class for saving and retrieving the plots
    Attributes
    ----------
    file_path : str, default "/Workspace/Shared/pics"
        path to the picture storage folder
    prefix : str, default ""
        a string added to each picture name to ensure name uniqueness between the savers
    show_saved : bool, default True
        whether to put the saved figure on the screen
    dpi : int, default 300
        figure resolution

    Methods
    -------
    save(plt: ModuleType, name: str, show_saved: Optional[bool] = None):
        Saves the plot

    display(name: str):
        Shows the plot
    """

    def __init__(
        self, file_path: str = "/Workspace/Shared/pics", prefix: str = "", show_saved: bool = True, dpi: int = 300
    ) -> None:
        self.file_path = f"{file_path}/{prefix}"
        self.show_saved = show_saved
        self.dpi = dpi

    def _file_path(self, name: str) -> str:
        return f"{self.file_path}{name}.png"

    def display(self, name: str) -> None:
        display_img(Image(filename=self._file_path(name)))
        return

    def save(self, plt: ModuleType, name: str, show_saved: Optional[bool] = None) -> None:
        file_path = self._file_path(name)
        print(file_path)
        plt.savefig(file_path, bbox_inches="tight", dpi=self.dpi)
        # check if the overall value is overwritten
        show_saved = self.show_saved if (show_saved is None) else show_saved
        if show_saved:
            plt.close()
            self.display(name)

    def save_plot(self, plt: ModuleType, name: str, show_saved: Optional[bool] = None) -> None:
        warnings.warn("Warning! The function name will be depreciated. Use .save() instead")
        self.save(plt, name, show_saved)


def si_classifier(value: Union[int, float]) -> Union[dict, None]:
    suffixes = {
        24: {"long_suffix": "yotta", "short_suffix": "Y", "scalar": 10**24},
        21: {"long_suffix": "zetta", "short_suffix": "Z", "scalar": 10**21},
        18: {"long_suffix": "exa", "short_suffix": "E", "scalar": 10**18},
        15: {"long_suffix": "peta", "short_suffix": "P", "scalar": 10**15},
        12: {"long_suffix": "tera", "short_suffix": "T", "scalar": 10**12},
        9: {"long_suffix": "giga", "short_suffix": "G", "scalar": 10**9},
        6: {"long_suffix": "mega", "short_suffix": "M", "scalar": 10**6},
        3: {"long_suffix": "kilo", "short_suffix": "k", "scalar": 10**3},
        0: {"long_suffix": "", "short_suffix": "", "scalar": 10**0},
        -3: {"long_suffix": "milli", "short_suffix": "m", "scalar": 10**-3},
        -6: {"long_suffix": "micro", "short_suffix": "µ", "scalar": 10**-6},
        -9: {"long_suffix": "nano", "short_suffix": "n", "scalar": 10**-9},
        -12: {"long_suffix": "pico", "short_suffix": "p", "scalar": 10**-12},
        -15: {"long_suffix": "femto", "short_suffix": "f", "scalar": 10**-15},
        -18: {"long_suffix": "atto", "short_suffix": "a", "scalar": 10**-18},
        -21: {"long_suffix": "zepto", "short_suffix": "z", "scalar": 10**-21},
        -24: {"long_suffix": "yocto", "short_suffix": "y", "scalar": 10**-24},
    }
    exponent = int(math.floor(math.log10(abs(value)) / 3.0) * 3)
    return suffixes.get(exponent, None)


def si_formatter(value: Union[int, float]) -> Tuple:
    """
    Return a triple of scaled value, short suffix, long suffix, or None if
    the value cannot be classified.
    """
    classifier = si_classifier(value)
    if classifier is None:
        # Don't know how to classify this value
        return (None, None, None)

    scaled = value / classifier["scalar"]
    return (scaled, classifier["short_suffix"], classifier["long_suffix"])


def si_format(value: Union[int, float], precision: int = 0, long_form: bool = False, separator: str = "") -> str:
    """
    "SI prefix" formatted string: return a string with the given precision
    and an appropriate order-of-3-magnitudes suffix, e.g.:
        si_format(1001.0) => '1.00K'
        si_format(0.00000000123, long_form=True, separator=' ') => '1.230 nano'
    """

    if value == 0:
        # Don't know how to format this value
        return "0"

    scaled, short_suffix, long_suffix = si_formatter(value)

    if scaled is None:
        # Don't know how to format this value
        return str(value)

    # make sure numbers like 2500 do not get truncated to 2k
    separats = str(scaled).split(".")
    if (len(separats[0]) < 2) & (len(separats[1].replace("0", "")) > 0):
        precision = max(precision, 1)

    suffix = long_suffix if long_form else short_suffix

    return "{scaled:.{precision}f}{separator}{suffix}".format(
        scaled=scaled, precision=precision, separator=separator, suffix=suffix
    )

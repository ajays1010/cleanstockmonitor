import hashlib
from decimal import Decimal
from pathlib import Path

from fastapi.templating import Jinja2Templates

from app.core.alerts import volume_text

HERE = Path(__file__).parent
templates = Jinja2Templates(directory=HERE / "templates")

# Added to the stylesheet URL so browsers fetch the new file after each deploy.
_css = (HERE / "static" / "app.css").read_bytes()
templates.env.globals["css_version"] = hashlib.sha256(_css).hexdigest()[:8]


def plain_number(value: Decimal | None) -> str:
    """5.00 -> '5', 2.50 -> '2.5', 10.00 -> '10' (Decimal.normalize alone would give '1E+1')."""
    if value is None:
        return ""
    text = f"{value:f}"
    return text.rstrip("0").rstrip(".") if "." in text else text


templates.env.filters["num"] = plain_number
templates.env.filters["shares"] = volume_text  # 500000 -> "5.00 L"

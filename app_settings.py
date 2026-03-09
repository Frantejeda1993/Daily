"""Centralized application configuration constants."""

from datetime import date


class AppConfig:
    LANGUAGE = "es"
    LOCALE = "es_ES"

    MONTHS_BY_LANGUAGE = {
        "es": [
            "Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio",
            "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre",
        ],
        "en": [
            "January", "February", "March", "April", "May", "June",
            "July", "August", "September", "October", "November", "December",
        ],
    }

    CHARTS = {
        "top_brands_count": 20,
        "stock_grid_cols": 4,
        "months_in_year": 12,
    }

    DATE_CALCULATIONS = {
        "year_start_day": 1,
        "year_start_month": 1,
    }

    @classmethod
    def get_months(cls) -> list[str]:
        return cls.MONTHS_BY_LANGUAGE.get(cls.LANGUAGE, cls.MONTHS_BY_LANGUAGE["es"])

    @staticmethod
    def get_days_in_year(year: int) -> int:
        return 366 if year % 4 == 0 and (year % 100 != 0 or year % 400 == 0) else 365

    @classmethod
    def get_year_start(cls, year: int) -> date:
        return date(year, cls.DATE_CALCULATIONS["year_start_month"], cls.DATE_CALCULATIONS["year_start_day"])

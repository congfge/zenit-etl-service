import calendar
from datetime import date


def get_last_3_closing_dates() -> list[str]:
    """Retorna las últimas 3 fechas de cierre (último día de cada mes).

    Ejemplo (hoy Feb 2026): ['30/11/2025', '31/12/2025', '31/01/2026']
    """
    today = date.today()
    year, month = today.year, today.month
    dates = []
    for _ in range(3):
        if month == 1:
            month, year = 12, year - 1
        else:
            month -= 1
        last_day = calendar.monthrange(year, month)[1]
        dates.insert(0, f"{last_day:02d}/{month:02d}/{year}")
    return dates

import pandas as pd
import numpy as np
from utils.logger import logger

# Columnas exactas de la tabla destino creditos_historico_etl
TABLE_COLUMNS = [
    'dpi', 'latitud', 'longitud', 'tiene_coordenadas', 'mora', 'capital_concedido', 'KPI',
    'nombre_del_cliente', 'departamento', 'municipio', 'fecha', 'Distrito',
    'cod_region', 'region', 'cod_sucursal', 'sucursal', 'Inlat_suc', 'finlog_suc',
    'sector', 'COD_PROMOTOR', 'cod_tipopromotor', 'tipo_promotor', 'tipo',
    'fud', 'producto', 'subproducto', 'programad', 'fecha_ultima_operación',
    'I_G', 'tipo_negociod', 'sexo', 'etapa', 'tel_principal', 'tel_secundario',
    'etnia', 'idioma'
]

class DataCleaner:
    # ? Límites geográficos de Guatemala
    LAT_MIN, LAT_MAX = 13.7, 17.8
    LON_MIN, LON_MAX = -92.3, -88.2

    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info(f"Iniciando limpieza de {len(df)} registros")
        df = self._clean_coordinate_values(df)
        df = self._validate_and_flag_coordinates(df)
        df = self._clean_financial_fields(df)
        df = self._standarize_kpis(df)
        df = self._deduplicate_by_dpi(df)
        df = self._select_table_columns(df)
        logger.info(f"Limpieza completada: {len(df)} registros")
        return df

    def _parse_coordinate(self, value):
        """Convierte un valor de coordenada a float. Retorna np.nan si no parseable."""
        if pd.isna(value):
            return np.nan
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            value = value.strip().replace('"', '').replace("'", '').replace(',', '.')
            try:
                return float(value)
            except (ValueError, TypeError):
                return np.nan
        return np.nan

    def _clean_coordinate_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Parsea latitud y longitud de string/numeric a float."""
        df['latitud'] = df['latitud'].apply(self._parse_coordinate)
        df['longitud'] = df['longitud'].apply(self._parse_coordinate)
        df['latitud'] = df['latitud'].round(5)
        df['longitud'] = df['longitud'].round(5)
        return df

    def _validate_and_flag_coordinates(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Agrega columna `tiene_coordenadas` (bool).
        True si lat/lon están dentro de Guatemala y no son nulos.
        NO elimina ningún registro.
        """
        lat_ok = (
            df['latitud'].notna() &
            df['latitud'].between(self.LAT_MIN, self.LAT_MAX)
        )
        lon_ok = (
            df['longitud'].notna() &
            df['longitud'].between(self.LON_MIN, self.LON_MAX)
        )
        df['tiene_coordenadas'] = (lat_ok & lon_ok)

        validos = df['tiene_coordenadas'].sum()
        invalidos = len(df) - validos
        logger.info(f"Coordenadas válidas: {validos} | inválidas/fuera de Guatemala: {invalidos}")
        return df

    def _clean_financial_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convierte mora y capital_concedido a numérico; negativos → NaN → 0."""
        df['mora'] = pd.to_numeric(df['mora'], errors='coerce')
        df['capital_concedido'] = pd.to_numeric(df['capital_concedido'], errors='coerce')
        df.loc[df['mora'] < 0, 'mora'] = np.nan
        df.loc[df['capital_concedido'] < 0, 'capital_concedido'] = np.nan
        df['mora'] = df['mora'].fillna(0)
        df['capital_concedido'] = df['capital_concedido'].fillna(0)
        return df

    def _standarize_kpis(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normaliza valores de KPI al estándar del negocio."""
        df['KPI'] = df['KPI'].str.strip().str.upper()
        kpi_map = {
            'SANA': 'Sana',
            'COLOCACION': 'Colocación',
            'COLOCACIÓN': 'Colocación',
            'MORA1_30D': 'MORA1_30D',
            'MORA31_60D': 'Mora31_60D',
            'MORA61_90D': 'Mora61_90D',
            'CASTIGADO': 'Castigado',
            'MAYOR_90': 'Mora61_90D',
        }
        df['KPI'] = df['KPI'].map(kpi_map).fillna(df['KPI'])
        return df

    def _deduplicate_by_dpi(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Consolida múltiples créditos por DPI:
        - Suma mora y capital_concedido
        - Usa primera coordenada encontrada y su flag tiene_coordenadas
        - KPI: valor más frecuente (mode)
        - Resto de columnas: primer valor
        """
        initial_count = len(df)
        df = df.drop_duplicates()

        if 'dpi' not in df.columns or df['dpi'].duplicated().sum() == 0:
            logger.info("Sin duplicados por DPI")
            return df

        sum_cols = [c for c in ['mora', 'capital_concedido'] if c in df.columns]
        mode_cols = [c for c in ['KPI'] if c in df.columns]
        fixed_cols = [
            c for c in df.columns
            if c != 'dpi' and c not in sum_cols and c not in mode_cols
        ]

        agg_dict = {}
        for col in sum_cols:
            agg_dict[col] = 'sum'
        for col in mode_cols:
            agg_dict[col] = lambda x: x.mode()[0] if len(x.mode()) > 0 else x.iloc[0]
        for col in fixed_cols:
            agg_dict[col] = 'first'

        df = df.groupby('dpi', sort=False).agg(agg_dict).reset_index()

        logger.info(f"DPIs consolidados: {initial_count - len(df)} registros fusionados → {len(df)} clientes únicos")
        return df

    def _select_table_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Retiene solo las columnas que existen en la tabla destino."""
        cols_present = [c for c in TABLE_COLUMNS if c in df.columns]
        cols_missing = [c for c in TABLE_COLUMNS if c not in df.columns]
        if cols_missing:
            logger.warning(f"Columnas ausentes en el DataFrame (se omitirán): {cols_missing}")
        return df[cols_present]

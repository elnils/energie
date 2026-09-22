"""
Eurostat — end-user electricity and gas prices, households vs. industry.

What this adds
--------------
The dashboard tracked wholesale prices in detail (day-ahead, TTF, futures)
but nothing about what a household or a company actually pays. That is the
number most people mean by "Strompreis", and the gap between it and the
day-ahead price is the whole story of grid fees, levies and taxes.

Datasets (semi-annual, published ~5 months after each half-year):

    nrg_pc_204   electricity, household consumers
    nrg_pc_205   electricity, non-household (industry) consumers
    nrg_pc_202   gas, household consumers
    nrg_pc_203   gas, non-household (industry) consumers

For each we take the standard reference consumption band that Eurostat and
the national regulators quote:

    electricity households   2 500 – 4 999 kWh/year   (band DC)
    electricity industry       500 – 1 999 MWh/year   (band IC)
    gas households             20 – 199 GJ/year       (band D2)
    gas industry            10 000 – 99 999 GJ/year   (band I3)

and three tax levels, so the chart can show the components rather than one
opaque total:

    all taxes and levies included      what the customer pays
    excluding VAT and other recoverable taxes
    excluding all taxes and levies     the energy + network share

Band, tax level and currency codes are resolved from each dataset's own
published vocabulary (see fetchers/_eurostat.py) rather than hard-coded —
the same approach that fixed the ten permanently-empty series in
eurostat_oil, where guessed codes returned HTTP 200 with an empty cube.
"""
from typing import Dict, List, Optional, Tuple

from core import history
from . import _eurostat as eu

# Same reach as eurostat_oil. Each country costs one request per series and
# tax level, which the per-source budget in fetch_all.SCHEDULE allows for.
GEO = ['EU27_2020', 'EA20', 'DE', 'FR', 'IT', 'ES', 'NL', 'PL', 'BE', 'AT',
       'CZ', 'DK', 'SE', 'FI', 'PT', 'GR', 'IE', 'NO']

# Tax level intents. The order here is the order they are drawn in.
TAX_LEVELS: List[Tuple[str, Tuple[str, ...], Tuple[str, ...], Tuple[str, ...]]] = [
    ('incl_all_taxes', ('I_TAX',),
     (r'all taxes and levies included', r'^all taxes', r'taxes.*included'),
     (r'excluding',)),
    ('excl_vat', ('X_VAT', 'X_TAX_LEV'),
     (r'excluding vat', r'excluding.*recoverable'),
     ()),
    ('excl_taxes', ('X_TAX',),
     (r'excluding taxes and levies', r'excluding all taxes', r'^excluding taxes'),
     (r'vat',)),
]

# (output_key, dataset, label, band preferred codes, band label patterns, unit hint)
PRICE_SERIES: List[Tuple[str, str, str, Tuple[str, ...], Tuple[str, ...], str]] = [
    ('electricity_household', 'nrg_pc_204',
     'Strom Haushalte (2 500–4 999 kWh/Jahr)',
     ('KWH2500-4999', 'MWH2500-4999'),
     (r'2\s?500\s?kwh.*4\s?999', r'band dc', r'2500.*4999'),
     'EUR/kWh'),

    ('electricity_industry', 'nrg_pc_205',
     'Strom Industrie (500–1 999 MWh/Jahr)',
     ('MWH500-1999',),
     (r'500\s?mwh.*1\s?999', r'band ic', r'500.*1999'),
     'EUR/kWh'),

    ('gas_household', 'nrg_pc_202',
     'Gas Haushalte (20–199 GJ/Jahr)',
     ('GJ20-199',),
     (r'20\s?gj.*199', r'band d2', r'20.*199'),
     'EUR/kWh'),

    ('gas_industry', 'nrg_pc_203',
     'Gas Industrie (10 000–99 999 GJ/Jahr)',
     ('GJ10000-99999',),
     (r'10\s?000\s?gj.*99\s?999', r'band i3', r'10000.*99999'),
     'EUR/kWh'),
]

SINCE = '2015-S1'


def _band_dimension(catalog: Dict[str, Dict[str, str]]) -> Optional[str]:
    """
    Name of the consumption-band dimension.

    Eurostat has used `nrg_cons` and, on older revisions, `consom`. Picking
    it by inspection rather than by name means a rename costs nothing.
    """
    for candidate in ('nrg_cons', 'consom'):
        if candidate in catalog:
            return candidate
    for name, cats in catalog.items():
        if name in ('geo', 'TIME_PERIOD', 'time', 'freq', 'unit', 'currency',
                    'tax', 'product'):
            continue
        # A band dimension's labels mention an energy amount.
        if any('kwh' in v.lower() or 'gj' in v.lower() or 'mwh' in v.lower()
               for v in cats.values()):
            return name
    return None


def fetch() -> dict:
    out: Dict[str, dict] = {}
    resolved_log: Dict[str, dict] = {}
    any_success = False

    for key, dataset, label, band_codes, band_patterns, unit_hint in PRICE_SERIES:
        catalog = eu.describe(dataset)
        band_dim = _band_dimension(catalog)
        band = eu.resolve(catalog.get(band_dim, {}) if band_dim else {},
                          preferred_codes=band_codes,
                          label_patterns=band_patterns)
        # Price per kWh in euro. `currency` is its own dimension on current
        # revisions and folded into `unit` on older ones, so try both.
        unit = eu.resolve(catalog.get('unit', {}),
                          preferred_codes=('KWH', 'EUR_KWH'),
                          label_patterns=(r'kilowatt.?hour', r'per kwh'))
        currency = eu.resolve(catalog.get('currency', {}),
                              preferred_codes=('EUR',),
                              label_patterns=(r'^euro', r'\beuro\b'))

        if band_dim is None or not band:
            print(f'  ! eurostat_prices/{key}: no consumption band found in {dataset}')
            out[key] = {'label': label, 'dataset': dataset, 'series_per_country': {},
                        'unavailable_reason': f'{dataset}: consumption band not resolvable'}
            continue

        by_tax: Dict[str, Dict[str, List[dict]]] = {}
        for tax_key, tax_codes, tax_patterns, tax_excl in TAX_LEVELS:
            tax = eu.resolve(catalog.get('tax', {}), tax_codes, tax_patterns, tax_excl)
            if not tax:
                continue
            filters = {band_dim: band, 'tax': tax}
            if unit:
                filters['unit'] = unit
            if currency:
                filters['currency'] = currency
            per_country = eu.fetch_per_country(dataset, filters, GEO, since=SINCE)
            if per_country:
                any_success = True
                by_tax[tax_key] = per_country
            print(f'    eurostat_prices/{key}/{tax_key}: '
                  f'{len(per_country)} countries [{band_dim}={band} tax={tax}]')

        out[key] = {
            'label': label,
            'dataset': dataset,
            'unit': unit_hint,
            'band': band,
            'band_dimension': band_dim,
            'band_label': (catalog.get(band_dim) or {}).get(band, band),
            # `series_per_country` mirrors the tax level customers actually
            # pay, so consumers of this file that only want "the price" can
            # read it the same way they read every other Eurostat series.
            'series_per_country': by_tax.get('incl_all_taxes', {}),
            'by_tax': by_tax,
        }
        resolved_log[key] = {'band_dimension': band_dim, 'band': band,
                             'unit': unit, 'currency': currency,
                             'tax_levels': sorted(by_tax)}

    if not any_success:
        raise RuntimeError(
            'Eurostat Preise: keine Serie lieferte Daten — API nicht erreichbar '
            'oder Dimensions-Vokabular geändert.'
        )

    # ── Derived: what share of the household electricity price is tax/levy ──
    # The number people argue about, and it falls straight out of the two
    # tax levels we already fetched.
    tax_share: Dict[str, List[dict]] = {}
    incl = out.get('electricity_household', {}).get('by_tax', {}).get('incl_all_taxes', {})
    excl = out.get('electricity_household', {}).get('by_tax', {}).get('excl_taxes', {})
    for geo, incl_series in incl.items():
        excl_map = {p['period']: p['v'] for p in excl.get(geo, [])}
        points = []
        for p in incl_series:
            base = excl_map.get(p['period'])
            if base is None or not p['v']:
                continue
            points.append({'period': p['period'],
                           'v': round((p['v'] - base) / p['v'] * 100, 2)})
        if points:
            tax_share[geo] = points
    out['electricity_household_tax_share_pct'] = {
        'label': 'Steuer- und Abgabenanteil am Haushalts-Strompreis',
        'unit': '%',
        'series_per_country': tax_share,
        'derived_from': ['electricity_household.by_tax.incl_all_taxes',
                         'electricity_household.by_tax.excl_taxes'],
    }

    _record_history(out)

    empty = [k for k, v in out.items() if not v.get('series_per_country')]
    return {
        'data': out,
        'meta': {
            'source': 'Eurostat — nrg_pc_204/205 (Strom), nrg_pc_202/203 (Gas)',
            'license': 'Eurostat open data — reuse permitted (EC terms)',
            'note': ('Halbjahreswerte, Veröffentlichung ca. 5 Monate nach '
                     'Periodenende. Preise inkl. aller Steuern und Abgaben, '
                     'zusätzlich ohne MwSt. und ohne Steuern unter by_tax.'),
            'resolved': resolved_log,
            'empty_series': empty,
        },
    }


def _record_history(out: Dict[str, dict]) -> None:
    """
    Append the German reference prices to data/history/eurostat_prices.jsonl.

    Eurostat revises published half-years, so the snapshot is worth keeping:
    the history file is the only place a revision is visible after the fact.
    """
    record: Dict[str, float] = {}
    for key in ('electricity_household', 'electricity_industry',
                'gas_household', 'gas_industry'):
        series = out.get(key, {}).get('series_per_country', {}).get('DE') or []
        if series:
            record[f'de_{key}_eur_kwh'] = series[-1]['v']
            record[f'de_{key}_period'] = series[-1]['period']
    share = out.get('electricity_household_tax_share_pct', {}) \
               .get('series_per_country', {}).get('DE') or []
    if share:
        record['de_electricity_household_tax_share_pct'] = share[-1]['v']
    if record:
        history.record_history('eurostat_prices', record)

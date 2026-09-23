"""
Eurostat — EU energy statistics: oil products, gas, electricity.

  oil  → nrg_cb_oilm  (monthly oil supply/consumption by product and balance)
  gas  → nrg_cb_gasm  (monthly gas)
  elec → nrg_cb_e     (annual electricity)

Why this was rewritten
----------------------
Ten of the twenty-three series in this file were permanently empty, among
them every series the Treibstoff tab draws. The cause was not the API being
down: the requests asked for dimension codes that do not exist in these
datasets, and Eurostat answers that with HTTP 200 and an empty cube.

    nrg_bal=INTSTOCK / PRIM_PROD / FC_NE     no such balance codes
    siec=O4651_4652                           no such oil product code
    siec=E7011 / E7012 / E7100 / E7200        no such electricity codes

The three series that used real codes (IMP, EXP, STK_CHG on gas) worked
throughout, which is why the file always looked half-alive.

Series are now declared by *intent* — "kerosene-type jet fuel", "imports" —
and the codes are resolved against the dataset's own published vocabulary at
fetch time (see fetchers/_eurostat.py). Preferred codes are still listed
first so a correct guess costs no extra work; the label patterns are what
survives the next vocabulary revision. Whatever each pattern resolved to is
written to meta.resolved so a rename shows up as a changed code in the data
file rather than as a chart that quietly stops drawing.

Docs: https://wikis.ec.europa.eu/display/EUROSTATHELP/API+-+Getting+started+with+statistics+API
"""
from typing import Dict, List, Optional, Tuple

from . import _eurostat as eu

# Eurostat serves EU members plus EFTA and candidates, so this is as wide as
# these datasets go — a G20 comparison needs a second provider (IEA or
# national statistics); the US, China, India and Japan are simply not in
# them. One request per country per series, so the list size drives the
# run time that fetch_all.SCHEDULE budgets for.
GEO = ['EU27_2020', 'EA20', 'DE', 'FR', 'IT', 'ES', 'NL', 'PL', 'BE', 'AT',
       'CZ', 'DK', 'SE', 'FI', 'PT', 'EL', 'IE', 'NO']

# Data starts here for monthly datasets. Annual ones ignore it and return
# their full history, which is what the long-run import/export view wants.
SINCE_MONTHLY = '2015-01'

# ──────────────────────────────────────────────────────────────────────
# SIEC (product) intents
# key -> (preferred codes, label patterns, label exclusions)
# ──────────────────────────────────────────────────────────────────────
PRODUCTS: Dict[str, Tuple[Tuple[str, ...], Tuple[str, ...], Tuple[str, ...]]] = {
    'jet_fuel':  (('O4661XR5230B', 'O4661'),
                  (r'kerosene.?type jet fuel', r'\bjet fuel\b', r'kerosene'),
                  (r'kerosene.*lamp',)),
    'crude':     (('O4100_TOT_4200-2200', 'O4100_TOT', 'O4100'),
                  (r'^crude oil', r'crude oil.*ngl', r'\bcrude\b'),
                  ()),
    'diesel':    (('O4671XR5220B', 'O4671'),
                  (r'gas oil and diesel', r'\bdiesel\b'),
                  ()),
    'gasoline':  (('O4652XR5210B', 'O4652'),
                  (r'motor gasoline',),
                  (r'aviation',)),
    'heating':   (('O4680', 'O4669'),
                  (r'fuel oil', r'heating.*oil', r'residual fuel'),
                  ()),
    'gas':       (('G3000',),
                  (r'^natural gas$', r'natural gas'),
                  ()),
    'electricity': (('E7000',),
                    (r'^electricity$', r'electricity'),
                    ()),
    'elec_wind': (('RA300',), (r'\bwind\b',), ()),
    'elec_solar': (('RA400', 'RA420'), (r'solar(?!.*thermal)', r'solar'), ()),
    'elec_nuclear': (('N9000',), (r'nuclear',), ()),
    'elec_hydro': (('RA100',), (r'hydro',), (r'pumped',)),
}

# ──────────────────────────────────────────────────────────────────────
# nrg_bal (energy balance / flow) intents
# ──────────────────────────────────────────────────────────────────────
BALANCES: Dict[str, Tuple[Tuple[str, ...], Tuple[str, ...], Tuple[str, ...]]] = {
    'imports':     (('IMP',), (r'^imports$', r'\bimports\b'), ()),
    'exports':     (('EXP',), (r'^exports$', r'\bexports\b'), ()),
    'production':  (('PPRD', 'PRD'), (r'primary production', r'^production$'), ()),
    # A level and a change are different quantities and must never fall back
    # to one another. The first version of this table listed both under
    # 'stocks', so jet fuel resolved to STK_CHG and the dashboard drew
    # monthly stock CHANGES (65 of 138 values negative) under the heading
    # "Lagerbestände", with a traffic light computing "percent of the
    # five-year mean" from numbers that swing around zero.
    'stock_level': (('STK_CL', 'STK_LVL'),
                    (r'closing stock', r'stock level', r'stocks at end',
                     r'^stocks$'),
                    (r'change',)),
    'stock_change': (('STK_CHG',), (r'stock change', r'change in stock'), ()),
    'deliveries':  (('GID_OBS', 'GID_CAL'),
                    (r'gross inland deliveries.*observ',
                     r'gross inland deliver', r'gross inland consumption'), ()),
    'consumption': (('FC_E', 'FC'),
                    (r'final consumption.*energy use', r'^final consumption$',
                     r'final consumption'), ()),
    'gross_production': (('GEP', 'NEP'),
                         (r'gross electricity production',
                          r'net electricity production',
                          r'gross production', r'primary production'), ()),
}

# ──────────────────────────────────────────────────────────────────────
# SERIES = (output_key, dataset, product_intent, balance_intent, description)
# Output keys are unchanged: existing history and the dashboard both key off
# them, and nothing is dropped from the file.
# ──────────────────────────────────────────────────────────────────────
SERIES: List[Tuple[str, str, str, str, str]] = [
    ('oil_jet_fuel_stocks',      'nrg_cb_oilm', 'jet_fuel', 'stock_level',  'Jet fuel stocks'),
    ('oil_jet_fuel_stock_change','nrg_cb_oilm', 'jet_fuel', 'stock_change', 'Jet fuel stock change'),
    ('oil_jet_fuel_supply',      'nrg_cb_oilm', 'jet_fuel', 'production',   'Jet fuel production/supply'),
    ('oil_jet_fuel_imports',     'nrg_cb_oilm', 'jet_fuel', 'imports',      'Jet fuel imports'),
    ('oil_jet_fuel_exports',     'nrg_cb_oilm', 'jet_fuel', 'exports',      'Jet fuel exports'),
    ('oil_jet_fuel_consumption', 'nrg_cb_oilm', 'jet_fuel', 'deliveries',   'Jet fuel gross inland deliveries'),
    ('oil_crude_imports',        'nrg_cb_oilm', 'crude',    'imports',      'Crude oil imports'),
    ('oil_crude_production',     'nrg_cb_oilm', 'crude',    'production',   'Crude oil production'),
    ('oil_diesel_stocks',        'nrg_cb_oilm', 'diesel',   'stock_level',  'Diesel/gasoil stocks'),
    ('oil_motor_gasoline',       'nrg_cb_oilm', 'gasoline', 'stock_level',  'Motor gasoline stocks'),
    ('oil_heating_oil_stocks',   'nrg_cb_oilm', 'heating',  'stock_level',  'Heating/fuel oil stocks'),
    ('oil_diesel_stock_change',  'nrg_cb_oilm', 'diesel',   'stock_change', 'Diesel/gasoil stock change'),

    ('gas_production',           'nrg_cb_gasm', 'gas', 'production',   'Gas production'),
    ('gas_imports',              'nrg_cb_gasm', 'gas', 'imports',      'Gas imports'),
    ('gas_exports',              'nrg_cb_gasm', 'gas', 'exports',      'Gas exports'),
    ('gas_consumption',          'nrg_cb_gasm', 'gas', 'deliveries',   'Gas gross inland deliveries'),
    ('gas_stocks',               'nrg_cb_gasm', 'gas', 'stock_change', 'Gas stock change'),

    ('electricity_generation',   'nrg_cb_e', 'electricity',  'gross_production', 'Electricity generation'),
    ('electricity_imports',      'nrg_cb_e', 'electricity',  'imports',          'Electricity imports'),
    ('electricity_exports',      'nrg_cb_e', 'electricity',  'exports',          'Electricity exports'),
    ('electricity_consumption',  'nrg_cb_e', 'electricity',  'consumption',      'Electricity final consumption'),
    ('electricity_wind',         'nrg_cb_e', 'elec_wind',    'gross_production', 'Wind electricity generation'),
    ('electricity_solar',        'nrg_cb_e', 'elec_solar',   'gross_production', 'Solar electricity generation'),
    ('electricity_nuclear',      'nrg_cb_e', 'elec_nuclear', 'gross_production', 'Nuclear electricity generation'),
    ('electricity_hydro',        'nrg_cb_e', 'elec_hydro',   'gross_production', 'Hydro electricity generation'),
]

# Preferred unit per dataset, resolved against what the dataset offers.
UNITS: Dict[str, Tuple[Tuple[str, ...], Tuple[str, ...]]] = {
    'nrg_cb_oilm': (('THS_T',), (r'thousand tonnes',)),
    'nrg_cb_gasm': (('MIO_M3', 'TJ_GCV'), (r'million m', r'terajoule')),
    'nrg_cb_e':    (('GWH',), (r'gigawatt.?hour',)),
}


def _resolve_dataset(dataset: str) -> Dict[str, Dict[str, str]]:
    return eu.describe(dataset)


def fetch() -> dict:
    output: Dict[str, dict] = {}
    resolved_log: Dict[str, dict] = {}
    unresolved: List[str] = []
    any_success = False

    # Resolve the unit once per dataset — it is the same for every series.
    unit_by_dataset: Dict[str, Optional[str]] = {}
    unit_label_by_dataset: Dict[str, str] = {}
    for dataset, (codes, patterns) in UNITS.items():
        cat = _resolve_dataset(dataset).get('unit', {})
        code = eu.resolve(cat, preferred_codes=codes, label_patterns=patterns)
        unit_by_dataset[dataset] = code
        unit_label_by_dataset[dataset] = cat.get(code, code or '')
        print(f'    eurostat/{dataset}: unit -> {code} ({unit_label_by_dataset[dataset]})')

    # Print each dataset's balance vocabulary once. Without it, "no code for
    # nrg_bal" in the log gives no way to tell whether the concept is missing
    # or our pattern is wrong.
    for dataset in dict.fromkeys(d for _k, d, *_r in SERIES):
        cat = _resolve_dataset(dataset)
        bal = cat.get('nrg_bal', {})
        if bal:
            print(f'    eurostat/{dataset}: nrg_bal codes = ' +
                  ', '.join(f'{c}({l[:28]})' for c, l in list(bal.items())[:25]))
        # Same for the product dimension. Eight series still resolve to
        # nothing — the per-source electricity ones (wind, solar, nuclear,
        # hydro) and the production intents — and without seeing what siec
        # codes a dataset offers there is no way to tell a wrong pattern
        # from a breakdown the dataset does not carry.
        siec = cat.get('siec', {})
        if siec:
            print(f'    eurostat/{dataset}: {len(siec)} siec codes, e.g. ' +
                  ', '.join(f'{c}({l[:30]})' for c, l in list(siec.items())[:20]))

    for key, dataset, product_intent, balance_intent, desc in SERIES:
        catalog = _resolve_dataset(dataset)
        siec_codes, siec_patterns, siec_excl = PRODUCTS[product_intent]
        bal_codes, bal_patterns, bal_excl = BALANCES[balance_intent]

        siec = eu.resolve(catalog.get('siec', {}), siec_codes, siec_patterns, siec_excl)
        nrg_bal = eu.resolve(catalog.get('nrg_bal', {}), bal_codes, bal_patterns, bal_excl)
        unit = unit_by_dataset.get(dataset)

        if not siec or not nrg_bal:
            # The dataset genuinely has nothing matching this intent. Say so
            # in the file rather than sending a request that returns an empty
            # cube indistinguishable from an outage.
            missing = [n for n, v in (('siec', siec), ('nrg_bal', nrg_bal)) if not v]
            print(f'  ! eurostat/{key}: no code for {missing} in {dataset}')
            unresolved.append(key)
            output[key] = {
                'series_per_country': {}, 'description': desc, 'dataset': dataset,
                'unavailable_reason': f'{dataset} has no code for {", ".join(missing)}',
            }
            continue

        filters = {'siec': siec, 'nrg_bal': nrg_bal}
        if unit:
            filters['unit'] = unit
        since = SINCE_MONTHLY if dataset.endswith('m') else None

        per_country = eu.fetch_per_country(dataset, filters, GEO, since=since)
        if per_country:
            any_success = True
        total = sum(len(v) for v in per_country.values())
        print(f'    eurostat/{key}: {len(per_country)} countries, {total} pts '
              f'[siec={siec} nrg_bal={nrg_bal} unit={unit}]')

        output[key] = {
            'series_per_country': per_country,
            'description': desc,
            'dataset': dataset,
            'product': siec,
            'flow': nrg_bal,
            # 'level' is a quantity in store at a point in time, 'change' is
            # a monthly delta that can be negative. Consumers must not treat
            # them alike — the dashboard labels and charts them differently.
            'measure': 'change' if balance_intent == 'stock_change' else (
                       'level' if balance_intent == 'stock_level' else 'flow'),
            'flow_label': (catalog.get('nrg_bal') or {}).get(nrg_bal, nrg_bal),
            'unit': unit_label_by_dataset.get(dataset) or unit or '',
            'unit_code': unit,
        }
        resolved_log[key] = {'siec': siec, 'nrg_bal': nrg_bal, 'unit': unit}

    if not any_success:
        raise RuntimeError(
            'Eurostat: alle Serien leer — API nicht erreichbar oder '
            'Dimensions-Vokabular komplett geändert. '
            'Letzter Datensatz wird als stale beibehalten.'
        )

    empty = [k for k, v in output.items() if not v.get('series_per_country')]
    if empty:
        print(f'  ! eurostat: {len(empty)}/{len(output)} series empty: {empty}')

    return {
        'data': output,
        'meta': {
            'source':  'Eurostat Statistics API (ec.europa.eu/eurostat)',
            'license': 'Eurostat open data — reuse permitted (EC terms)',
            'url':     'https://ec.europa.eu/eurostat/databrowser/view/nrg_cb_oilm',
            'note': (
                'Dimension codes are resolved against each dataset\'s own '
                'published vocabulary at fetch time, not hard-coded. '
                'meta.resolved records what every series matched.'
            ),
            'resolved': resolved_log,
            'unresolved': unresolved,
            'empty_series': empty,
        },
    }

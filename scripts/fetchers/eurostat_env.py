"""
Eurostat — greenhouse gas emissions and derived heat.

Why this is its own fetcher
---------------------------
eurostat_oil declares every series as a (siec, nrg_bal) pair, which fits the
oil, gas and electricity balances. These two datasets do not:

    env_air_gge   emissions, keyed by air pollutant and CRF source sector
                  (airpol x src_crf), no siec at all
    nrg_cb_h      derived heat, which does use siec/nrg_bal but answers a
                  different question and belongs beside emissions in the UI

Forcing them into that table would have meant special-casing the dimension
names in the middle of a loop that is otherwise uniform. They share the same
discovery approach instead — codes are resolved against each dataset's own
published vocabulary by label (fetchers/_eurostat.py), never hard-coded —
and the resolved codes go into meta so a vocabulary change shows up as a
changed code rather than an empty chart.

Both datasets are annual. Emissions lag roughly two years (the inventory is
submitted in arrears), heat about one.
"""
from typing import Dict, List, Optional, Tuple

from core import history
from . import _eurostat as eu

# Same reach as the other Eurostat sources: EU members plus EFTA and
# candidates. Emissions are a smaller request count than the oil series
# (two datasets, few series) so the list can stay wide.
GEO = ['EU27_2020', 'DE', 'FR', 'IT', 'ES', 'NL', 'PL', 'BE', 'AT',
       'CZ', 'DK', 'SE', 'FI', 'PT', 'GR', 'IE', 'NO']

SINCE = '1990'

# ──────────────────────────────────────────────────────────────────────
# EMISSIONS — env_air_gge
# key -> (preferred codes, label patterns, exclusions)
# ──────────────────────────────────────────────────────────────────────
POLLUTANTS: Dict[str, Tuple[Tuple[str, ...], Tuple[str, ...], Tuple[str, ...]]] = {
    # The headline figure: all greenhouse gases in CO2 equivalent.
    'ghg_total': (('GHG', 'GHG_CO2E'),
                  (r'greenhouse gases.*co2 equivalent',
                   r'^greenhouse gases', r'total.*greenhouse'),
                  (r'indirect',)),
    'co2':       (('CO2',), (r'^carbon dioxide', r'\bCO2\b'), (r'equivalent', r'biomass')),
    'ch4':       (('CH4', 'CH4_CO2E'), (r'^methane', r'\bCH4\b'), ()),
    'n2o':       (('N2O', 'N2O_CO2E'), (r'nitrous oxide', r'\bN2O\b'), ()),
}

# CRF is a nested classification: CRF1 "Energy" CONTAINS CRF1A3 transport and
# CRF1A4B residential. Charting those four together double-counted — the
# first live run gave Germany 83.6% energy + 22.3% transport + 7.4% industry
# + 12.0% households = 125% of its own total.
#
# The `stack_*` intents below are the disjoint top-level split that actually
# sums to the total; the overlapping ones are kept because they are the
# figures people quote, and are labelled as subsets where they are shown.
SECTORS: Dict[str, Tuple[Tuple[str, ...], Tuple[str, ...], Tuple[str, ...]]] = {
    'total':      (('TOTX4_MEMO', 'TOTX4_MEMONIA', 'TOTXMEMO'),
                   (r'total.*excluding lulucf', r'^total'),
                   (r'including',)),
    # Overlapping aggregates — useful headline numbers, never stacked.
    'energy':     (('CRF1',), (r'^energy$',), (r'industries', r'combustion')),
    'households': (('CRF1A4B',), (r'^residential$', r'residential'), ()),
    # Disjoint split. CRF1A1+1A2+1A3+1A4+1B are the parts of Energy;
    # CRF2, CRF3 and CRF5 sit beside it.
    'stack_energy_ind': (('CRF1A1',),
                         (r'fuel combustion in energy industries',), ()),
    'stack_manufact':   (('CRF1A2',),
                         (r'fuel combustion in manufacturing industries',), ()),
    'stack_transport':  (('CRF1A3',), (r'^transport$', r'\btransport\b'),
                         (r'international', r'aviation')),
    'stack_other':      (('CRF1A4',), (r'other sectors',), (r'residential',)),
    'stack_fugitive':   (('CRF1B',), (r'fugitive emissions',), ()),
    'stack_processes':  (('CRF2',), (r'industrial processes',), ()),
    'stack_agri':       (('CRF3',), (r'^agriculture$', r'\bagriculture\b'), ()),
    'stack_waste':      (('CRF5',), (r'^waste$', r'\bwaste\b'), ()),
}

# (output key, pollutant intent, sector intent, description)
EMISSION_SERIES: List[Tuple[str, str, str, str]] = [
    ('ghg_total',       'ghg_total', 'total',      'Treibhausgase gesamt (ohne LULUCF)'),
    # Aggregates that overlap the split below — headline figures only.
    ('ghg_energy',      'ghg_total', 'energy',     'Treibhausgase aus Energie (enthält Verkehr und Haushalte)'),
    ('ghg_households',  'ghg_total', 'households', 'Treibhausgase aus Haushalten (Teil von Energie)'),
    # The disjoint split that sums to the total.
    ('ghg_s_energy_ind','ghg_total', 'stack_energy_ind', 'Energiewirtschaft'),
    ('ghg_s_manufact',  'ghg_total', 'stack_manufact',   'Verarbeitendes Gewerbe'),
    ('ghg_s_transport', 'ghg_total', 'stack_transport',  'Verkehr'),
    ('ghg_s_other',     'ghg_total', 'stack_other',      'Gebäude und übrige Sektoren'),
    ('ghg_s_fugitive',  'ghg_total', 'stack_fugitive',   'Diffuse Emissionen'),
    ('ghg_s_processes', 'ghg_total', 'stack_processes',  'Industrieprozesse'),
    ('ghg_s_agri',      'ghg_total', 'stack_agri',       'Landwirtschaft'),
    ('ghg_s_waste',     'ghg_total', 'stack_waste',      'Abfallwirtschaft'),
    ('co2_total',       'co2',       'total',      'CO₂ gesamt'),
    ('ch4_total',       'ch4',       'total',      'Methan gesamt'),
    ('n2o_total',       'n2o',       'total',      'Lachgas gesamt'),
]

# ──────────────────────────────────────────────────────────────────────
# HEAT — nrg_cb_h (derived heat: district heating and CHP output)
# ──────────────────────────────────────────────────────────────────────
HEAT_PRODUCT = (('H8000',), (r'^derived heat', r'\bheat\b'), (r'pump',))
HEAT_BALANCES: Dict[str, Tuple[Tuple[str, ...], Tuple[str, ...], Tuple[str, ...]]] = {
    'production':  (('GHP', 'PPRD'),
                    (r'gross heat production', r'^production', r'primary production'), ()),
    'consumption': (('FC_E', 'FC'),
                    (r'final consumption.*energy use', r'^final consumption'), ()),
    'households':  (('FC_OTH_HH_E',), (r'households',), ()),
    'industry':    (('FC_IND_E',), (r'industry.*energy use', r'^industry'), ()),
}

HEAT_SERIES: List[Tuple[str, str, str]] = [
    ('heat_production',  'production',  'Fernwärme-Erzeugung'),
    ('heat_consumption', 'consumption', 'Fernwärme-Endverbrauch'),
    ('heat_households',  'households',  'Fernwärme Haushalte'),
    ('heat_industry',    'industry',    'Fernwärme Industrie'),
]


def _emissions() -> Tuple[Dict[str, dict], Dict[str, dict]]:
    dataset = 'env_air_gge'
    catalog = eu.describe(dataset)
    out: Dict[str, dict] = {}
    resolved: Dict[str, dict] = {}

    # Emissions are reported in thousand tonnes of CO2 equivalent.
    unit = eu.resolve(catalog.get('unit', {}),
                      preferred_codes=('THS_T',),
                      label_patterns=(r'thousand tonnes',),
                      exclude_patterns=(r'per capita', r'index'))
    if catalog:
        for dim in ('airpol', 'src_crf', 'unit'):
            cat = catalog.get(dim, {})
            if cat:
                print(f'    eurostat/{dataset}: {len(cat)} {dim} codes, e.g. ' +
                      ', '.join(f'{c}({l[:34]})' for c, l in list(cat.items())[:10]))

    for key, pol_intent, sec_intent, desc in EMISSION_SERIES:
        pol = eu.resolve(catalog.get('airpol', {}), *POLLUTANTS[pol_intent])
        sec = eu.resolve(catalog.get('src_crf', {}), *SECTORS[sec_intent])
        if not pol or not sec:
            missing = [n for n, v in (('airpol', pol), ('src_crf', sec)) if not v]
            print(f'  ! eurostat_env/{key}: no code for {missing}')
            out[key] = {'series_per_country': {}, 'description': desc,
                        'dataset': dataset,
                        'unavailable_reason': f'{dataset}: kein Code für {", ".join(missing)}'}
            continue
        filters = {'airpol': pol, 'src_crf': sec}
        if unit:
            filters['unit'] = unit
        per_country = eu.fetch_per_country(dataset, filters, GEO, since=SINCE)
        total = sum(len(v) for v in per_country.values())
        print(f'    eurostat_env/{key}: {len(per_country)} Länder, {total} Punkte '
              f'[airpol={pol} src_crf={sec}]')
        out[key] = {
            'series_per_country': per_country, 'description': desc,
            'dataset': dataset, 'airpol': pol, 'src_crf': sec,
            'unit': (catalog.get('unit') or {}).get(unit, unit or ''),
            'sector_label': (catalog.get('src_crf') or {}).get(sec, sec),
        }
        resolved[key] = {'airpol': pol, 'src_crf': sec, 'unit': unit}
    return out, resolved


def _heat() -> Tuple[Dict[str, dict], Dict[str, dict]]:
    dataset = 'nrg_cb_h'
    catalog = eu.describe(dataset)
    out: Dict[str, dict] = {}
    resolved: Dict[str, dict] = {}

    siec = eu.resolve(catalog.get('siec', {}), *HEAT_PRODUCT)
    unit = eu.resolve(catalog.get('unit', {}),
                      preferred_codes=('GWH', 'TJ'),
                      label_patterns=(r'gigawatt.?hour', r'terajoule'))
    if catalog:
        bal = catalog.get('nrg_bal', {})
        if bal:
            print(f'    eurostat/{dataset}: {len(bal)} nrg_bal codes, e.g. ' +
                  ', '.join(f'{c}({l[:34]})' for c, l in list(bal.items())[:10]))

    for key, bal_intent, desc in HEAT_SERIES:
        bal = eu.resolve(catalog.get('nrg_bal', {}), *HEAT_BALANCES[bal_intent])
        if not siec or not bal:
            missing = [n for n, v in (('siec', siec), ('nrg_bal', bal)) if not v]
            print(f'  ! eurostat_env/{key}: no code for {missing}')
            out[key] = {'series_per_country': {}, 'description': desc,
                        'dataset': dataset,
                        'unavailable_reason': f'{dataset}: kein Code für {", ".join(missing)}'}
            continue
        filters = {'siec': siec, 'nrg_bal': bal}
        if unit:
            filters['unit'] = unit
        per_country = eu.fetch_per_country(dataset, filters, GEO, since=SINCE)
        total = sum(len(v) for v in per_country.values())
        print(f'    eurostat_env/{key}: {len(per_country)} Länder, {total} Punkte '
              f'[siec={siec} nrg_bal={bal}]')
        out[key] = {
            'series_per_country': per_country, 'description': desc,
            'dataset': dataset, 'product': siec, 'flow': bal,
            'unit': (catalog.get('unit') or {}).get(unit, unit or ''),
        }
        resolved[key] = {'siec': siec, 'nrg_bal': bal, 'unit': unit}
    return out, resolved


def fetch() -> dict:
    emissions, res_e = _emissions()
    heat, res_h = _heat()

    out: Dict[str, dict] = {}
    out.update(emissions)
    out.update(heat)

    if not any(v.get('series_per_country') for v in out.values()):
        raise RuntimeError(
            'Eurostat Umwelt: weder Emissionen noch Wärme lieferten Daten — '
            'API nicht erreichbar oder Vokabular komplett geändert.'
        )

    # Derived: emissions per unit of energy is not published, but the share
    # of the total that each sector accounts for is worth having and falls
    # straight out of what we fetched.
    shares: Dict[str, List[dict]] = {}
    total_by_geo = out.get('ghg_total', {}).get('series_per_country', {})
    for sector_key in [k for k, *_ in
                       [(e[0],) for e in EMISSION_SERIES] if k.startswith('ghg_s_')]:
        node = out.get(sector_key, {}).get('series_per_country', {})
        for geo, series in node.items():
            totals = {p['period']: p['v'] for p in total_by_geo.get(geo, [])}
            pts = [{'period': p['period'],
                    'v': round(p['v'] / totals[p['period']] * 100, 2)}
                   for p in series
                   if totals.get(p['period']) not in (None, 0)]
            if pts:
                shares.setdefault(sector_key.replace('ghg_s_', ''), {})[geo] = pts
    out['ghg_sector_share_pct'] = {
        'description': 'Anteil der Sektoren an den Gesamtemissionen',
        'unit': '%',
        'by_sector': shares,
        'derived_from': ['ghg_total'] + [e[0] for e in EMISSION_SERIES
                                         if e[0].startswith('ghg_s_')],
    }

    _record_history(out)

    empty = [k for k, v in out.items()
             if 'series_per_country' in v and not v['series_per_country']]
    return {
        'data': out,
        'meta': {
            'source': 'Eurostat — env_air_gge (Treibhausgase), nrg_cb_h (Fernwärme)',
            'license': 'Eurostat open data — reuse permitted (EC terms)',
            'note': ('Jahreswerte. Treibhausgas-Inventare erscheinen mit rund zwei '
                     'Jahren Verzug, Wärmebilanzen mit etwa einem. Codes werden '
                     'gegen das veröffentlichte Vokabular aufgelöst, nicht fest '
                     'verdrahtet; meta.resolved hält fest, was getroffen wurde.'),
            'resolved': {**res_e, **res_h},
            'empty_series': empty,
        },
    }


def _record_history(out: Dict[str, dict]) -> None:
    """German headline figures, so a later Eurostat revision stays visible."""
    record: Dict[str, float] = {}
    for key in ('ghg_total', 'ghg_energy', 'ghg_s_transport', 'heat_production'):
        series = out.get(key, {}).get('series_per_country', {}).get('DE') or []
        if series:
            record[f'de_{key}'] = series[-1]['v']
            record[f'de_{key}_period'] = series[-1]['period']
    if record:
        history.record_history('eurostat_env', record)

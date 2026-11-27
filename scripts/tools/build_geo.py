"""
Baut die Kartengeometrie für das Dashboard (einmalig, nicht im Cronlauf).

Quelle: Eurostat GISCO, NUTS 2021, Maßstab 1:20 Mio., WGS84.
  Länder:        NUTS_RG_20M_2021_4326_LEVL_0.geojson
  Bundesländer:  NUTS_RG_20M_2021_4326_LEVL_1.geojson (CNTR_CODE = DE)
  Kreise:        NUTS_RG_10M_2024_4326_LEVL_3.geojson (CNTR_CODE = DE), dazu die
                 Ländergrenzen im selben Maßstab als Linien darüber
Lizenz: © EuroGeographics für die Verwaltungsgrenzen.

Die Polygone werden hier schon in SVG-Pfade umgerechnet (flächentreue
Näherung: Längengrade mit cos(Mittelbreite) gestaucht), damit die Seite
keine Kartenbibliothek braucht. Teile außerhalb des Kartenausschnitts
(Übersee-Départements, Kanaren usw.) fallen weg.

    python scripts/tools/build_geo.py
"""
import json
import math
import os
import urllib.request

BASE = 'https://gisco-services.ec.europa.eu/distribution/v2/nuts/geojson/'
OUT = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'geo')


def load(level, scale='20M', year='2021'):
    with urllib.request.urlopen(f'{BASE}NUTS_RG_{scale}_{year}_4326_LEVL_{level}.geojson', timeout=120) as r:
        return json.load(r)


def build(features, box, width, key, name_key='NAME_LATN', tol=0.6):
    lon0, lat0, lon1, lat1 = box
    k = math.cos(math.radians((lat0 + lat1) / 2))
    sx = width / ((lon1 - lon0) * k)
    height = (lat1 - lat0) * sx

    def proj(lon, lat):
        return ((lon - lon0) * k * sx, (lat1 - lat) * sx)

    def ring_path(ring):
        pts, last = [], None
        for lon, lat in ring:
            x, y = proj(lon, lat)
            if last and abs(x - last[0]) < tol and abs(y - last[1]) < tol:
                continue
            pts.append((x, y))
            last = (x, y)
        if len(pts) < 4:
            return ''
        return 'M' + 'L'.join(f'{x:.1f},{y:.1f}' for x, y in pts) + 'Z'

    out = {}
    for f in features:
        g = f['geometry']
        polys = g['coordinates'] if g['type'] == 'MultiPolygon' else [g['coordinates']]
        parts = []
        for poly in polys:
            outer = poly[0]
            cx = sum(p[0] for p in outer) / len(outer)
            cy = sum(p[1] for p in outer) / len(outer)
            if not (lon0 <= cx <= lon1 and lat0 <= cy <= lat1):
                continue
            parts += [ring_path(r) for r in poly]
        d = ''.join(p for p in parts if p)
        if d:
            p = f['properties']
            out[p[key]] = {'name': p[name_key], 'd': d}
    return {'width': width, 'height': round(height, 1), 'box': box, 'shapes': out,
            'source': 'Eurostat GISCO NUTS 2021 (1:20 Mio.), © EuroGeographics'}


def main():
    os.makedirs(OUT, exist_ok=True)
    l0 = load(0)['features']
    eu = build([f for f in l0 if f['properties']['CNTR_CODE'] not in ('TR', 'CY')],
               (-11.0, 34.5, 32.0, 71.2), 600, 'CNTR_CODE')
    l1 = load(1)['features']
    de = build([f for f in l1 if f['properties']['CNTR_CODE'] == 'DE'],
               (5.7, 47.2, 15.2, 55.1), 420, 'NUTS_ID', tol=0.4)
    # Kreise (NUTS 3) feiner als die Länderkarte; Ländergrenzen im selben Maßstab,
    # damit Linien und Flächen exakt aufeinanderliegen
    box_de = (5.7, 47.2, 15.2, 55.1)
    l3 = load(3, '10M', '2024')['features']
    kreise = build([f for f in l3 if f['properties']['CNTR_CODE'] == 'DE'], box_de, 420, 'NUTS_ID', tol=0.35)
    l1b = load(1, '10M', '2024')['features']
    laender = build([f for f in l1b if f['properties']['CNTR_CODE'] == 'DE'], box_de, 420, 'NUTS_ID', tol=0.35)
    kreise['laender'] = ''.join(v['d'] for v in laender['shapes'].values())
    kreise['source'] = 'Eurostat GISCO NUTS 2024 (1:10 Mio.), © EuroGeographics'
    for name, data in (('europa', eu), ('deutschland', de), ('kreise', kreise)):
        fp = os.path.join(OUT, f'{name}.json')
        with open(fp, 'w', encoding='utf-8') as fh:
            json.dump(data, fh, ensure_ascii=False, separators=(',', ':'))
        print(name, len(data['shapes']), 'Flächen,', os.path.getsize(fp) // 1024, 'KB')


if __name__ == '__main__':
    main()

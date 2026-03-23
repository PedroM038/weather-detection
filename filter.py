"""
Filtra dados horários das estações INMET com base nos eventos de chuva
extrema definidos em events.csv. Gera um CSV por evento/região contendo
apenas o recorte temporal (delta de 4 meses), preservando a ordem
cronológica da série.
"""

import os
import re
import csv
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
EVENTS_FILE = os.path.join(BASE_DIR, "events.csv")
STATIONS_FILE = os.path.join(BASE_DIR, "stations.csv")
OUTPUT_DIR = os.path.join(BASE_DIR, "filtered")
DATA_DIRS = [os.path.join(BASE_DIR, y) for y in ("2022", "2023", "2024", "2025")]

INMET_ENCODING = "latin-1"
INMET_METADATA_LINES = 8          # linhas 0-7: metadados; linha 8: cabeçalho
INMET_SEPARATOR = ";"

# Padrão do nome: INMET_{região}_{UF}_{código}_{cidade}_{dd-mm-yyyy}_A_{dd-mm-yyyy}.CSV
FILE_PATTERN = re.compile(
    r"^INMET_[A-Z]{1,2}_([A-Z]{2})_([A-Z0-9]+)_(.+?)_\d{2}-\d{2}-\d{4}_A_\d{2}-\d{2}-\d{4}\.CSV$",
    re.IGNORECASE,
)


# ── parsing de events.csv ──────────────────────────────────────────────
def _parse_date_range(text: str) -> tuple[datetime, datetime]:
    """Converte '28/08/2022 até 28/12/2022' em (datetime_ini, datetime_fim)."""
    parts = text.split("até")
    fmt = "%d/%m/%Y"
    start = datetime.strptime(parts[0].strip(), fmt)
    end = datetime.strptime(parts[1].strip(), fmt)
    return start, end


def parse_events(events_path: str, stations_path: str) -> list[dict]:
    """
    Lê events.csv e stations.csv, cruzando as duas tabelas
    retornando uma lista de dicts prontos para filtragem.
    """
    # ─── Tabela 1: eventos e datas (events.csv) ───────────────────────
    events_by_region: dict[str, dict] = {}
    with open(events_path, encoding="utf-8", newline="") as f:
        reader1 = csv.reader(f)
        next(reader1)  # pula cabeçalho
        for row in reader1:
            if len(row) < 5:
                continue
            region = row[0].strip()
            state = row[1].strip().upper()
            date_range = row[4].strip()
            start, end = _parse_date_range(date_range)
            events_by_region[region] = {
                "region": region,
                "state": state,
                "start": start,
                "end": end,
            }

    # ─── Tabela 2: estações selecionadas (stations.csv) ───────────────
    events: list[dict] = []
    with open(stations_path, encoding="utf-8", newline="") as f:
        reader2 = csv.reader(f)
        next(reader2)  # pula cabeçalho
        for row in reader2:
            if len(row) < 5:
                continue
            region = row[0].strip()
            code = row[3].strip().upper()
            years = [y.strip() for y in row[4].split(",")]

            base = events_by_region.get(region)
            if base is None:
                continue
            events.append({**base, "code": code, "years": years})

    return events


# ── busca e leitura de arquivos INMET ──────────────────────────────────
def find_inmet_files(code: str, years: list[str]) -> list[str]:
    """Encontra os caminhos dos CSVs do INMET para dado código e anos."""
    paths = []
    for data_dir in DATA_DIRS:
        if not os.path.isdir(data_dir):
            continue
        folder_year = os.path.basename(data_dir)
        if folder_year not in years:
            continue
        for fname in os.listdir(data_dir):
            match = FILE_PATTERN.match(fname)
            if match and match.group(2).upper() == code:
                paths.append(os.path.join(data_dir, fname))
    return sorted(paths)


def read_inmet_csv(path: str) -> tuple[list[str], list[list[str]]]:
    """
    Lê um CSV do INMET e retorna (cabeçalho, linhas_de_dados).
    Cada linha de dados é uma lista de campos (split por ';').
    """
    with open(path, encoding=INMET_ENCODING) as f:
        lines = f.readlines()

    header_line = lines[INMET_METADATA_LINES]
    header = [c.strip() for c in header_line.strip().rstrip(";").split(INMET_SEPARATOR)]

    rows = []
    for line in lines[INMET_METADATA_LINES + 1:]:
        line = line.strip()
        if not line:
            continue
        fields = [c.strip() for c in line.rstrip(";").split(INMET_SEPARATOR)]
        rows.append(fields)
    return header, rows


def parse_row_datetime(row: list[str]) -> datetime:
    """Extrai datetime a partir das colunas Data e Hora UTC de uma linha."""
    date_str = row[0]            # ex: 2022/01/01
    hour_str = row[1]            # ex: 0000 UTC
    hour_str = hour_str.replace(" UTC", "").strip()
    return datetime.strptime(f"{date_str} {hour_str}", "%Y/%m/%d %H%M")


def filter_rows(
    rows: list[list[str]], start: datetime, end: datetime
) -> list[list[str]]:
    """Filtra linhas dentro do intervalo [start, end]."""
    filtered = []
    for row in rows:
        try:
            dt = parse_row_datetime(row)
        except (ValueError, IndexError):
            continue
        if start <= dt <= end:
            filtered.append(row)
    return filtered


# ── escrita do CSV filtrado ────────────────────────────────────────────
def sanitize_filename(name: str) -> str:
    """Remove caracteres problemáticos para nomes de arquivo."""
    name = name.lower().replace(" ", "_").replace("/", "-")
    return re.sub(r"[^\w\-]", "", name)


def write_filtered_csv(
    output_path: str, header: list[str], rows: list[list[str]]
) -> None:
    """Grava CSV filtrado com encoding UTF-8 e separador ';'."""
    with open(output_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, delimiter=";")
        writer.writerow(header)
        for row in rows:
            writer.writerow(row)


# ── main ───────────────────────────────────────────────────────────────
def main():
    events = parse_events(EVENTS_FILE, STATIONS_FILE)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print("=" * 70)
    print("FILTRAGEM DE DADOS INMET POR EVENTO DE CHUVA EXTREMA")
    print("=" * 70)

    total_rows = 0

    for ev in events:
        region = ev["region"]
        code = ev["code"]
        start = ev["start"]
        end = ev["end"]
        years = ev["years"]

        print(f"\n▸ {region} ({ev['state']})")
        print(f"  Estação: {code} | Período: {start:%d/%m/%Y} – {end:%d/%m/%Y}")
        print(f"  Anos buscados: {', '.join(years)}")

        paths = find_inmet_files(code, years)
        if not paths:
            print("  ⚠ Nenhum arquivo INMET encontrado para esta estação/anos.")
            continue
        print(f"  Arquivos encontrados: {len(paths)}")

        # Lê e concatena dados de múltiplos anos
        header = None
        all_rows: list[list[str]] = []
        for p in paths:
            h, rows = read_inmet_csv(p)
            if header is None:
                header = h
            all_rows.extend(rows)

        # Filtra pelo intervalo temporal
        filtered = filter_rows(all_rows, start, end)

        # Garante ordem cronológica
        filtered.sort(key=parse_row_datetime)

        fname = f"{sanitize_filename(region)}_{ev['state']}_{code}.csv"
        out_path = os.path.join(OUTPUT_DIR, fname)
        write_filtered_csv(out_path, header, filtered)

        total_rows += len(filtered)
        print(f"  ✓ {len(filtered)} registros → {fname}")

    print("\n" + "=" * 70)
    print(f"TOTAL: {total_rows} registros filtrados em {OUTPUT_DIR}/")
    print("=" * 70)


if __name__ == "__main__":
    main()

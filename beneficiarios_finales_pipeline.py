"""Pipeline para extraer Beneficiarios Finales desde PDFs usando OCR.

Este módulo está diseñado para ejecutarse en un entorno Databricks o local
con PySpark disponible. Incluye utilidades para convertir PDFs en imágenes,
extraer tablas mediante OCR con pytesseract (IA basada en visión) y
conciliar los resultados contra una tabla maestra de beneficiarios.
"""
from __future__ import annotations

import re
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Sequence
from urllib.parse import urljoin

import fitz  # type: ignore
import pandas as pd
from PIL import Image
import requests
import numpy as np

try:
    import pytesseract
except ImportError as exc:  # pragma: no cover - dependencia opcional
    raise ImportError(
        "pytesseract es obligatorio para ejecutar la extracción OCR. "
        "Instálalo con `pip install pytesseract` y asegúrate de tener "
        "Tesseract OCR disponible en tu sistema."
    ) from exc

try:
    from pyspark.sql import DataFrame as SparkDataFrame
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
except ImportError:  # pragma: no cover - permite importación fuera de Spark
    SparkSession = None  # type: ignore
    SparkDataFrame = None  # type: ignore
    F = None  # type: ignore


RUT_PATTERN = re.compile(r"\b(\d{1,2}\.?\d{3}\.?\d{3}-[\dkK])\b")
PCT_PATTERN = re.compile(r"\b(\d{1,3}(?:[\.,]\d+)?\s*%)\b")

DEFAULT_SAMPLE_SOURCE = "https://www.cmfchile.cl/portal/principal/613/w3-propertyvalue-46350.html"


@dataclass
class ExtractionResult:
    """Representa una fila extraída desde la tabla de Beneficiarios."""

    origen_pdf: str
    pagina: int
    rut: str
    porcentaje_participacion: float
    ruta_imagen: str
    texto_fila: str


def discover_pdf_links(html: str, base_url: str) -> List[str]:
    """Extrae enlaces a PDFs desde un documento HTML."""

    pattern = re.compile(r"href=\"([^\"]+\.pdf(?:\?[^\"]*)?)\"", re.IGNORECASE)
    links: List[str] = []
    seen = set()
    for match in pattern.findall(html):
        absolute = urljoin(base_url, match)
        if absolute not in seen:
            links.append(absolute)
            seen.add(absolute)
    return links


def download_public_beneficiary_pdfs(
    destination: Path,
    limit: int = 50,
    catalog_urls: Iterable[str] | None = None,
    timeout: float = 30.0,
) -> List[Path]:
    """Descarga PDFs de beneficiarios finales desde la web hacia una carpeta local.

    Args:
        destination: carpeta donde se guardarán los PDFs.
        limit: número máximo de archivos a descargar.
        catalog_urls: URLs con listados HTML que contengan enlaces a PDFs.
            Si es None se usa :data:`DEFAULT_SAMPLE_SOURCE`.
        timeout: tiempo máximo de espera por cada solicitud HTTP.

    Returns:
        Lista de rutas a los archivos descargados.
    """

    destination.mkdir(parents=True, exist_ok=True)

    seeds = list(catalog_urls or [DEFAULT_SAMPLE_SOURCE])
    session = requests.Session()
    pdf_links: List[str] = []
    pending_pages: List[str] = list(seeds)
    visited_pages = set()

    max_pages = 30
    while pending_pages and len(pdf_links) < limit and len(visited_pages) < max_pages:
        current = pending_pages.pop(0)
        if current in visited_pages:
            continue
        try:
            response = session.get(current, timeout=timeout)
            response.raise_for_status()
        except requests.RequestException as exc:
            print(f"Advertencia: no se pudo acceder a {current}: {exc}")
            visited_pages.add(current)
            continue

        html = response.text
        visited_pages.add(current)

        new_links = discover_pdf_links(html, current)
        for link in new_links:
            if link not in pdf_links:
                pdf_links.append(link)
                if len(pdf_links) >= limit:
                    break

        if len(pdf_links) >= limit:
            break

        # Descubrir nuevas páginas relacionadas dentro del mismo dominio.
        related_pattern = re.compile(r"href=\"([^\"]+w3-propertyvalue-[^\"]+\.html)\"", re.IGNORECASE)
        for extra in related_pattern.findall(html):
            candidate = urljoin(current, extra)
            if candidate not in visited_pages and candidate not in pending_pages and candidate not in seeds:
                pending_pages.append(candidate)

    if not pdf_links:
        raise RuntimeError(
            "No se encontraron enlaces a PDFs en las URLs proporcionadas. "
            "Actualiza catalog_urls con páginas que contengan listas públicas."
        )

    descargados: List[Path] = []
    for index, pdf_url in enumerate(pdf_links[:limit], start=1):
        nombre_base = pdf_url.split("/")[-1].split("?")[0] or f"beneficiarios_{index:03d}.pdf"
        nombre_base = re.sub(r"[^A-Za-z0-9._-]", "_", nombre_base)
        destino_final = destination / nombre_base
        if destino_final.exists():
            destino_final = destination / f"{destino_final.stem}_{index:03d}{destino_final.suffix}"

        try:
            with session.get(pdf_url, timeout=timeout, stream=True) as descarga:
                descarga.raise_for_status()
                with open(destino_final, "wb") as archivo:
                    for chunk in descarga.iter_content(chunk_size=8192):
                        if chunk:
                            archivo.write(chunk)
        except requests.RequestException as exc:
            print(f"Advertencia: no se pudo descargar {pdf_url}: {exc}")
            continue

        descargados.append(destino_final)

    return descargados


def convert_pdf_to_images(
    pdf_path: Path,
    output_dir: Path,
    dpi: int = 200,
    overwrite: bool = False,
) -> List[Path]:
    """Convierte un PDF a imágenes PNG por página.

    Args:
        pdf_path: ruta del archivo PDF de entrada.
        output_dir: carpeta donde se guardarán las imágenes.
        dpi: resolución objetivo para el renderizado.
        overwrite: si es True, elimina imágenes existentes de un proceso previo.

    Returns:
        Lista de rutas a las imágenes generadas.
    """

    if not pdf_path.exists():
        raise FileNotFoundError(f"No se encontró el PDF {pdf_path}")

    output_dir.mkdir(parents=True, exist_ok=True)

    if overwrite and output_dir.exists():
        shutil.rmtree(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

    zoom = dpi / 72.0
    matrix = fitz.Matrix(zoom, zoom)
    generated_images: List[Path] = []

    with fitz.open(pdf_path) as pdf_doc:
        for page_index in range(pdf_doc.page_count):
            page = pdf_doc.load_page(page_index)
            pix = page.get_pixmap(matrix=matrix, alpha=False)
            image_path = output_dir / f"page_{page_index + 1:04d}.png"
            img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
            img.save(image_path, format="PNG", optimize=True)
            generated_images.append(image_path)

    return generated_images


def _ocr_page(image_path: Path, lang: str = "spa") -> pd.DataFrame:
    """Ejecuta OCR sobre una imagen y retorna el DataFrame crudo de pytesseract."""

    image = Image.open(image_path)
    df = pytesseract.image_to_data(image, lang=lang, output_type=pytesseract.Output.DATAFRAME)
    df = df.dropna(subset=["text"]).copy()
    df["text"] = df["text"].str.strip()
    df = df[df["text"] != ""]
    return df


def _extraer_lineas(df: pd.DataFrame) -> List[str]:
    """Agrupa las detecciones OCR por línea para facilitar su análisis."""

    lineas: List[str] = []
    for (_, line_df) in df.groupby(["block_num", "par_num", "line_num"], sort=True):
        texto = " ".join(token for token in line_df["text"] if token)
        if texto:
            lineas.append(texto)
    return lineas


def _normalizar_rut(rut: str) -> str:
    """Normaliza un RUT removiendo signos y rellenando con ceros hasta 11 caracteres."""

    limpio = rut.replace(".", "").replace(" ", "").upper()
    if "-" not in limpio:
        raise ValueError(f"Formato de RUT no reconocido: {rut}")

    cuerpo, dv = limpio.split("-")
    if not cuerpo or not dv:
        raise ValueError(f"Formato de RUT no reconocido: {rut}")

    cuerpo_num = re.sub(r"[^0-9]", "", cuerpo)
    dv = dv.strip()
    if not cuerpo_num or not dv:
        raise ValueError(f"Formato de RUT no reconocido: {rut}")

    cuerpo_formateado = cuerpo_num.zfill(10)
    dv_normalizado = dv[-1].upper()
    return f"{cuerpo_formateado}{dv_normalizado}"


def _normalizar_rut_flexible(rut: object) -> str | None:
    """Normaliza un RUT sin fallar cuando el formato difiere del esperado."""

    if rut is None:
        return None

    if isinstance(rut, float) and np.isnan(rut):
        return None

    rut_str = str(rut).strip()
    if not rut_str:
        return None

    try:
        return _normalizar_rut(rut_str)
    except ValueError:
        limpio = re.sub(r"[^0-9Kk]", "", rut_str.upper())
        if len(limpio) < 2:
            return None
        cuerpo, dv = limpio[:-1], limpio[-1]
        cuerpo_formateado = cuerpo.zfill(10)
        return f"{cuerpo_formateado}{dv}"


def _parsear_porcentaje(pct: str) -> float:
    """Convierte un string de porcentaje a float (0-100)."""

    pct = pct.replace("%", "").replace(" ", "")
    pct = pct.replace(",", ".")
    return float(pct)


def extract_beneficiarios_from_image(
    image_path: Path,
    origen_pdf: str,
    pagina: int,
    section_keyword: str = "Beneficiarios Finales",
    rut_pattern: re.Pattern[str] = RUT_PATTERN,
    pct_pattern: re.Pattern[str] = PCT_PATTERN,
    lang: str = "spa",
) -> List[ExtractionResult]:
    """Extrae filas de la tabla de Beneficiarios en una imagen.

    Se basa en OCR (IA de visión) para detectar texto, busca líneas que
    contengan un RUT y un porcentaje, y retorna pares normalizados.

    Args:
        image_path: imagen de entrada generada desde el PDF.
        origen_pdf: nombre base del PDF (sin extensión).
        pagina: número de página original.
        section_keyword: texto que delimita la sección objetivo.
        rut_pattern: patrón regex para localizar RUTs.
        pct_pattern: patrón regex para localizar porcentajes.
        lang: idioma para el OCR de pytesseract.
    """

    df = _ocr_page(image_path, lang=lang)
    lineas = _extraer_lineas(df)

    resultados: List[ExtractionResult] = []
    seccion_activa = False
    palabra_clave = section_keyword.lower()

    for linea in lineas:
        texto_minus = linea.lower()
        if palabra_clave in texto_minus:
            seccion_activa = True

        if not seccion_activa:
            continue

        ruts = rut_pattern.findall(linea)
        porcentajes = pct_pattern.findall(linea)
        if not ruts or not porcentajes:
            continue

        # Emparejar por posición: asumimos un porcentaje por RUT.
        for idx, rut_encontrado in enumerate(ruts):
            pct_match = porcentajes[min(idx, len(porcentajes) - 1)]
            try:
                resultado = ExtractionResult(
                    origen_pdf=origen_pdf,
                    pagina=pagina,
                    rut=_normalizar_rut(rut_encontrado),
                    porcentaje_participacion=_parsear_porcentaje(pct_match),
                    ruta_imagen=str(image_path),
                    texto_fila=linea,
                )
                resultados.append(resultado)
            except (ValueError, IndexError):
                continue

    return resultados


def extract_beneficiarios_from_pdf(
    pdf_path: Path,
    output_root: Path,
    dpi: int = 200,
    lang: str = "spa",
) -> List[ExtractionResult]:
    """Convierte un PDF a imágenes y extrae los beneficiarios de cada página.

    Args:
        pdf_path: archivo PDF de entrada.
        output_root: carpeta raíz donde se crearán subcarpetas con las imágenes.
        dpi: resolución de renderizado.
        lang: idioma a utilizar por pytesseract (por defecto español).
    """

    nombre_pdf = pdf_path.stem
    subfolder = output_root / nombre_pdf
    images = convert_pdf_to_images(pdf_path, subfolder, dpi=dpi)

    resultados: List[ExtractionResult] = []
    for idx, image_path in enumerate(images, start=1):
        try:
            resultados.extend(
                extract_beneficiarios_from_image(
                    image_path=image_path,
                    origen_pdf=nombre_pdf,
                    pagina=idx,
                    lang=lang,
                )
            )
        except Exception as exc:  # pragma: no cover - logging mínimo
            print(f"Advertencia: no se pudo procesar {image_path}: {exc}")
    return resultados


def consolidar_resultados(resultados: Sequence[ExtractionResult]) -> pd.DataFrame:
    """Transforma una lista de resultados en un DataFrame ordenado."""

    if not resultados:
        return pd.DataFrame(
            columns=[
                "origen_pdf",
                "pagina",
                "rut",
                "porcentaje_participacion",
                "ruta_imagen",
                "texto_fila",
            ]
        )

    data = [
        {
            "origen_pdf": r.origen_pdf,
            "pagina": r.pagina,
            "rut": r.rut,
            "porcentaje_participacion": r.porcentaje_participacion,
            "ruta_imagen": r.ruta_imagen,
            "texto_fila": r.texto_fila,
        }
        for r in resultados
    ]
    df = pd.DataFrame(data)
    df.sort_values(["origen_pdf", "pagina", "rut"], inplace=True)
    return df


def reconciliar_con_base_pandas(
    resultados_pdf: pd.DataFrame,
    base_df: pd.DataFrame,
    *,
    columna_rut_base: str = "rut",
    columna_pct_base: str = "porcentaje_participacion",
    tolerancia: float = 0.01,
) -> pd.DataFrame:
    """Conciliar resultados de OCR contra una base maestra usando pandas.

    Este helper es útil en entornos sin PySpark (por ejemplo, ejecutando el
    pipeline en un computador personal o en un clúster ligero). El DataFrame
    resultante replica la semántica de :func:`reconciliar_con_base`.
    """

    if resultados_pdf.empty:
        columnas_extra = [
            "porcentaje_base",
            "diferencia_absoluta",
            "estado_conciliacion",
            "observacion",
        ]
        return resultados_pdf.assign(**{col: pd.Series(dtype="object") for col in columnas_extra})

    df_ocr = resultados_pdf.copy()
    df_ocr["rut"] = df_ocr["rut"].apply(_normalizar_rut_flexible)

    base_trabajo = base_df.copy()
    if columna_rut_base not in base_trabajo.columns:
        raise KeyError(f"La columna '{columna_rut_base}' no existe en la base maestra")
    if columna_pct_base not in base_trabajo.columns:
        raise KeyError(f"La columna '{columna_pct_base}' no existe en la base maestra")

    base_trabajo["rut"] = base_trabajo[columna_rut_base].apply(_normalizar_rut_flexible)
    base_trabajo = base_trabajo.dropna(subset=["rut"]).copy()
    base_trabajo = base_trabajo[["rut", columna_pct_base]].drop_duplicates("rut", keep="last")

    merged = df_ocr.merge(base_trabajo, on="rut", how="left")
    merged.rename(columns={columna_pct_base: "porcentaje_base"}, inplace=True)

    def _coerce_float(valor: object) -> float:
        if valor is None or (isinstance(valor, float) and np.isnan(valor)):
            return float("nan")
        if isinstance(valor, (int, float)):
            return float(valor)
        texto = str(valor).strip()
        if not texto:
            return float("nan")
        texto = texto.replace("%", "").replace(" ", "").replace(",", ".")
        try:
            return float(texto)
        except ValueError:
            return float("nan")

    merged["porcentaje_participacion"] = merged["porcentaje_participacion"].apply(_coerce_float)
    merged["porcentaje_base"] = merged["porcentaje_base"].apply(_coerce_float)
    merged["diferencia_absoluta"] = (
        merged["porcentaje_participacion"] - merged["porcentaje_base"]
    ).abs()

    merged["estado_conciliacion"] = np.where(
        merged["porcentaje_base"].isna(),
        "NO ENCONTRADO",
        np.where(
            merged["diferencia_absoluta"] <= tolerancia,
            "EXITOSO",
            "INCIDENCIA",
        ),
    )

    merged["observacion"] = np.where(
        merged["estado_conciliacion"] == "INCIDENCIA",
        "Porcentaje OCR: "
        + merged["porcentaje_participacion"].astype(str)
        + " vs base: "
        + merged["porcentaje_base"].astype(str),
        "",
    )

    columnas_orden = list(resultados_pdf.columns) + [
        "porcentaje_base",
        "diferencia_absoluta",
        "estado_conciliacion",
        "observacion",
    ]
    return merged[columnas_orden]


def reconciliar_con_base(
    spark: "SparkSession",
    resultados_pdf: pd.DataFrame,
    tabla_destino: str,
    tabla_base: str,
    tolerancia: float = 0.01,
) -> "SparkDataFrame":
    """Conciliar resultados OCR con una tabla maestra en Databricks.

    Args:
        spark: sesión Spark activa en Databricks.
        resultados_pdf: DataFrame con columnas [origen_pdf, pagina, rut,
            porcentaje_participacion, ruta_imagen, texto_fila].
        tabla_destino: nombre de la tabla donde se guardará el resultado bruto.
        tabla_base: nombre de la tabla maestra existente con columnas `rut`
            y `porcentaje_participacion` (o similar).
        tolerancia: diferencia absoluta permitida entre porcentajes para
            considerarlos iguales.

    Returns:
        DataFrame de Spark con columnas adicionales que indican el estado
        de coincidencia (EXITOSO, INCIDENCIA, NO ENCONTRADO).
    """

    if SparkSession is None or F is None:
        raise ImportError(
            "PySpark no está disponible. Ejecuta esta función únicamente en un "
            "entorno con Spark, como Databricks."
        )

    spark_df = spark.createDataFrame(resultados_pdf)
    spark_df.write.mode("overwrite").saveAsTable(tabla_destino)

    base_df = spark.table(tabla_base)

    joined = spark_df.alias("ocr").join(
        base_df.alias("base"),
        on=F.col("ocr.rut") == F.col("base.rut"),
        how="left",
    )

    joined = joined.withColumn(
        "porcentaje_base",
        F.col("base.porcentaje_participacion"),
    ).withColumn(
        "diferencia_absoluta",
        F.abs(F.col("ocr.porcentaje_participacion") - F.col("porcentaje_base")),
    )

    joined = joined.withColumn(
        "estado_conciliacion",
        F.when(F.col("base.rut").isNull(), F.lit("NO ENCONTRADO"))
        .when(F.col("diferencia_absoluta") <= F.lit(tolerancia), F.lit("EXITOSO"))
        .otherwise(F.lit("INCIDENCIA")),
    )

    joined = joined.withColumn(
        "observacion",
        F.when(F.col("estado_conciliacion") == F.lit("INCIDENCIA"),
               F.concat_ws(
                   " ",
                   F.lit("Porcentaje OCR:"),
                   F.col("ocr.porcentaje_participacion"),
                   F.lit("vs base:"),
                   F.col("porcentaje_base"),
               )),
    )

    return joined


def procesar_lote_pdfs(
    carpeta_pdfs: Path,
    carpeta_imagenes: Path,
    dpi: int = 200,
    lang: str = "spa",
) -> pd.DataFrame:
    """Procesa todos los PDFs en una carpeta y retorna un DataFrame consolidado."""

    resultados_totales: List[ExtractionResult] = []

    for pdf_file in sorted(carpeta_pdfs.glob("*.pdf")):
        print(f"Procesando {pdf_file.name}")
        resultados = extract_beneficiarios_from_pdf(pdf_file, carpeta_imagenes, dpi=dpi, lang=lang)
        resultados_totales.extend(resultados)

    df = consolidar_resultados(resultados_totales)
    return df


if __name__ == "__main__":
    # Ejemplo de uso local. Ajusta las rutas según tu estructura en Databricks.
    carpeta_pdf = Path("/dbfs/FileStore/original_pdfs")
    carpeta_img = Path("/dbfs/FileStore/pdfs")

    df_resultados = procesar_lote_pdfs(carpeta_pdf, carpeta_img, dpi=220)
    print(f"Filas extraídas: {len(df_resultados)}")

    # En Databricks: guardar como tabla temporal para revisión manual.
    if SparkSession is not None:
        spark = SparkSession.builder.getOrCreate()
        spark_df = spark.createDataFrame(df_resultados)
        spark_df.createOrReplaceTempView("beneficiarios_ocr_temp")
        print("Vista temporal creada: beneficiarios_ocr_temp")

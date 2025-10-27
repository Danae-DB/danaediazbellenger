"""Ejemplo ejecutable en Python puro para el pipeline de Beneficiarios Finales.

Este script descarga PDFs públicos (hasta el límite indicado), ejecuta la
extracción OCR y muestra un resumen en consola. Es equivalente al notebook
`beneficiarios_finales_demo.ipynb`, pero en formato `.py` para quienes prefieran
trabajar fuera de Jupyter.

Uso rápido:
    python notebooks/beneficiarios_finales_demo.py --limit 10 --output-root data

Antes de ejecutarlo asegúrate de tener instaladas las dependencias:
    pip install -r requirements.txt
    # y Tesseract OCR disponible en tu sistema
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Callable, Iterable, List


def _ensure_repo_on_path() -> None:
    """Añade la raíz del repositorio al PYTHONPATH si fuese necesario."""

    repo_root = Path(__file__).resolve().parent.parent
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))


def _import_pipeline_module():  # type: ignore[no-untyped-def]
    try:
        from beneficiarios_finales_pipeline import (  # type: ignore import
            consolidar_resultados,
            download_public_beneficiary_pdfs,
            extract_beneficiarios_from_pdf,
        )
    except ModuleNotFoundError as exc:  # pragma: no cover - solo en entornos sin setup
        mensaje = [
            "No se encontró el módulo 'beneficiarios_finales_pipeline'.",
            "Ejecuta el script desde la raíz del repositorio o añade esa carpeta",
            "al PYTHONPATH.",
            "\nEjemplo:",
            "  python notebooks/beneficiarios_finales_demo.py",
        ]
        raise SystemExit("\n".join(mensaje)) from exc

    return consolidar_resultados, download_public_beneficiary_pdfs, extract_beneficiarios_from_pdf


def _import_fitz():  # type: ignore[no-untyped-def]
    try:
        import fitz  # type: ignore import
    except ModuleNotFoundError as exc:  # pragma: no cover - solo en entornos sin deps
        mensaje = [
            "No se encontró la librería 'PyMuPDF' (módulo 'fitz').",
            "Instala las dependencias con:",
            "  pip install -r requirements.txt",
            "o, en su defecto:",
            "  pip install pymupdf pillow pytesseract pandas requests",
        ]
        raise SystemExit("\n".join(mensaje)) from exc

    return fitz


def parse_args(argv: List[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Demostración del pipeline de Beneficiarios Finales",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=Path("data"),
        help="Carpeta base donde se almacenarán PDFs, imágenes y resultados.",
    )
    parser.add_argument(
        "--pdf-folder",
        type=Path,
        default=None,
        help=(
            "Carpeta con PDFs ya descargados. Si se indica, el script no descargará "
            "nuevos archivos a menos que también se use --download."
        ),
    )
    parser.add_argument(
        "--download",
        action="store_true",
        help="Forzar la descarga de PDFs públicos aunque existan archivos locales.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Número máximo de PDFs públicos a descargar si --download está activo.",
    )
    parser.add_argument(
        "--catalog-url",
        dest="catalog_urls",
        action="append",
        default=None,
        help=(
            "URL adicional con listados de PDFs públicos. Se puede repetir el "
            "argumento para añadir múltiples fuentes."
        ),
    )
    parser.add_argument(
        "--dpi",
        type=int,
        default=200,
        help="Resolución (en DPI) para rasterizar las páginas del PDF.",
    )
    parser.add_argument(
        "--lang",
        default="spa",
        help="Código de idioma para pytesseract (por ejemplo 'spa' o 'spa+eng').",
    )
    parser.add_argument(
        "--output-csv",
        type=Path,
        default=None,
        help="Ruta donde guardar el CSV resultante con los beneficiarios extraídos.",
    )
    return parser.parse_args(argv)


def ensure_pdf_set(
    pdf_folder: Path,
    force_download: bool,
    limit: int,
    catalog_urls: Iterable[str] | None,
    downloader: Callable[..., List[Path]],
) -> List[Path]:
    existing = sorted(pdf_folder.glob("*.pdf"))
    if existing and not force_download:
        print(f"Se utilizarán {len(existing)} PDFs ya presentes en {pdf_folder}.")
        return existing

    print(
        "Descargando PDFs públicos... (puedes usar --pdf-folder y --download para controlar esto)"
    )
    descargados = downloader(
        destination=pdf_folder,
        limit=limit,
        catalog_urls=catalog_urls,
    )
    print(f"Se descargaron {len(descargados)} archivos en {pdf_folder}.")
    return descargados


def main(argv: List[str] | None = None) -> int:
    _ensure_repo_on_path()
    _import_fitz()
    (
        consolidar_resultados,
        download_public_beneficiary_pdfs,
        extract_beneficiarios_from_pdf,
    ) = _import_pipeline_module()

    args = parse_args(argv)

    output_root = args.output_root.resolve()
    pdf_folder = (args.pdf_folder or (output_root / "pdfs_publicos")).resolve()
    images_folder = (output_root / "imagenes").resolve()
    existed_before = pdf_folder.exists()
    pdf_folder.mkdir(parents=True, exist_ok=True)
    images_folder.mkdir(parents=True, exist_ok=True)

    pdf_paths = ensure_pdf_set(
        pdf_folder=pdf_folder,
        force_download=args.download or not existed_before,
        limit=args.limit,
        catalog_urls=args.catalog_urls,
        downloader=download_public_beneficiary_pdfs,
    )
    if not pdf_paths:
        print("No se encontraron PDFs para procesar. Usa --download para obtener ejemplos.")
        return 1

    print(f"Procesando {len(pdf_paths)} PDFs...")
    resultados = []
    for pdf_path in pdf_paths:
        print(f"  - {pdf_path.name}")
        resultados.extend(
            extract_beneficiarios_from_pdf(
                pdf_path=pdf_path,
                output_root=images_folder,
                dpi=args.dpi,
                lang=args.lang,
            )
        )

    if not resultados:
        print("No se extrajeron beneficiarios. Revisa la calidad de los PDFs o ajusta los parámetros.")
        return 1

    df_resultados = consolidar_resultados(resultados)
    print("\nResumen de beneficiarios extraídos:")
    print(df_resultados.head())
    print(f"Total de filas extraídas: {len(df_resultados)}")

    if args.output_csv:
        args.output_csv.parent.mkdir(parents=True, exist_ok=True)
        df_resultados.to_csv(args.output_csv, index=False)
        print(f"CSV guardado en {args.output_csv}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

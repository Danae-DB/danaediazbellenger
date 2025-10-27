# Extracción de Beneficiarios Finales

Este repositorio contiene utilidades para automatizar la extracción de la tabla
**Beneficiarios Finales** desde lotes de PDFs. El flujo implementado funciona en
Databricks o en un entorno local sin PySpark, apoyándose en PyMuPDF para generar
imágenes por página y en **pytesseract** (IA de OCR) para recuperar el RUT y el
porcentaje de participación de cada beneficiario.

## Flujo general

1. **Conversión a imágenes:** cada PDF se renderiza página a página en formato
   PNG, lo que facilita el procesamiento por OCR incluso cuando el PDF es un
   escaneo.
2. **OCR y extracción inteligente:** a partir de los resultados de pytesseract
   se detectan líneas que contengan el encabezado "Beneficiarios Finales", un
   RUT válido y un porcentaje. Cada RUT se normaliza como una cadena de 11
   caracteres rellenada con ceros (`00987654321`, sin guiones) y se convierte el
   porcentaje a número decimal.
3. **Persistencia de resultados:** el DataFrame consolidado puede guardarse en
   Databricks (con PySpark) o en archivos/parquet locales usando pandas.
4. **Conciliación con base maestra:** se incluyen funciones que comparan los
   resultados extraídos contra una tabla existente y clasifican cada caso como
   `EXITOSO`, `INCIDENCIA` o `NO ENCONTRADO` según la coincidencia de RUT y
   porcentaje. Puedes elegir entre la versión con PySpark o la versión 100%
   pandas según tu entorno.

## Requisitos

Instala las dependencias básicas en tu clúster o entorno local:

```bash
pip install -r requirements.txt
# o, en notebooks de Databricks:
%pip install pymupdf pillow pytesseract pandas requests
```

En Databricks, recuerda que pytesseract necesita que el ejecutable de
Tesseract esté disponible en la imagen del clúster (Ubuntu/Debian: `apt-get
install tesseract-ocr`).

PySpark **no es obligatorio**. Si tu entorno no dispone de Spark, puedes saltar
las funciones de conciliación específicas de Databricks y utilizar la versión
basada en pandas descrita más abajo.

### ¿Ves errores `ModuleNotFoundError`?

1. Ejecuta `pip install -r requirements.txt` en la terminal (o la celda `%pip install ...`
   en Databricks) para asegurarte de que `PyMuPDF` y el resto de dependencias estén
   disponibles.
2. Si al usar `notebooks/beneficiarios_finales_demo.py` recibes un mensaje indicando
   que no se encuentra `beneficiarios_finales_pipeline`, ejecuta el script desde la
   raíz del repositorio (`python notebooks/beneficiarios_finales_demo.py`) o añade
   esa ruta al `PYTHONPATH`.

## Uso rápido en Databricks

```python
from pathlib import Path
import beneficiarios_finales_pipeline as bfp

carpeta_pdf = Path("/dbfs/FileStore/original_pdfs")
carpeta_img = Path("/dbfs/FileStore/pdfs")

# Procesar todos los PDFs y obtener un DataFrame de pandas
resultados_pdf = bfp.procesar_lote_pdfs(carpeta_pdf, carpeta_img, dpi=220, lang="spa")

# Guardar resultado crudo y conciliar con la tabla oficial
spark = spark  # Sesión existente en el notebook
conciliado = bfp.reconciliar_con_base(
    spark=spark,
    resultados_pdf=resultados_pdf,
    tabla_destino="beneficiarios_ocr",  # nueva tabla gestionada
    tabla_base="beneficiarios_empresa",  # tabla SQL existente
    tolerancia=0.05,
)

# Revisar incidencias
conciliado.filter("estado_conciliacion != 'EXITOSO'").display()
```

Las columnas resultantes permiten identificar rápidamente qué PDF, página e
inclusive qué texto OCR generó cada registro, facilitando la trazabilidad en
caso de incidencias.

## Conciliación sin PySpark

Si trabajas en un entorno donde PySpark no está disponible, utiliza la función
`reconciliar_con_base_pandas` para comparar los resultados del OCR con tu base
maestra cargada en pandas:

```python
import pandas as pd
from pathlib import Path
import beneficiarios_finales_pipeline as bfp

carpeta_pdf = Path("data/pdfs")
carpeta_img = Path("data/pdfs_imagenes")

resultados_pdf = bfp.procesar_lote_pdfs(carpeta_pdf, carpeta_img)
base_maestra = pd.read_csv("data/base_beneficiarios.csv")

conciliado = bfp.reconciliar_con_base_pandas(
    resultados_pdf,
    base_maestra,
    columna_rut_base="rut_empresa",
    columna_pct_base="porcentaje",
    tolerancia=0.05,
)

print(conciliado[conciliado["estado_conciliacion"] != "EXITOSO"])
```

La columna `estado_conciliacion` indica los casos que requieren revisión manual,
con la observación detallando la diferencia de porcentaje cuando aplique.

## Descarga de PDFs públicos de ejemplo

El módulo incluye la función `download_public_beneficiary_pdfs`, que busca PDFs
de beneficiarios finales publicados por la Comisión para el Mercado Financiero
(CMF). Puedes usarla para armar un set de hasta 50 documentos de prueba antes
de apuntar a tus propios archivos privados:

```python
from pathlib import Path
from beneficiarios_finales_pipeline import download_public_beneficiary_pdfs

destino = Path("data/pdfs_publicos")
descargados = download_public_beneficiary_pdfs(destino, limit=50)
print(f"Se descargaron {len(descargados)} PDFs")
```

Si tu red corporativa restringe el acceso a internet, reemplaza el parámetro
`catalog_urls` con páginas internas que contengan enlaces a los PDFs que quieras
descargar.

## Script de ejemplo en Python puro

Si prefieres ejecutar todo desde la línea de comandos o compartir un script autosuficiente, usa `notebooks/beneficiarios_finales_demo.py`. El archivo contiene la misma lógica del notebook pero escrita como programa Python y admite argumentos para personalizar directorios, cantidad de PDFs y salida.

```bash
python notebooks/beneficiarios_finales_demo.py --limit 10 --output-root data --output-csv data/resultado_beneficiarios.csv
```

Opcionalmente puedes indicar `--pdf-folder` para reutilizar PDFs ya descargados y `--download` para forzar una nueva descarga desde las fuentes públicas.

## Notebook de referencia

En `notebooks/beneficiarios_finales_demo.ipynb` encontrarás un ejemplo paso a
paso pensado para ejecutarse en Databricks o Jupyter local. El cuaderno cubre:

1. Instalación de dependencias.
2. Descarga de PDFs públicos (limit=50 por defecto).
3. Extracción OCR y consolidación en un `DataFrame`.
4. Opciones para guardar los resultados y preparar la conciliación.

Úsalo como plantilla para tus propios notebooks y reemplaza las rutas de salida
por las de tu entorno seguro una vez valides el flujo con los datos públicos.

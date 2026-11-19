# ETL Entrega Final — Huella de Carbono en Hogares Colombianos (ODS 12)

**Curso:** ETL (G01) · Universidad Autónoma de Occidente · Ingeniería de Datos e Inteligencia Artificial

> Entrega final del proyecto de ETL: integra el pipeline batch (Airflow, Great Expectations, Data Warehouse MySQL y dashboard en Power BI) de las entregas anteriores y lo extiende con un **componente de streaming** (Apache Kafka, consumidor con contrato de eventos y dashboard en tiempo real en Grafana). Este README sigue las 12 secciones del enunciado. Los números del batch salen del Data Warehouse tras la corrida final (sección 11.1) y cada supuesto está en [`docs/supuestos_y_trazabilidad.md`](docs/supuestos_y_trazabilidad.md).
>
> **Repositorios anteriores (se conservan sin cambios):** [Primera Entrega](https://github.com/dianapanesso/etl-proyecto-huella-carbono) · [Segunda Entrega](https://github.com/nikolalehurtado/etl-project-delivery-2-huella-carbono)

**Contenido:** 
- [1. Descripción del proyecto y objetivos](#1-descripción-del-proyecto-y-objetivos) — *Sebastián Celorio (Analítica y Negocio)*
- [2. Fuentes de datos y EDA](#2-fuentes-de-datos-y-eda) — *David Aponte (Calidad de Datos y API)*
- [3. Arquitectura del sistema](#3-arquitectura-del-sistema) — *Nikol Hurtado (Infraestructura y Orquestación)*
- [4. Pipeline ETL batch](#4-pipeline-etl-batch) — *Nikol Hurtado (Infraestructura y Orquestación)*
- [5. Calidad de datos](#5-calidad-de-datos) — *David Aponte (Calidad de Datos y API)*
- [6. Data Warehouse](#6-data-warehouse) — *Diana Panesso (Transformación y DW)*
- [7. Dashboard analítico (batch)](#7-dashboard-analítico-batch) — *Sebastián Celorio (Analítica y Negocio)*
- [8. Pipeline de streaming](#8-pipeline-de-streaming) — *Diana Panesso (Streaming y DW)*
- [9. Dashboard en tiempo real](#9-dashboard-en-tiempo-real) — *Sebastián Celorio & Diana Panesso*
- [10. Ejecución y reproducibilidad](#10-ejecución-y-reproducibilidad) — *Nikol Hurtado (Infraestructura y Orquestación)*
- [11. Evidencia funcional](#11-evidencia-funcional) — *Equipo Completo (Nikol H., David A., Diana P., Sebastián C.)*
- [12. Decisiones de ingeniería y limitaciones](#12-decisiones-de-ingeniería-y-limitaciones) — *Sebastián Celorio (Analítica y Negocio)*
- [13. Integrantes y distribución de responsabilidades](#13-integrantes-y-distribución-de-responsabilidades) — *Equipo Completo*

## 1. Descripción del proyecto y objetivos

> **Responsable:** Sebastián Celorio (Persona 4: Analítica y Negocio)

### 1.1 Problema y objetivo analítico

Estimar y caracterizar la huella de carbono residencial de los hogares colombianos (ODS 12) con la ECV 2025 del DANE
y, en esta entrega, explicar parte de la variación del consumo eléctrico con la temperatura oficial del IDEAM.

### 1.2 Línea base de la Primera Entrega y enlace al repositorio

**Repositorio de la primera entrega (se conserva intacto):** https://github.com/dianapanesso/etl-proyecto-huella-carbono

**Qué se construyó en la primera fase.** Un Data Warehouse dimensional en MySQL con los microdatos de la ECV 2025 del DANE (único origen) y factores oficiales de emisión, para estimar la **huella de carbono residencial anual** de los hogares colombianos (ODS 12). Los gastos de energía y de combustible de cocina se convertían en kWh y en emisiones mediante tarifas, precios y factores declarados como supuestos. La primera fase respondía cinco requisitos (R1–R5) con consultas SQL sobre el DW.

**Grano declarado:** una fila de hechos por hogar encuestado, identificado por `DIRECTORIO` + `SECUENCIA_P` (llaves que identifican al hogar). La segunda entrega conserva este grano.

**Modelo dimensional previo:** esquema en estrella con una tabla de hechos (`fact_huella_carbono`) y cuatro dimensiones (`dim_hogar`, `dim_ubicacion`, `dim_combustibles`, `dim_tarifas`). No tenía ninguna variable de clima.

![Modelo estrella de la Primera Entrega](docs/evidence/star_schema.png)

**Resultados principales de la primera fase** (según su README):

| Requisito | Resultado reportado |
|---|---|
| R1 | El consumo y las emisiones eléctricas bajan del estrato 1–2 (≈137–139 kWh/mes) al estrato 6 (≈94 kWh/mes) |
| R2 | Con leña, un hogar emite unas 3 veces más en cocina (≈3.145 kg/año) que con GLP o gas natural (resultado con el factor completo; ver corrección D-04 en la segunda entrega) |
| R3 | Los hogares sin carro mostraron mayor huella (≈1.232 kg) que los que tienen (≈1.030 kg), un resultado contraintuitivo |
| R4 | Vichada, Guainía, Amazonas y Vaupés lideran la huella per cápita (≈620–740 kg) |
| R5 | La huella per cápita baja de ≈1.000 kg (hogares de 1 persona) a ≈243 kg (hogares de 8 personas) |

*Nota:* estos resultados de la primera fase se calcularon con el factor eléctrico 0,097, con la leña a factor completo y, además, con `P5010` como "estrato". Esa variable es en realidad el número de cuartos donde duermen las personas del hogar; el estrato verdadero es `P8520S1A1` (estrato para tarifa de energía). La decisión D-07 lo corrige en esta entrega. Las correcciones D-04 (leña) y D-06 (factor eléctrico), explicadas en la sección 12.1, cambian sus valores absolutos; las nuevas cifras de R2 a R5 están en la sección 7.2.

**Limitaciones de la primera fase que motivaron cambios.**
- R3 no era coherente con los datos: la ECV solo dice si el hogar tiene vehículo, no su consumo (sección 1.3).
- No incluía un tablero de BI conectado al DW.
- Los supuestos que convierten gasto en consumo y emisiones no estaban documentados con su fuente (sección 12.1 y [`docs/supuestos_y_trazabilidad.md`](docs/supuestos_y_trazabilidad.md)).
- La ECV no trae información de clima, lo que impedía preguntar si el clima se asocia con el consumo. La segunda entrega lo resuelve con la API del IDEAM (sección 2.2).

### 1.3 Estado y cambios de R1–R5

Los requisitos de la primera entrega se revisaron uno por uno. Ninguno se cambió en silencio: cada cambio queda registrado aquí y en la bitácora de [`docs/supuestos_y_trazabilidad.md`](docs/supuestos_y_trazabilidad.md).

| Req. | Estado | Cambio introducido | Justificación | Salida analítica en la segunda entrega |
|---|---|---|---|---|
| R1 | Refinar | Además de por estrato, se puede desagregar por piso térmico uniendo con `dim_clima` | Sirve de puente con R6; no cambia el cálculo de la huella eléctrica | Huella eléctrica media por estrato (y por piso térmico) |
| R2 | Refinar | Ninguno | El requisito sigue siendo válido, pero su resultado cambia: la leña se trata como carbono neutra (CO₂ = 0), como indica la tabla de factores, por lo que ya no emite más que GLP o gas natural (decisión D-04, supuesto S9). Los precios fijos de combustible quedan como limitación documentada | Emisiones de cocina por combustible |
| R3 | **Reemplazar** (queda como **R3′**) | De "peso de los vehículos en las emisiones del hogar" a "comparar la huella por energía y cocina de hogares con y sin vehículo, a igual estrato" | Limitación analítica documentada: la ECV solo informa si hay vehículo (`P1077S15`, carro particular), no su combustible ni su uso, así que el peso del vehículo no se puede calcular. La primera entrega midió otra cosa sin cambiar el requisito. Detalle y fecha en la bitácora | Huella media por estrato y tenencia de vehículo (asociación; las emisiones del vehículo no están incluidas) |
| R4 | Refinar (precisión) | Se declara que se responde **por departamento** | La primera entrega hablaba de "municipios o departamentos", pero el modelo y la ECV usada solo traen departamento. Es una precisión, no un cambio de fondo | Huella per cápita por departamento |
| R5 | Mantener | Ninguno | Sigue siendo válido y no depende de la API | Huella per cápita por tamaño del hogar |

Los requisitos nuevos R6 y R7, que sí usan la API, están en la sección 1.4.

### 1.4 Requisitos integrados R6 y R7

| ID | Requisito | Datos ECV | Datos API | Llave | KPI | Detalle |
|---|---|---|---|---|---|---|
| R6 | ¿Cuántos kWh/mes y kg CO₂ eléctrico más generan los hogares de departamentos cálidos que los de templados o fríos, a igual estrato? | `P5018` (pago del último mes por electricidad), `P8520S1A1` (estrato para tarifa de energía), departamento, tarifa | Temperatura media 2025 por departamento | Departamento (DANE) | kWh/mes y kg CO₂/año por piso térmico y estrato; correlación temperatura–kWh | Departamento × estrato |
| R7 | ¿El % de hogares con aire acondicionado o ventilador sube con la temperatura del departamento, y cuánto CO₂ eléctrico extra tienen? | `P1077S11` (aire acondicionado), `P1077S12` (ventilador o abanico), huella eléctrica | Temperatura media 2025 por departamento | Departamento (DANE) | % de hogares con equipo vs temperatura; diferencia de kg CO₂ | Departamento |

### 1.5 Requisitos batch y requisitos de streaming

Los requisitos R1 a R7 son **analíticos batch**: describen la huella histórica de la muestra de la ECV 2025 y se responden con consultas sobre el Data Warehouse, que el DAG recarga completo en cada corrida (la ECV es anual y la API se extrae por ventanas de fechas). Una respuesta con horas o días de retraso no cambia su valor.

La entrega final agrega requisitos de **baja latencia**, pensados para un caso operativo: un programa de monitoreo de emisiones (o una empresa de energía) que recibe, uno por uno, los reportes de consumo de los hogares y necesita reaccionar mientras llegan, sin esperar a la próxima carga batch.

| ID | Requisito de streaming | Métrica | Latencia esperada | Por qué no basta el batch |
|---|---|---|---|---|
| S-R1 | Detectar en segundos los hogares con huella anormalmente alta | Alertas de alta huella: hogares por encima del p95 histórico del DW (1.868,2 kg CO₂/año) | < 1 s desde la publicación | Con el batch, un hogar crítico se vería en la siguiente recarga; la alerta inmediata permite priorizarlo (visita, asesoría de eficiencia) |
| S-R2 | Seguir la huella y el consumo medios a medida que llegan reportes | Huella media acumulada (kg CO₂/año) y kWh/mes medio en piso cálido contra templado/frío (R6 en vivo) | Se actualiza con cada evento | Muestra cómo se estabilizan los indicadores y si un grupo de reportes nuevos los desvía |
| S-R3 | Vigilar la salud del flujo de datos | Eventos procesados en el último minuto, % de eventos inválidos y latencia media | Ventana de 1 minuto | Una fuente que deja de enviar o que manda datos dañados se detecta en minutos, no en la siguiente corrida del DAG |

**Valor del componente de streaming.** El batch responde *qué pasó* en la muestra; el streaming responde *qué está pasando ahora*: qué hogares requieren atención inmediata, si los indicadores se mueven y si el flujo de datos está sano. Los dos se complementan y usan el mismo Data Warehouse: el productor lee los hogares de `fact_huella_carbono` y el consumidor escribe sus resultados en tablas `stream_*` de la misma base (sección 6.3).

**Cumplimiento de los objetivos.** Cada requisito batch tiene su consulta, su resultado y su visualización (sección 7.2). Los requisitos de streaming se demuestran con la corrida de la sección 11.4: 600 eventos procesados con latencia típica de 8 a 25 ms, 30 alertas de alta huella, 8 eventos inválidos rechazados y recuperación completa después de detener el consumidor.

## 2. Fuentes de datos y EDA

> **Responsable:** David Aponte (Persona 2: Calidad de Datos y API)

### 2.1 Fuente primaria y fuente API

**Fuente primaria:** ECV 2025 (DANE), 4 capítulos en CSV (`data/raw/ecv/`): Servicios del hogar, Datos de la vivienda,
Características y composición del hogar, Condiciones de vida del hogar y tenencia de bienes (se excluye el capítulo "(programas)").

**API (sección 1.4.1 del enunciado):**

| Ítem | Documentación |
|---|---|
| Proveedor y propósito | Instituto de Hidrología, Meteorología y Estudios Ambientales (IDEAM). Dataset "Temperatura Ambiente del Aire" en datos.gov.co (id `sbwg-7ju4`): lecturas horarias de estaciones del IDEAM |
| Valor analítico | Aporta clima oficial por departamento, que la ECV no tiene; sin él no se pueden responder R6 ni R7 |
| Endpoint y parámetros | `https://www.datos.gov.co/resource/sbwg-7ju4.json` (SODA). Filtro `codigosensor='0068'` (Temp Aire 2 m, °C). Ventanas de fechas con `$where`, agregación en el servidor con `$select`, `$group`, `$order`, `$limit`, `$offset` |
| Respuesta | JSON (lista de objetos). Se conservan: estación, departamento, municipio, latitud, longitud, suma, conteo, mínimo y máximo por ventana |
| Autenticación | Sin autenticación obligatoria; token de aplicación opcional en la cabecera `X-App-Token` (variable `DATOS_GOV_APP_TOKEN`, nunca en el repositorio) |
| Comportamiento operativo | Páginas de hasta 50 000 filas; reintentos por ventana ante timeout, red, HTTP 429/5xx; error 4xx se trata como falla determinista; sello de tiempo en `api_manifest.json` |
| Reproducibilidad | `docker compose up` + disparar el DAG; otro evaluador usa las mismas fechas (`.env.example`). Licencia CC BY-SA 4.0 (fuente: IDEAM) |

Aviso de la ficha del dataset: el IDEAM no validó los valores publicados y pueden existir errores por fallas de sensor (base de las reglas DQ15 y DQ17).

### 2.2 Valor analítico aportado por la API

**Qué no puede hacer la ECV sola.** La ECV 2025 describe cuánto paga un hogar de energía, de qué estrato es y en qué departamento vive, pero **no dice qué clima tiene ese departamento**. Sin esa variable no se puede preguntar si el clima se asocia con el consumo eléctrico: R6 y R7 no se pueden ni plantear.

**Qué aporta la API del IDEAM.** La temperatura media del aire de 2025 por departamento (32 de los 33 departamentos de la ECV, ver [`docs/evidence/cobertura_api_ideam_2025.md`](docs/evidence/cobertura_api_ideam_2025.md)). Integrada por el código de departamento (sección 4.2), permite clasificar cada hogar en piso térmico cálido, templado o frío (cortes en el supuesto S14) y calcular su consumo y emisiones según el clima de su departamento. Cubre al 98,26 % de los hogares (85.339 de 86.848).

**Demostración con el DW (R6).** Estas consultas se ejecutaron sobre el Data Warehouse (`fact_huella_carbono` unida a `dim_clima` y `dim_ubicacion`), solo con hogares con clima emparejado.

*Consumo eléctrico promedio por piso térmico:*

```sql
SELECT c.piso_termico, COUNT(*) AS hogares,
       ROUND(AVG(f.kwh_consumidos_estimados),1) AS kwh_mes_promedio,
       ROUND(AVG(f.emisiones_energia_kg),1) AS kg_co2_energia_promedio
FROM fact_huella_carbono f JOIN dim_clima c ON f.fk_clima = c.sk_clima
WHERE f.clima_emparejado = 1
GROUP BY c.piso_termico ORDER BY kwh_mes_promedio DESC;
```

| Piso térmico | Hogares | kWh/mes promedio | kg CO₂ eléctrico/año promedio |
|---|---|---|---|
| Cálido | 37.694 | 132,5 | 349,9 |
| Templado | 29.661 | 112,1 | 295,9 |
| Frío | 17.984 | 94,5 | 249,4 |

En promedio, un hogar de departamento cálido consume un 40,3 % más electricidad que uno de departamento frío. Esa comparación mezcla estratos, así que se repitió **a igual estrato**, que es lo que pregunta R6:

```sql
SELECT u.estrato, c.piso_termico, COUNT(*) AS hogares,
       ROUND(AVG(f.kwh_consumidos_estimados),1) AS kwh_mes_promedio
FROM fact_huella_carbono f
JOIN dim_clima c ON f.fk_clima = c.sk_clima
JOIN dim_ubicacion u ON f.fk_ubicacion = u.sk_ubicacion
WHERE f.clima_emparejado = 1
GROUP BY u.estrato, c.piso_termico ORDER BY u.estrato, kwh_mes_promedio DESC;
```

![kWh por estrato y piso térmico](docs/evidence/r6_kwh_por_estrato_y_piso.png)

| Estrato | Cálido (hogares · kWh/mes) | Templado | Frío | Cálido vs. frío |
|---|---|---|---|---|
| 1 | 21.156 · 139,4 | 12.993 · 106,1 | 6.492 · 89,9 | +49,5 kWh (+55,1 %) |
| 2 | 6.318 · 185,3 | 10.372 · 123,4 | 7.425 · 99,2 | +86,1 kWh (+86,8 %) |
| 3 | 2.203 · 170,6 | 3.343 · 115,9 | 2.395 · 94,3 | +76,3 kWh (+80,9 %) |
| 4 | 357 · 225,1 | 1.082 · 127,4 | 611 · 99,2 | +125,9 kWh (+126,9 %) |
| 5 | 95 · 250,0 | 375 · 131,2 | 245 · 117,8 | +132,2 kWh (+112,2 %) |
| 6 | 70 · 275,8 | 103 · 156,3 | 244 · 154,0 | +121,8 kWh (+79,1 %) |
| Sin estrato | 7.495 · 50,3 | 1.393 · 54,2 | 572 · 46,5 | no se interpreta |

(**Sin estrato** agrupa a los hogares sin un estrato 1 a 6 en la ECV: factura sin estrato o servicio ilegal, vivienda con planta eléctrica, o estrato desconocido sin factura. En la base se guarda con el valor 0 en `dim_ubicacion.estrato`, pero no es un estrato. Son 9.460 hogares con clima y no se interpretan.)

**Interpretación.**
- **A igual estrato, el consumo eléctrico sube con el calor del departamento: cálido > templado > frío en los seis estratos.** La diferencia entre cálido y frío va de 55 % a 127 %.
- **La diferencia global (40,3 %) es menor que la de cada estrato por la mezcla de hogares sin estrato.** Esos hogares son el 19,9 % de los hogares cálidos y solo el 3,2 % de los fríos, y su consumo estimado es bajo (alrededor de 50 kWh/mes), lo que baja el promedio del piso cálido. Sin ellos, el promedio global sería 153,0 kWh en cálido contra 96,1 en frío (+59 %).
- Los estratos 1 a 3 reúnen el 85,2 % de los hogares con clima (72.697 de 85.339) y el 95,8 % de los que tienen estrato 1 a 6, con miles de hogares por celda.
- **Cautela con los estratos 4 a 6:** tienen menos hogares por celda (la más pequeña es de 70 hogares, estrato 6 en piso cálido), así que sus promedios son menos estables. En el estrato 6 los pisos templado y frío casi no se diferencian (156,3 contra 154,0 kWh).
- El CO₂ eléctrico anual se calcula como kWh × 12 × 0,220 kg/kWh (supuesto S3, decisión D-06), por lo que la diferencia porcentual de CO₂ es idéntica a la de kWh.

**Límites que se deben declarar.**
- Es una **asociación**, no una causa: los departamentos cálidos y fríos difieren también en otros aspectos (región, tipo de vivienda, ingresos dentro del estrato) que la ECV no controla en este análisis.
- Los kWh **no se miden**: se estiman dividiendo el pago de energía (`P5018`, pago del último mes por electricidad) entre una tarifa por estrato (supuestos S1 y S2); si el pago cubre varios meses se divide entre ellos (decisión D-08). Si las tarifas reales difieren de las supuestas, cambian los niveles, aunque la comparación entre pisos térmicos dentro de un mismo estrato usa la misma tarifa.
- La temperatura es un **promedio departamental** de las estaciones del IDEAM, no la del hogar (supuesto S14). Vaupés queda fuera de R6 y R7 por no tener datos (decisión D-02).

**Demostración con el DW (R7).** La consulta de R7 (`sql/analytical_queries.sql`) une `fact_huella_carbono` con `dim_clima` y `dim_hogar` y calcula, por departamento, el porcentaje de hogares con aire acondicionado (`P1077S11`) y con ventilador o abanico (`P1077S12`), según el [diccionario de datos de la ECV 2025](https://microdatos.dane.gov.co/index.php/catalog/905), junto con la temperatura y el consumo eléctrico. Solo entran hogares con clima emparejado (85.339 hogares, 32 departamentos).

| Piso térmico (IDEAM) | Departamentos | Hogares con aire acondicionado | Hogares con ventilador | kWh/mes | CO₂ eléctrico (kg/año) |
|---|---|---|---|---|---|
| Cálido (>24 °C) | 16 | 9,3 % | 71,8 % | 132,5 | 349,9 |
| Templado (18–24 °C) | 10 | 2,3 % | 31,9 % | 112,1 | 295,9 |
| Frío (<18 °C) | 6 | 2,2 % | 19,6 % | 94,5 | 249,4 |

(Promedios ponderados por número de hogares de cada departamento.)

**Interpretación de R7.**
- **El porcentaje de hogares con equipo sube con la temperatura del departamento.** La correlación de rangos (Spearman) entre temperatura y porcentaje de hogares es 0,82 para ventilador y 0,71 para aire acondicionado. A nivel nacional, 46,9 % de los hogares tiene ventilador y 5,4 % aire acondicionado.
- **El impacto en la huella eléctrica es notable:** un hogar en un departamento cálido consume ≈40 % más kWh (132,5 contra 94,5) y emite ≈40 % más CO₂ eléctrico (349,9 contra 249,4 kg/año) que uno en un departamento frío. Los dos porcentajes coinciden porque el CO₂ se calcula como kWh × 12 × 0,220 (supuesto S3, decisión D-06).
- **Anomalía de Arauca (decisión D-05, supuesto S16).** Arauca aparece con 10,2 °C y piso "frío", pero 91,3 % de sus hogares tiene ventilador y 11,7 % aire acondicionado, un perfil de clima cálido. La API trae **una sola estación** para Arauca, y su promedio parece no representar a la población del departamento (hipótesis: la ubicación de la estación no se verificó). Los datos no se modificaron. **Sin Arauca la relación es más fuerte** (0,92 para ventilador y 0,81 para aire acondicionado) y el grupo "frío" pasa de 19,6 % a ≈8 % de hogares con ventilador. La conclusión no cambia con ni sin Arauca.
- La misma anomalía afecta al piso "frío" de R6, donde Arauca está incluido.

**Límites de R7.** Es una asociación entre 32 departamentos, no una causa. No se controla por estrato ni por ingresos (el ventilador también depende del nivel económico). La temperatura es un promedio departamental y los kWh son estimados (supuestos S1, S2 y S14).

**Qué se habría perdido sin la API.** La pregunta de R6 no tendría respuesta y el análisis se quedaría en R1–R5 (huella por estrato, combustible, departamento y tamaño del hogar), que describen *cuánto* emite un hogar pero no *si el clima se asocia con ese consumo*. Este bloque se complementa con el KPI y la gráfica del dashboard (sección 7.1) y con la trazabilidad R6 → KPI (sección 7.2).

### 2.3 Hallazgos de perfilamiento

Dos notebooks reproducibles en `notebooks/`:

| Notebook | Contenido | Reglas DQ cubiertas |
|---|---|---|
| [`01_perfilamiento_ecv_primaria.ipynb`](notebooks/01_perfilamiento_ecv_primaria.ipynb) | Grano y duplicados, completitud P5018 (~77%), distribución de estrato, contrato columnar de los 4 capítulos, validez P1_DEPARTAMENTO (código del departamento), dominio P1077S15 (carro particular)/S11/S12, reformulación R3 | DQ01–DQ11 |
| [`02_perfilamiento_api_ideam.ipynb`](notebooks/02_perfilamiento_api_ideam.ipynb) | Contrato del endpoint SODA, rango físico de temperatura [-15°C, 50°C], cobertura departamental (≥30 dptos, decisión D-03), consistencia temporal (2025), coordenadas dentro de Colombia, normalización de nombres de departamentos, clasificación por piso térmico | DQ12–DQ18 |

**Hallazgos clave (con impacto en el pipeline):**
- `P5018` (pago del último mes por electricidad) completitud ≈77% (umbral DQ04 ≥70% — conservador con 7pp de margen)
- 212 duplicados en la fase 1 (0.24%) → umbral DQ03 ≥99%; ≤1% = error de carga
- Estrato para tarifa (`P8520S1A1`): el 91,5 % de los valores informados es un estrato 1 a 6; los códigos 0, 8 y 9 y los vacíos (10.060 hogares) se reclasifican a "sin estrato" → DQ05 con dos controles: dominio del diccionario DANE (≥ 99,9 %) y estrato 1 a 6 (≥ 85 %)
- `P1077S15` (carro particular) es puramente binaria {1, 2} → sustenta reformulación de R3
- Nombres de departamentos del IDEAM no estandarizados → tabla `ALIAS_DEPARTAMENTO` en `src/transform.py`
- Fallas de sensor (DQ15): umbral 99.5% basado en aviso oficial de la ficha IDEAM

### 2.4 Origen de los eventos de streaming

La API del IDEAM se usa en **extracción batch periódica** (ventanas de 7 días dentro del DAG); no es una fuente de eventos. Los eventos de streaming se generan así:

- **Qué es un evento:** un hogar de `fact_huella_carbono`, unido a sus dimensiones (departamento, estrato, piso térmico, personas), con su consumo y su huella. Cada evento lleva `simulated: true`.
- **Es una simulación declarada:** los hogares son datos históricos de la ECV 2025 que se **re-emiten** para simular la llegada de reportes de consumo, como lo permite el enunciado. No son lecturas nuevas.
- **Orden y frecuencia:** el productor ordena los hogares al azar con una semilla fija (reproducible) para que desde el primer evento se mezclen departamentos y estratos, y publica **2 eventos por segundo** por defecto (`--rate`).
- **Por qué no se publican KPI repetidos:** cada mensaje trae un hogar distinto, así que cada evento cambia las métricas del consumidor. El productor de la versión anterior enviaba los mismos tres promedios del DW cada 5 segundos; se reemplazó porque esos valores no cambiaban entre recargas del DAG.
- **Por qué requieren baja latencia:** ver los requisitos S-R1 a S-R3 (sección 1.5). El detalle del productor, el topic y el contrato está en la sección 8.

## 3. Arquitectura del sistema

> **Responsable:** Nikol Hurtado (Persona 1: Infraestructura y Orquestación)

```mermaid
flowchart TB
  subgraph FUENTES["Fuentes de datos"]
    ECV[("ECV 2025 · DANE<br/>4 capítulos CSV")]
    API[("API del IDEAM · datos.gov.co<br/>temperatura del aire")]
  end

  subgraph AIRFLOW["BATCH · Apache Airflow · DAG etl_huella_carbono · dependencias, reintentos, logs y monitoreo"]
    direction TB
    EP["extract_primary"] --> RAWE[/"raw ECV<br/>+ manifiesto"/] --> G1{{"Compuerta 1 · GX<br/>validate_primary_raw"}}
    EA["extract_api<br/>ventanas de 7 días · 3 reintentos"] --> RAWA[/"raw API<br/>+ manifiesto"/] --> G2{{"Compuerta 2 · GX<br/>validate_api_raw"}}
    G1 --> T["transform_and_clean_and_integrate<br/>integración por código DANE"]
    G2 --> T
    T --> PREP[/"6 tablas preparadas<br/>+ integration_report.json"/] --> G3{{"Compuerta 3 · GX<br/>validate_prepared"}}
    G3 --> L["load_dimensional_dw<br/>transacción · conciliación · ROLLBACK"]
  end

  subgraph DW["Data Warehouse · MySQL 8"]
    F[("fact_huella_carbono")]
    D[("5 dimensiones")]
    AUD[("etl_audit_log")]
    ST[("stream_events · stream_invalid_events<br/>stream_metrics_snapshot")]
  end

  subgraph BI["Dashboard analítico (histórico)"]
    Q["Consultas SQL R1 a R7"] --> DASH["Power BI"]
  end

  subgraph STREAM["STREAMING · Apache Kafka"]
    direction TB
    PR["Productor Python<br/>re-emite hogares · 2 eventos/s"] --> TOP[["topic huella-events<br/>3 particiones · llave = departamento"]]
    TOP --> CO["Consumidor Python<br/>valida el contrato · actualiza métricas"]
    CO -- "inválidos" --> DLQ[["topic huella-events-dlq"]]
  end

  subgraph RT["Dashboard en tiempo real"]
    GF["Grafana · refresco cada 5 s"]
  end

  ECV --> EP
  API --> EA
  L --> F
  L --> D
  L --> AUD
  F --> Q
  D --> Q
  F -. "lectura de hogares" .-> PR
  CO -- "INSERT por lote" --> ST
  ST -- "consultas SQL" --> GF

  classDef gate fill:#fff3cd,stroke:#b8860b,color:#000
  classDef store fill:#e8f1fb,stroke:#2f5d9e,color:#000
  classDef task fill:#ffffff,stroke:#555,color:#000
  classDef stream fill:#e6f4ea,stroke:#2f855a,color:#000
  class G1,G2,G3 gate
  class ECV,API,RAWE,RAWA,PREP,F,D,AUD,ST store
  class EP,EA,T,L,Q,DASH task
  class PR,TOP,CO,DLQ,GF stream
```

*Cómo se lee:* las formas indican el tipo de elemento. Los cilindros y paralelogramos azules son datos (fuentes, archivos raw y preparados, tablas del DW); los hexágonos amarillos son las tres compuertas de validación de Great Expectations (GX); los rectángulos blancos son tareas de Airflow; los elementos verdes son la capa de streaming. Las flechas del batch son dependencias: una tarea solo corre si la anterior terminó bien. La flecha punteada indica que el productor **lee** los hogares ya cargados y validados en el DW; el streaming nunca escribe en el esquema estrella.

*Versión detallada de la parte batch, como imagen:* [`docs/architecture.png`](docs/architecture.png) (fuente editable: [`docs/architecture.dot`](docs/architecture.dot), se genera con Graphviz: `dot -Tpng docs/architecture.dot -o docs/architecture.png`). El diagrama Mermaid de arriba es el integrado (batch + streaming).

**Cómo se lee.** El flujo va de arriba hacia abajo: dos fuentes independientes, cada una con su propia rama de extracción, almacenamiento raw y compuerta de validación; las dos ramas se unen en la transformación e integración; el resultado se valida otra vez antes de cargarse al Data Warehouse, y de ahí salen las consultas y el dashboard analítico. Todo el recuadro del batch lo orquesta Apache Airflow. En paralelo, la capa de streaming lee los hogares del DW, los publica en Kafka, los valida en el consumidor y deja sus métricas en tablas `stream_*` que lee Grafana.

| Componente | Qué es | Dónde vive |
|---|---|---|
| Fuente primaria | ECV 2025 del DANE, 4 capítulos en CSV | `data/raw/ecv/` (no se sube a Git) |
| Fuente API | Temperatura del aire del IDEAM, vía Socrata (dataset `sbwg-7ju4`) | datos.gov.co |
| `extract_primary` | Copia los capítulos tal como llegan, sin limpiar | tarea de Airflow |
| `extract_api` | 53 ventanas de 7 días agregadas en el servidor; cada ventana descargada se guarda y no se vuelve a pedir si la tarea se reintenta | tarea de Airflow |
| Raw ECV y raw API | Copia fiel de cada fuente, con su manifiesto, por corrida | `data/work/<run_id>/raw/` |
| Compuertas 1 y 2 | Validación raw con Great Expectations, una por rama | `validate_primary_raw`, `validate_api_raw` |
| `transform_and_clean_and_integrate` | Limpieza y armonización de la ECV, temperatura por departamento, integración por código DANE y construcción de las tablas del modelo estrella | tarea de Airflow |
| Preparado | 6 tablas CSV y `integration_report.json`, por corrida | `data/work/<run_id>/prepared/` |
| Compuerta 3 | Validación de los datos preparados, una suite por tabla | `validate_prepared` |
| `load_dimensional_dw` | Carga transaccional: borra, inserta dimensiones y hechos, concilia conteos y confirma, o deshace todo si algo falla | tarea de Airflow |
| Data Warehouse | MySQL 8 con esquema en estrella (`fact_huella_carbono` y cinco dimensiones) y `etl_audit_log` | contenedor `mysql-dw`, puerto 3307 |
| Consultas y dashboard | Consultas SQL y KPI de R1–R7, y un dashboard conectado al DW (sección 7.1) | `sql/` y herramienta de BI |
| Productor de Kafka | Lee los hogares de `fact_huella_carbono` y los publica como eventos (simulación) | `kafka/producer_events.py` |
| Kafka y Zookeeper | Broker que guarda y distribuye los eventos: topic `huella-events` (3 particiones) y `huella-events-dlq` | contenedores `kafka` (puerto 9092) y `zookeeper` |
| Consumidor de Kafka | Valida cada evento con el contrato, actualiza las métricas y escribe en las tablas `stream_*` | `kafka/consumer_events.py` |
| Dashboard en tiempo real | Grafana con el dashboard aprovisionado desde el repositorio; lee las tablas `stream_*` cada 5 s | contenedor `grafana`, puerto 3000 |
| Orquestación | Apache Airflow 3.1.8: dependencias, reintentos selectivos, estados, logs e interfaz web; sus metadatos están en PostgreSQL | contenedores de Docker Compose |

**Capas de persistencia, de la más cruda a la más refinada:** (1) las fuentes originales, (2) los archivos raw por corrida, (3) los archivos preparados por corrida, (4) el Data Warehouse, y (5) las tablas `stream_*` de la capa de streaming. Los archivos raw y preparados son almacenamiento intermedio y de evidencia; **no son el DW analítico**: las consultas y el dashboard leen solo el DW.

**Compuertas de validación.** Hay tres, y una falla Critical en cualquiera hace fallar la tarea y deja sin ejecutar todo lo posterior (la transformación no corre si falla una de las dos compuertas raw, y nada se carga si falla la tercera). Las reglas de severidad `warning` se reportan sin bloquear.

**Qué cubre el diagrama frente a lo que pide la guía:**

| Elemento exigido | Dónde está en el diagrama |
|---|---|
| Fuente primaria y fuente API programática | Recuadro "Fuentes de datos" |
| Extracción o persistencia raw de cada rama | `extract_primary` → RAW ECV; `extract_api` → RAW API |
| Compuertas de validación raw de ambas fuentes | Compuertas 1 y 2 |
| Transformación e integración | `transform_and_clean_and_integrate` |
| Validación de los datos preparados | Compuerta 3 |
| Data Warehouse dimensional en MySQL | Recuadro "Data Warehouse · MySQL 8" |
| Consultas SQL, KPI y dashboard | Recuadro "Consumo analítico" |
| Airflow: dependencias, reintentos, logs y monitoreo | Recuadro "Apache Airflow" (las flechas son las dependencias; `extract_api` muestra sus reintentos) y la tabla de componentes de esta sección |

**Proceso de negocio y grano.** El proceso que se modela es la estimación anual de la huella de carbono residencial de un hogar. El grano de la tabla de hechos es **un registro por hogar encuestado** (`DIRECTORIO` + `SECUENCIA_P`, llaves que identifican al hogar).

## 4. Pipeline ETL batch

> **Responsable:** Nikol Hurtado (Persona 1: Infraestructura y Orquestación)

### 4.1 Diseño del DAG de Airflow e interfaces de las tareas

**Lo que muestra Airflow.** Vista de grafo del DAG `etl_huella_carbono` en la interfaz web de Airflow 3.1.8:

![Vista de grafo del DAG en Airflow](docs/evidence/airflow_dag_grafo.png)

*Qué demuestra:* el DAG tiene **7 tareas** en dos ramas independientes que se unen. A la izquierda, `extract_primary` con `validate_primary_raw` y `extract_api` con `validate_api_raw`; en el centro, `transform_and_clean_and_integrate`; a la derecha, `validate_prepared` y `load_dimensional_dw`.

*Cómo leer las flechas.* Hay dos tipos de dependencia, y ambas se ven en el grafo:
- **Dependencias de datos.** En el código, cada tarea recibe como argumento lo que devolvió otra. `validate_primary_raw` recibe la salida de `extract_primary`, `validate_api_raw` la de `extract_api`, y la transformación recibe las dos extracciones. Por eso salen líneas directas desde `extract_primary` y `extract_api` hacia la transformación, y otra desde la transformación hacia `load_dimensional_dw`, que recibe las tablas preparadas.
- **Dependencias de compuerta, escritas a propósito.** `[validate_primary_raw, validate_api_raw] >> transform_and_clean_and_integrate` y `validate_prepared >> load_dimensional_dw`. Hacen que la transformación solo corra si **ambas** compuertas raw aprobaron, y que nada se cargue si la compuerta de datos preparados falla, aunque la carga ya tenga sus datos listos.

**Configuración del DAG** (en [`dags/etl_huella_carbono_dag.py`](dags/etl_huella_carbono_dag.py)):

| Parámetro | Valor | Por qué |
|---|---|---|
| `schedule` | ninguno (ejecución manual) | Cada corrida se dispara y se inspecciona a mano |
| `max_active_runs` | 1 | Dos cargas simultáneas al DW no son seguras |
| `catchup` | falso | No se recuperan fechas pasadas |
| `execution_timeout` | 20 minutos por tarea (la API tiene 60) | Una tarea colgada no bloquea el pipeline |
| Parámetro `ecv_source` | `raw` (datos reales) o `test` (lote dañado de la Prueba B) | Permite la falla controlada sin tocar el código |
| Reintentos | Ninguno en validaciones, extracción local y transformación; 3 en `extract_api` y 3 en la carga | Solo se reintenta lo transitorio (sección 4.3) |

**Interfaces de las tareas:**

| Tarea | Entrada | Salida | Identidad del lote | Señal de falla | Re-ejecución |
|---|---|---|---|---|---|
| extract_primary | parámetro `ecv_source` | rutas raw + `ecv_manifest.json` | `run_id` | excepción (archivo o columna faltante) | rescribe `data/work/<run_id>/` |
| extract_api | fechas de `.env` | `api_ideam_raw.csv` + `api_manifest.json` | `run_id` | `APITransientError` / `APIContractError` | reanuda: reutiliza las ventanas ya descargadas |
| validate_*_raw | ruta del lote raw | resumen de GX + JSON | `run_id` | `ValueError` si hay Critical | idempotente |
| transform_and_clean_and_integrate | rutas raw | rutas de 6 tablas preparadas + reporte de integración | `run_id` | excepción | reescribe `data/work/<run_id>/prepared/` |
| validate_prepared | rutas de las 6 tablas preparadas | resumen de GX por tabla | `run_id` | `ValueError` si hay Critical | idempotente |
| load_dimensional_dw | rutas preparadas | conciliación de conteos + filas en `etl_audit_log` | `run_id` | `LoadReconciliationError` / error de BD | transaccional: DELETE+INSERT+COMMIT; ROLLBACK si falla (prueba C) |

Las tareas intercambian rutas y metadatos compactos; nunca DataFrames completos por XCom.

### 4.2 Estrategia de transformación y contrato de integración

La estrategia de integración une los microdatos de la ECV con los datos climáticos del IDEAM utilizando el departamento como eje geográfico. El cruce se realiza en memoria mediante Pandas durante la transformación, garantizando la calidad antes de la carga a MySQL.

**Contrato de Integración:**
*   **Llave ECV:** `P1_DEPARTAMENTO` (código DANE numérico), extraído del capítulo "Datos de la vivienda".
*   **Estrato:** `P8520S1A1` (estrato para tarifa de energía), también del capítulo "Datos de la vivienda" y unido por `DIRECTORIO` + `SECUENCIA_P`. Los códigos 0, 8 y 9 y los vacíos se reclasifican a "sin estrato" (valor 0 en la base) (decisión D-07).
*   **Llave API:** `departamento` (texto del IDEAM), el cual se normaliza (minúsculas, sin tildes ni puntuación) y se traduce al código DANE usando el diccionario `data/reference/dane_departamentos.csv` (incluyendo alias como Bogotá y San Andrés).
*   **Cardinalidad:** Muchos a uno (N:1). Muchos hogares se asocian a 1 única fila de clima por departamento.
*   **Alineación temporal:** Ambas fuentes corresponden a ventanas temporales del año 2025.
*   **Tratamiento de no emparejados:** 
    *   Hogares en un departamento sin estaciones del IDEAM: Quedan con `fk_clima` en NULL y bandera `clima_emparejado = 0`. Se conservan para responder R1 a R5, pero se excluyen de R6 y R7.
    *   Departamentos del IDEAM sin código DANE: Se descartan y se reportan en la bitácora.
*   **Tratamiento de duplicados:** Se agrupan múltiples estaciones por departamento calculando un promedio ponderado por el número de lecturas. Filas con temperaturas fuera del rango físico plausible (ej. < -15°C o > 50°C) se excluyen.
*   **Supuestos de negocio (S14):** Los cortes de piso térmico se establecieron en: Cálido (>24°C), Templado (18°C - 24°C) y Frío (<18°C). El promedio departamental resume el clima de las estaciones, asumiendo que representa la región.

*   **Cobertura real de la API y decisión sobre Vaupés (D-02):** la consulta al IDEAM en tres ventanas de 2025 dio **32 de 33 departamentos** ([evidencia](docs/evidence/cobertura_api_ideam_2025.md)). **Vaupés (97) no tiene estaciones con datos**, así que sus 1.509 hogares (≈1,7 %) quedan con `fk_clima` NULL y `clima_emparejado = 0`: se conservan en R1–R5 y se excluyen de R6 y R7. **No se le asigna 0 °C ni la temperatura de otro departamento**, porque eso sería un dato inventado: Vaupés es cálido y un 0 lo clasificaría como "Frío", sesgando R6 y R7. Razonamiento completo en [`docs/supuestos_y_trazabilidad.md`](docs/supuestos_y_trazabilidad.md) (D-02).

**Evidencia de transformación e integración (antes y después).** El pipeline genera automáticamente un informe con los conteos de cada corrida. Estos son los de la corrida exitosa `manual__2026-10-23T04_01_15.889885_00_00` (archivo completo: [`docs/evidence/integration_report.json`](docs/evidence/integration_report.json)):

*Limpieza de la ECV (fuente primaria).*

| Paso | Hogares / valores |
|---|---|
| Hogares de entrada (4 capítulos unidos) | 87.060 |
| Duplicados eliminados (llave `DIRECTORIO` + `SECUENCIA_P`, las que identifican al hogar) | 212 |
| Hogares sin estrato 1 a 6 (códigos 0, 8, 9 o vacío), reclasificados a "sin estrato" | 10.060 |
| `P5018` (pago del último mes por electricidad) con código 98 o 99, pasado a nulo | 233 |
| `P5018` con pago de varios meses, dividido entre los meses (`P5018S1`) | 7.039 |
| `P3163` (pago de gas natural) con código 98 o 99, pasado a nulo | 208 |
| `P3163` con pago de varios meses, dividido entre los meses (`P3163S1`) | 214 |
| `P8540` (pago de combustible para cocinar) con código 98 o 99, pasado a nulo | 471 |
| `P5018` nulos imputados con la mediana del estrato | 20.202 |
| `P5018` atípicos truncados al percentil 99 | 761 |
| `P5018` en cero imputados (estrato distinto de 1) | 71 |
| **Hogares de salida** | **86.848** (= 87.060 − 212) |

*Integración con la API del IDEAM (llave: código de departamento DANE).*

| Indicador | Valor |
|---|---|
| Filas de la API (estación × ventana de 7 días) | 14.963 |
| Filas sin `suma` o `n`, o con temperatura fuera de −15…50 °C, excluidas | 0 |
| Nombres de departamento del IDEAM sin código DANE (descartados) | 0 |
| Departamentos en la ECV | 33 |
| Departamentos con clima (filas de `dim_clima`) | 32 |
| Departamento de la ECV sin clima | 97 (Vaupés) |
| **Hogares con clima emparejado** | **85.339 (98,26 %)** |
| **Hogares sin clima** (`fk_clima` NULL, `clima_emparejado = 0`) | **1.509 (1,74 %)** |

*Interpretación.*
- Los 86.848 hogares coinciden con los de `dim_hogar` y con las filas de `fact_huella_carbono` en el DW (sección 11.1): ningún hogar se perdió en la integración.
- **Hogares no emparejados:** solo los de Vaupés. Se conservan para R1–R5 y se excluyen de R6 y R7, según la decisión D-02 de la sección anterior.
- **Coincidencias repetidas o ambiguas:** no pueden ocurrir. `dim_clima` tiene una sola fila por departamento (32 filas para 32 departamentos), así que cada hogar cruza como máximo con una fila de clima. Las estaciones múltiples de un departamento se resumen antes del cruce con un promedio ponderado por número de lecturas.
- **Calidad de la API:** en esta corrida ninguna fila fue excluida por faltar datos o por salir del rango físico, y todos los nombres de departamento se tradujeron a código DANE.
- **Límite de la integración:** la temperatura es un promedio departamental. Resume el clima de las estaciones del departamento y no distingue entre municipios de distinta altitud; por eso R6 y R7 se interpretan como asociaciones a nivel de departamento (supuesto S14).

### 4.3 Políticas de falla y reintentos

**Principio.** Un reintento solo tiene sentido si la falla **puede desaparecer sola** al repetir la tarea sin cambiar datos ni código (red caída, servicio ocupado). Si la falla es **determinista** (un dato inválido, una regla Critical, una tabla inexistente), repetir solo gasta tiempo y reproduce el mismo error; en ese caso la tarea falla de inmediato y se deja la causa visible en el log.

**Configuración en el DAG** (`dags/etl_huella_carbono_dag.py`):

| Tarea | Reintentos | Espera entre reintentos | Tope de espera | Tiempo máximo |
|---|---|---|---|---|
| `extract_api` | 3 | 1 min, exponencial (1 → 2 → 4 min) | 5 min | 60 min |
| `load_dimensional_dw` | 3 | 30 s, exponencial (30 → 60 → 120 s) | 3 min | — |
| `extract_primary`, `validate_primary_raw`, `validate_api_raw`, `transform_and_clean_and_integrate`, `validate_prepared` | 0 | — | — | — |

**Condiciones de falla y respuesta:**

| Condición | Tipo | Respuesta | Reintento | Evidencia o justificación |
|---|---|---|---|---|
| API caída, timeout, HTTP 429 o 5xx | Transitoria | Primero 3 intentos locales por ventana (esperas de 5 y 10 s); si persiste, falla la tarea y Airflow la reintenta | Sí | Justificación abajo. No se forzó una falla transitoria en Docker. |
| API responde 4xx o con forma inesperada | Determinista | `APIContractError` → `AirflowFailException` | No | Repetir la misma petición reproduce el error |
| MySQL no disponible, conexión cortada, deadlock, timeout de bloqueo | Transitoria | La excepción se relanza y Airflow reintenta | Sí | Justificación abajo. No se forzó en Docker. |
| Error de datos, de esquema o de restricción en la carga | Determinista | `AirflowFailException`; `ROLLBACK` | No | **Prueba C-2** (sección 11.3): error 1644, falló en el intento 1 y no se reintentó |
| Conteos del DW que no concilian con lo preparado | Determinista | `LoadReconciliationError`; `ROLLBACK` | No | Mismo lote daría el mismo resultado |
| Falta un archivo o una columna requerida de la ECV | Determinista | Falla de `extract_primary` | No | Reintentar no crea el archivo ni la columna |
| Critical de Great Expectations (validación raw o preparada) | Determinista | Falla la compuerta; lo posterior no se ejecuta | No | **Prueba B** (sección 11.2): `validate_primary_raw` falló en el intento 1 y las tres tareas siguientes quedaron sin ejecutar |

**Por qué estos valores.**
- **API (3 reintentos, 1 → 2 → 4 min, tope 5 min).** `extract_api` hace unas 53 consultas seguidas (año 2025 en ventanas de 7 días) a un servicio público. En nuestras corridas la tarea tardó 1 min 28 s, 4 min 45 s y 4 min 53 s: el tiempo de respuesta varía bastante entre una corrida y otra. Por eso hay dos capas: los intentos por ventana (5 y 10 s) absorben fallos cortos sin repetir la tarea, y los reintentos de la tarea esperan un total de 7 minutos para cubrir una indisponibilidad más larga. Cada ventana descargada se guarda como JSON, de modo que **un reintento continúa desde la ventana que falló, no desde el principio**: reintentar es barato. El tiempo máximo de 60 min es unas 12 veces lo observado, para que una API lenta no se confunda con una caída.
- **Base de datos (3 reintentos, 30 → 60 → 120 s, tope 3 min).** La primera espera de 30 s coincide con el periodo de arranque que damos a MySQL en `docker-compose.yaml` (`start_period: 30s`), y el total de 3,5 min cubre un reinicio del contenedor. Reintentar la carga es seguro porque es transaccional y repetible: una carga repetida sobre el mismo lote no duplica (Prueba C) y una carga interrumpida se deshace por completo (Prueba C-2).
- **Sin reintentos en el resto.** Extracción local, validaciones y transformación operan sobre archivos que no cambian entre intentos, así que una segunda ejecución daría el mismo resultado.

**Alcance de la evidencia.** Las dos decisiones de **no reintentar** están demostradas en Docker (Pruebas B y C-2). Los reintentos ante fallas **transitorias** están configurados y justificados, pero no se forzó en Docker una caída de la API ni de MySQL, por lo que no se presentan como evidencia ejecutada.

## 5. Calidad de datos

> **Responsable:** David Aponte (Persona 2: Calidad de Datos y API)

### 5.1 Riesgos y reglas de calidad

30 reglas de calidad (DQ01–DQ30) distribuidas en 3 compuertas. Cada umbral tiene base empírica,
contractual, analítica o física documentada. Ningún umbral fue elegido únicamente porque los datos actuales lo cumplen.

### 5.2 Diseño de Great Expectations y trazabilidad de reglas

| Elemento GX | Implementación |
|---|---|
| Objeto de datos | DataFrame del lote raw (compuertas 1 y 2) o preparado (compuerta 3) |
| Expectation | Una por regla; el ID de la regla va en `meta.rule_id`; severidad en `expectation.severity` |
| Suites (compuerta 1 — raw ECV) | `ecv_servicios_raw_suite` (DQ01–DQ04), `ecv_vivienda_raw_suite` (DQ05–DQ08), `ecv_caracteristicas_raw_suite` (DQ09), `ecv_condiciones_raw_suite` (DQ10–DQ11) |
| Suites (compuerta 2 — raw API) | `api_ideam_raw_suite` (DQ12–DQ18) |
| Suites (compuerta 3 — preparados) | `prepared_fact_huella_suite` (DQ19–DQ25), `prepared_dim_hogar_suite` (DQ26), `prepared_dim_ubicacion_suite` (DQ27), `prepared_dim_clima_suite` (DQ28), `prepared_dim_tarifas_suite` (DQ29), `prepared_dim_combustibles_suite` (DQ30) |
| Validation Definition | Batch del DataFrame + suite (`src/validation.py: run_suite`) — modo `ephemeral` (sin servidor GX) |
| Ejecución controlada | `validate_primary_raw` → `run_ecv_raw_gate()`; `validate_api_raw` → `run_api_raw_gate()`; `validate_prepared` → `run_prepared_gate()` |
| Resultado | `gx/validation_results/<run_id>/<suite>.json`; suites exportadas a `gx/expectations/<suite>.json` |
| Respuesta del pipeline | Critical bloquea (`ValueError`); Warning se registra y continúa; Info solo monitoreo |

### 5.3 Diseño de validación raw de ambas fuentes

Compuerta 1 (`run_ecv_raw_gate`): valida las 4 tablas de la ECV en paralelo; cualquier Critical bloquea la transformación.  
Compuerta 2 (`run_api_raw_gate`): valida el lote de la API; cualquier Critical bloquea la transformación.  
Ambas compuertas deben superarse antes de que `transform_and_clean_and_integrate` reciba datos.

### 5.4 Validación de datos preparados

Compuerta 3 implementada en `run_prepared_gate()` (`src/validation.py`). Valida 6 tablas:

| Tabla | Suite | Reglas | Checks clave |
|---|---|---|---|
| `fact_huella_carbono` | `prepared_fact_huella_suite` | DQ19–DQ25 | Grano único, FKs no nulas, emparejamiento ≥50%, emisiones ≥0, consistencia total |
| `dim_hogar` | `prepared_dim_hogar_suite` | DQ26 | `sk_hogar` único, `num_personas` ≥1, tenencia en `{Sí, No}` |
| `dim_ubicacion` | `prepared_dim_ubicacion_suite` | DQ27 | Surrogate único, estrato en [0,6], código departamento no nulo |
| `dim_clima` | `prepared_dim_clima_suite` | DQ28 | 1 fila/departamento, temperatura [-15,50]°C, consistencia min≤media≤max, piso térmico en dominio, ≥30 dptos |
| `dim_tarifas` | `prepared_dim_tarifas_suite` | DQ29 | Surrogate único, estratos 1–6, costo > 0 |
| `dim_combustibles` | `prepared_dim_combustibles_suite` | DQ30 | Surrogate único, factor emisión > 0 |

## 6. Data Warehouse

> **Responsable:** Diana Panesso (Persona 3: Transformación, DW y Streaming)

### 6.1 Modelo dimensional refinado y justificación de cambios

```mermaid
erDiagram
  dim_hogar ||--o{ fact_huella_carbono : "fk_hogar"
  dim_ubicacion ||--o{ fact_huella_carbono : "fk_ubicacion"
  dim_clima |o--o{ fact_huella_carbono : "fk_clima (opcional)"
  dim_combustibles |o--o{ fact_huella_carbono : "fk_combustible_cocina (opcional)"
  dim_tarifas |o--o{ fact_huella_carbono : "fk_tarifa_energia (opcional)"

  fact_huella_carbono {
    int sk_fact PK
    int fk_hogar FK
    int fk_ubicacion FK
    int fk_clima FK
    int fk_combustible_cocina FK
    int fk_tarifa_energia FK
    tinyint clima_emparejado
    decimal kwh_consumidos_estimados
    decimal emisiones_energia_kg
    decimal emisiones_coccion_kg
    decimal huella_total_anual_kg
  }
  dim_hogar {
    int sk_hogar PK
    varchar directorio
    varchar secuencia_p
    int num_personas
    varchar tiene_vehiculo
    varchar bien_p1077s11
    varchar bien_p1077s12
  }
  dim_ubicacion {
    int sk_ubicacion PK
    int codigo_departamento
    varchar nombre_departamento
    int estrato
  }
  dim_clima {
    int sk_clima PK
    int codigo_departamento
    varchar nombre_departamento
    int anio
    decimal temp_media_c
    decimal temp_min_c
    decimal temp_max_c
    int n_estaciones
    int n_lecturas
    varchar piso_termico
  }
  dim_combustibles {
    int sk_combustible PK
    varchar tipo_combustible
    decimal factor_emision_kg_tj
  }
  dim_tarifas {
    int sk_tarifa PK
    int estrato_aplica
    decimal costo_promedio_kwh
  }
```

*Cómo se lee:* cada recuadro es una tabla con sus columnas; `PK` es la llave primaria y `FK` la llave foránea. Las líneas unen la tabla de hechos con cada dimensión (un hogar de la tabla de hechos apunta a una fila de cada dimensión). Las llaves del clima, el combustible y la tarifa pueden ser nulas: no todo hogar tiene clima emparejado, combustible de cocina ni tarifa.

*Versión como imagen:* [`docs/star_schema.png`](docs/star_schema.png) (fuente: [`docs/star_schema.dot`](docs/star_schema.dot)). El modelo de la primera entrega está en [`docs/evidence/star_schema.png`](docs/evidence/star_schema.png).

**Tablas del modelo:**

| Tabla | Tipo | Grano (una fila es...) | Llave | Filas | Fuente |
|---|---|---|---|---|---|
| `fact_huella_carbono` | Hechos | un hogar encuestado | `sk_fact` | 86.848 | ECV 2025, calculada en la transformación |
| `dim_hogar` | Dimensión | un hogar | `sk_hogar` | 86.848 | ECV (características y condiciones) |
| `dim_ubicacion` | Dimensión | un departamento y un estrato | `sk_ubicacion` | 192 | ECV (vivienda y servicios) |
| `dim_clima` | Dimensión (**nueva**) | un departamento | `sk_clima` | 32 | **API del IDEAM** |
| `dim_combustibles` | Dimensión | un combustible de cocina | `sk_combustible` | 3 | `data/reference/dim_combustibles.csv` |
| `dim_tarifas` | Dimensión | un estrato | `sk_tarifa` | 6 | `data/reference/dim_tarifas.csv` |
| `etl_audit_log` | Auditoría (fuera del modelo estrella) | una tabla cargada en una corrida | — | 6 por corrida | pipeline |

**Grano del modelo:** 
Una fila en la tabla de hechos (`fact_huella_carbono`) representa la estimación del consumo eléctrico y la huella de carbono anual de **un (1) hogar encuestado** en la ECV 2025.

**Bitácora de cambios frente a la Fase 1.** Para responder R6 y R7, el esquema estrella de la primera entrega se refinó así (comparación hecha contra [`docs/fase1_referencia/create_dw.sql`](docs/fase1_referencia/create_dw.sql)):

| Cambio | Dónde | Por qué |
|---|---|---|
| **Nueva dimensión `dim_clima`** (temperatura media, mínima y máxima, estaciones, lecturas y `piso_termico`) | Modelo | Es el lugar donde entra la API del IDEAM: sin ella no hay clima por departamento y R6 y R7 no tienen respuesta |
| Nueva llave `fk_clima` (puede ser nula) | `fact_huella_carbono` | Une cada hogar con el clima de su departamento. Nula para Vaupés, que no tiene datos (D-02) |
| Nueva columna `clima_emparejado` (0 o 1) | `fact_huella_carbono` | Permite excluir de R6 y R7 a los hogares sin clima sin perderlos para R1 a R5 |
| Nuevas columnas `bien_p1077s11` (aire acondicionado) y `bien_p1077s12` (ventilador) | `dim_hogar` | Datos de R7: porcentaje de hogares con equipo de climatización |
| Medidas en `DECIMAL` en vez de `REAL` | `fact_huella_carbono` y dimensiones | Evita errores de redondeo de punto flotante al sumar y promediar emisiones (decisión de diseño) |
| Restricciones `ON DELETE RESTRICT`, valores por defecto e índices | Todas | Protegen la integridad referencial y aceleran las consultas por estrato, clima y combustible |
| Nueva tabla `etl_audit_log` | Fuera del modelo estrella | Registra cada carga (tabla, filas, estado), como respaldo de la conciliación y del `ROLLBACK` (sección 11.3) |

**Validación de Requisitos (R1 - R7):**
El modelo actualizado mantiene plena compatibilidad con los requerimientos originales, permitiendo cruzar las emisiones con el estrato (`dim_ubicacion`), combustible (`dim_combustibles`) y características del hogar (`dim_hogar`). Además, el nuevo modelo valida con éxito la expansión analítica al permitir cruzar el consumo eléctrico y las emisiones contra la nueva `dim_clima` para responder a las preguntas R6 y R7.

### 6.2 Implementación y carga del Data Warehouse

La base de datos analítica es **MySQL 8** (contenedor `mysql-dw`, expuesto en el puerto 3307 del equipo). El Data Warehouse es independiente de la base de metadatos de Airflow (PostgreSQL) y de los archivos raw o preparados, que son solo almacenamiento intermedio.

*   **Estructuras dimensionales:** [`sql/dw_schema.sql`](sql/dw_schema.sql) crea con `CREATE TABLE IF NOT EXISTS` cinco dimensiones (`dim_hogar`, `dim_ubicacion`, `dim_clima`, `dim_combustibles`, `dim_tarifas`), la tabla de hechos `fact_huella_carbono` y la tabla de auditoría `etl_audit_log`. `load_dw` aplica el esquema en cada carga, así que el DW se crea solo al primer uso.
*   **Grano de la tabla de hechos:** un registro por hogar encuestado (`DIRECTORIO` + `SECUENCIA_P`, llaves que identifican al hogar).
*   **Llaves y relaciones:** cada tabla tiene llave primaria surrogate (`sk_*`). La tabla de hechos tiene cinco llaves foráneas (`fk_hogar`, `fk_ubicacion`, `fk_clima`, `fk_combustible_cocina`, `fk_tarifa_energia`) con `ON DELETE RESTRICT`, así que MySQL rechaza cualquier hecho que apunte a una dimensión inexistente. `fk_clima`, `fk_combustible_cocina` y `fk_tarifa_energia` admiten NULL; en `fk_clima` es la forma de representar a los hogares sin clima (Vaupés), junto con la bandera `clima_emparejado = 0`.
*   **Solo datos aceptados:** `load_dimensional_dw` depende de `validate_prepared` en el DAG. Si esa compuerta tiene un Critical, la carga ni siquiera empieza (ver sección 5.4).
*   **Orden de carga:** primero se borran los hechos y luego las dimensiones; después se insertan las dimensiones (padres) y por último los hechos (hija), siempre dentro de una sola transacción (sección 11.3). Los `INSERT` se hacen en bloques de 5.000 filas.
*   **Conciliación de conteos:** antes de confirmar, la carga cuenta las filas de cada tabla en MySQL y las compara con las filas de los archivos preparados. Si alguna tabla no coincide lanza `LoadReconciliationError` y se hace `ROLLBACK`. Cada carga exitosa deja una fila por tabla en `etl_audit_log`.

**Evidencia de carga.** Conteo directo en MySQL después de la corrida exitosa (Prueba A, sección 11.1):

![Conteos del DW](docs/evidence/prueba_A_conteos_dw.png)

| Tabla | Filas en el DW |
|---|---|
| `dim_hogar` | 86.848 |
| `dim_ubicacion` | 192 |
| `dim_clima` | 32 |
| `dim_combustibles` | 3 |
| `dim_tarifas` | 6 |
| `fact_huella_carbono` | 86.848 |

**Controles de integridad y conciliación.** Consultas de solo lectura sobre el DW cargado:

```sql
SELECT clima_emparejado, COUNT(*) AS hechos FROM fact_huella_carbono GROUP BY clima_emparejado;
SELECT COUNT(*) AS filas_hechos, COUNT(DISTINCT fk_hogar) AS hogares_distintos FROM fact_huella_carbono;
SELECT COUNT(*) AS hechos_sin_hogar FROM fact_huella_carbono f
  LEFT JOIN dim_hogar h ON f.fk_hogar = h.sk_hogar WHERE h.sk_hogar IS NULL;
SELECT COUNT(*) AS hechos_con_clima_inexistente FROM fact_huella_carbono f
  LEFT JOIN dim_clima c ON f.fk_clima = c.sk_clima WHERE f.fk_clima IS NOT NULL AND c.sk_clima IS NULL;
```

![Controles de integridad del DW](docs/evidence/dw_integridad_conciliacion.png)

| Control | Resultado | Qué demuestra |
|---|---|---|
| Hechos con `clima_emparejado = 1` | 85.339 | Coincide con `hogares_con_clima` del reporte de integración (sección 4.2) |
| Hechos con `clima_emparejado = 0` | 1.509 | Coincide con `hogares_sin_clima`: los hogares de Vaupés, conservados con `fk_clima` NULL |
| Filas de hechos / hogares distintos | 86.848 / 86.848 | Se cumple el grano: una fila por hogar, sin repetidos |
| Hechos sin hogar en `dim_hogar` | 0 | No hay hechos huérfanos |
| Hechos con clima inexistente en `dim_clima` | 0 | Integridad referencial de la dimensión de clima |

**Interpretación.** Los 86.848 hogares aceptados por la compuerta de datos preparados llegaron completos al DW: ninguno se perdió ni se duplicó. La suma de hechos con y sin clima (85.339 + 1.509) es igual al total, y esos dos números coinciden con el informe que genera la transformación, así que la conciliación cuadra de extremo a extremo (ECV → transformación → DW). Los hogares sin clima no son un error: están contados, marcados y excluidos de R6 y R7 por la decisión D-02.

### 6.3 Tablas de la capa de streaming

El consumidor de Kafka escribe en tres tablas de la misma base (`huella_carbono_dw_v2`). Están definidas en [`sql/streaming_schema.sql`](sql/streaming_schema.sql) y el consumidor las crea solo si no existen. **No forman parte del esquema estrella:** la carga batch (`load_dimensional_dw`) solo borra y recarga las tablas del modelo dimensional, así que recargar el DW no borra el historial de streaming.

| Tabla | Grano | Llave | Uso |
|---|---|---|---|
| `stream_events` | Un evento válido procesado | `event_id` (UUID) | Historial de eventos con su posición en Kafka (`kafka_partition`, `kafka_offset`), la latencia y la marca `alta_huella`. La llave primaria hace idempotente la escritura: un evento repetido no se cuenta dos veces |
| `stream_invalid_events` | Un mensaje rechazado | `id`; único por (`kafka_partition`, `kafka_offset`) | Texto original del mensaje y motivo del rechazo |
| `stream_metrics_snapshot` | Una foto de las métricas por lote (o cada 5 s sin eventos) | `snapshot_ts` | Serie de tiempo que dibuja el dashboard en tiempo real |

**Estrategia de carga.** El consumidor escribe cada lote leído de Kafka (hasta 200 mensajes) en **una transacción** de MySQL: eventos válidos, inválidos y la foto de métricas. Solo después de confirmar la transacción confirma el offset en Kafka (sección 8.5).

## 7. Dashboard analítico (batch)

> **Responsable:** Sebastián Celorio (Persona 4: Analítica y Negocio)

### 7.1 Dashboard y salidas analíticas

El producto analítico es un informe de **Power BI Desktop** (`dashboard/dashboard_huella_carbono.pbix`) conectado al Data Warehouse MySQL (`localhost:3307`, base `huella_carbono_dw_v2`). Importa las seis tablas del esquema estrella con sus relaciones; **no lee CSV, respuestas crudas de la API ni archivos intermedios**. Los pasos de conexión están en [`docs/bi_dashboard_guide.md`](docs/bi_dashboard_guide.md). El [dashboard publicado en Power BI](https://app.powerbi.com/view?r=eyJrIjoiZmVhYWMzZGMtNGI1ZS00N2Q0LWJlMTAtMTYzM2YyMTAwMzAzIiwidCI6IjY5M2NiZWEwLTRlZjktNDI1NC04OTc3LTc2ZTA1Y2I1ZjU1NiIsImMiOjR9) también está disponible en la web.

Todas las gráficas del informe deben ser **visuales nativos de Power BI**. No usar visuales de Python ni generar las gráficas con scripts de Python. Para conectarse a MySQL, obtener `MYSQL_USER` y `MYSQL_PASSWORD` del archivo local `.env`; **no pedir, pegar ni compartir estas credenciales en chats**.

Las cuatro evidencias requeridas son `docs/evidence/esquema_powerbi.PNG` (nombre de archivo: `esquema_powerbi.PNG`), `docs/evidence/dashboard_conexion_mysql.png`, `docs/evidence/dashboard_p1_huella_residencial.png` y `docs/evidence/dashboard_p2_api_clima.png`.

![Esquema del modelo de Power BI](docs/evidence/esquema_powerbi.PNG)

![Conexión de Power BI al Data Warehouse MySQL](docs/evidence/dashboard_conexion_mysql.png)

*Qué demuestra:* el servidor `localhost:3307` y la base `huella_carbono_dw_v2` son el DW de la sección 6.2, y las tablas importadas son las del modelo de la sección 6.1.

**Página 1 — Huella residencial (R1 a R5).**
- Tarjetas: hogares (86.848), huella media por hogar (859 kg CO₂/año) y emisión eléctrica media (307 kg CO₂/año).
- Gráficas: R1 columnas por estrato (sin los hogares sin estrato); R2 barras por combustible de cocina; R3′ barras por estrato y tenencia de carro; R4 barras por departamento (huella per cápita); R5 líneas por tamaño del hogar.
- Filtros: estrato y departamento.

![Página 1 del dashboard: R1 a R5](docs/evidence/dashboard_p1_huella_residencial.png)

**Página 2 — Valor de la API del IDEAM (R6 y R7).**
- Tarjetas: hogares con clima emparejado (85.339, 98,3 %), kWh/mes en piso cálido contra frío (132,5 contra 94,5, es decir +40,3 %) y diferencia de CO₂ eléctrico (+100,5 kg/año por hogar).
- Gráficas: R6 columnas de kWh por estrato y piso térmico; R7 dispersión de temperatura departamental contra porcentaje de hogares con ventilador y con aire acondicionado (un punto por departamento).
- Filtros: departamento (permite ver los resultados sin Arauca, decisión D-05) y piso térmico.

![Página 2 del dashboard: R6 y R7 con la API](docs/evidence/dashboard_p2_api_clima.png)

**Requisito → elemento del DW → visualización → valor de la integración.**

| Req. | Elemento del DW o consulta | KPI o visualización | Valor de la integración demostrado |
|---|---|---|---|
| R1 | `fact_huella_carbono` + `dim_ubicacion`; consulta R1 de [`sql/analytical_queries.sql`](sql/analytical_queries.sql) | Columnas por estrato (kWh y kg CO₂) | No usa la API |
| R2 | `fact_huella_carbono` + `dim_combustibles`; consulta R2 | Barras por combustible de cocina | No usa la API |
| R3′ | `fact_huella_carbono` + `dim_hogar` + `dim_ubicacion`; consulta R3 | Barras por estrato y tenencia de carro | No usa la API |
| R4 | `fact_huella_carbono` + `dim_ubicacion` + `dim_hogar` (personas); consulta R4 | Barras por departamento (huella per cápita) | No usa la API |
| R5 | `fact_huella_carbono` + `dim_hogar`; consulta R5 | Líneas por tamaño del hogar (huella per cápita) | No usa la API |
| R6 | `fact_huella_carbono` + `dim_clima` (**API**) + `dim_ubicacion`; consulta R6 | Tarjetas de kWh y CO₂ cálido contra frío; columnas por estrato y piso térmico | **Sin la API no existe `dim_clima`**: no habría piso térmico y la pregunta no tendría respuesta |
| R7 | `dim_hogar` (columnas de aire acondicionado, `P1077S11`, y de ventilador, `P1077S12`) + `dim_clima` (**API**); consulta R7 | Dispersión temperatura contra % de hogares con ventilador y con aire acondicionado | **Sin la API** no se podría ordenar ni comparar los departamentos por temperatura |

La tarjeta "hogares con clima emparejado" (98,3 %) y las visualizaciones de R6 y R7 son el **KPI y las gráficas que demuestran el valor de integrar la API**: dependen por completo de `dim_clima`, que solo existe por la fuente del IDEAM.

*Vista previa alternativa sin Power BI:* `python scripts/dashboard_bi.py` genera una página HTML con los mismos cálculos directamente desde MySQL. Sirve solo para revisar resultados; no sustituye el informe evaluado ni sus visuales nativos de Power BI.

### 7.2 Trazabilidad requisito → KPI de extremo a extremo

**Parte A — Requisito → consulta → resultado → visualización.** Todos los números salen del Data Warehouse tras la corrida final (sección 11.1). Las consultas están en [`sql/analytical_queries.sql`](sql/analytical_queries.sql).

| Req. | Consulta | Resultado principal | Visualización y evidencia |
|---|---|---|---|
| R1 | R1 | El consumo eléctrico sube con el estrato: de 119,4 kWh/mes (estrato 1) a 175,0 (estrato 6), con el estrato 3 (124,5) algo por debajo del 2 (132,1). La huella eléctrica pasa de 315 a 462 kg CO₂/año y la huella total de 903 a 1.624 kg/año. Los 10.060 hogares sin estrato no se interpretan | Página 1 del dashboard |
| R2 | R2 | GLP 994 kg/año de cocina, gas natural 433, leña 0 (carbono neutra, D-04) | Página 1 |
| R3′ | R3 | A igual estrato, los hogares **con** carro tienen mayor huella que los que no: estrato 1, 1.127 contra 893; estrato 2, 1.064 contra 858; estrato 3, 1.054 contra 854; estrato 4, 1.058 contra 865; estrato 5, 1.290 contra 1.040; estrato 6, 1.770 contra 1.223 kg. Global: 1.112 contra 832 kg | Página 1 |
| R4 | R4 | Mayor huella per cápita: San Andrés 649, Amazonas 467, Arauca 403, Antioquia 394 y Caldas 377 kg/año. Menor: Vaupés 104, La Guajira 198 y Casanare 223 | Página 1 |
| R5 | R5 | La huella per cápita baja de 709 kg (hogares de 1 persona) a 431 (2), 301 (3), 236 (4) y 109 (8). La huella total del hogar casi no cambia entre 2 y 8 personas (862–945 kg), así que la curva refleja economías de escala | Página 1 |
| R6 | R6 | A igual estrato, cálido > templado > frío en los seis estratos (entre cálido y frío, de 55 % a 127 %); global: 132,5 contra 94,5 kWh (+40,3 %) y 349,9 contra 249,4 kg CO₂/año | Página 2; capturas de la sección 2.2 |
| R7 | R7 | El porcentaje de hogares con ventilador y con aire acondicionado sube con la temperatura: ventilador 71,8 % (cálido), 31,9 % (templado) y 19,6 % (frío); aire acondicionado 9,3 %, 2,3 % y 2,2 % | Página 2; capturas de la sección 2.2 |

*Qué cambió frente a la primera entrega.*
- **R3′:** el resultado "contraintuitivo" de la primera entrega (los hogares sin carro con mayor huella: 1.232 contra 1.030 kg) venía de la leña. Los hogares sin carro cocinan más con leña y el factor completo inflaba su huella. Con la leña como carbono neutra, el resultado es el esperado. Esto es una interpretación respaldada por el cambio de resultado, no una prueba causal.
- **R4:** los departamentos que lideraban (Vichada, Guainía, Amazonas y Vaupés) eran los que más cocinan con leña. Ahora Vaupés, La Guajira y Casanare quedan al final, en parte por esa misma convención.
- **R5:** la relación inversa se mantiene, con valores más bajos que en la primera entrega (de 709 a 109 kg frente a 1.000 a 243 kg).
- **R1, R3′ y R6:** además, ahora usan el estrato verdadero (`P8520S1A1`, decisión D-07). La primera entrega usaba `P5010`, que son los cuartos donde duermen las personas; por eso la tendencia de R1 es distinta: con el estrato real el consumo sube con el estrato.
- Los valores absolutos de R1 a R5 no son comparables con los de la primera entrega por las correcciones D-04, D-06, D-07 y D-08 (sección 12.1). R4 usa las personas de la muestra, sin factores de expansión de la ECV: describe la muestra, no la población nacional.

**Parte B — Cadena de extremo a extremo (filas representativas).** Cada fila sigue el recorrido requisito, dato requerido, fuente, riesgo de calidad, regla, Expectation de Great Expectations, transformación, elemento del DW y visualización. Todas las reglas están en [`docs/quality_rules.md`](docs/quality_rules.md) y en `src/validation.py`. Respuesta del pipeline: **Critical** bloquea la tarea, **Warning** se registra y continúa (sección 4.3).

| Req. | Dato requerido y fuente | Riesgo de calidad | Regla (severidad) | Expectation de GX | Transformación o integración | Elemento del DW | Visualización |
|---|---|---|---|---|---|---|---|
| R6, R7 | `P1_DEPARTAMENTO` (ECV vivienda) | Departamento nulo o inválido: el hogar no se puede cruzar con el clima | DQ07 y DQ08 (Critical) | `ExpectColumnValuesToNotBeNull` y `ExpectColumnValuesToBeInSet` sobre `P1_DEPARTAMENTO` | Llave de integración: código DANE | `fact_huella_carbono.fk_clima`, `dim_ubicacion` | R6 y R7 (página 2) |
| R6, R7 | `departamento` (API IDEAM) | Estaciones sin departamento: no aportan a `dim_clima` | DQ14 (Critical, ≥ 99 %) | `ExpectColumnValuesToNotBeNull(departamento, mostly=0.99)` | Normalización del nombre y traducción a código DANE con `dane_departamentos.csv` | `dim_clima.codigo_departamento` | R6 y R7 |
| R6, R7 | `minimo` y `maximo` de temperatura (API) | Fallas de sensor (el IDEAM no validó los valores) | DQ15 (Critical, ≥ 99,5 %) | `ExpectColumnValuesToBeBetween(-15, 50, mostly=0.995)` | Se excluyen filas fuera de rango; promedio por departamento ponderado por lecturas | `dim_clima.temp_media_c`, `piso_termico` | R6 y R7 |
| R6, R7 | Cobertura de departamentos (API) | Departamentos sin estaciones dejan hogares sin clima | DQ17 (Warning, ≥ 30; decisión D-03) | `ExpectColumnUniqueValueCountToBeBetween(min_value=30)` | Hogares sin clima: `fk_clima` NULL y `clima_emparejado = 0` (D-02, Vaupés) | `fact_huella_carbono.clima_emparejado` | Tarjeta "hogares con clima" (98,3 %) |
| R1, R3′, R6 | `P8520S1A1`, estrato para tarifa de energía (ECV vivienda) | Código fuera del diccionario DANE, o muy pocos hogares con estrato 1 a 6 | DQ05 (Warning) | `ExpectColumnValuesToBeInSet(P8520S1A1, [0–6, 8, 9], mostly=0.999)` y `ExpectColumnValuesToBeBetween(P8520S1A1, 1, 6, mostly=0.85)` | Códigos 0, 8, 9 y vacíos → "sin estrato" (0); estrato 1 a 6 se conserva (D-07) | `dim_ubicacion.estrato`, `dim_tarifas` | R1, R3′ y R6 |
| R1, R6 | `P5018`, pago de energía (ECV servicios) | Muchos valores nulos: la imputación dominaría el KPI | DQ04 (Warning, ≥ 70 %) | `ExpectColumnValuesToNotBeNull(P5018, mostly=0.70)` | Imputación con la mediana del estrato, truncado al percentil 99 y kWh = pago ÷ tarifa (S1, S2) | `fact_huella_carbono.kwh_consumidos_estimados` | R1 y R6 |
| R3′ | `P1077S15`, carro particular (ECV condiciones) | Código fuera de {1, 2} | DQ10 (Critical) | `ExpectColumnValuesToBeInSet(P1077S15, [1, 2])` | 1 → Sí, 2 → No | `dim_hogar.tiene_vehiculo` | R3′ (página 1) |
| R7 | `P1077S11` aire acondicionado y `P1077S12` ventilador (ECV condiciones) | Código fuera de {1, 2} | DQ11 (Warning) | `ExpectColumnValuesToBeInSet(P1077S11 y P1077S12, [1, 2])` | 1 → Sí, 2 → No | `dim_hogar.bien_p1077s11`, `bien_p1077s12` | R7 (página 2) |
| R1–R7 | Un hogar por fila (hechos) | Duplicados: doble conteo de emisiones | DQ19 (Critical) | `ExpectColumnValuesToBeUnique(sk_fact y fk_hogar)` | Una fila de hechos por hogar; llaves sustitutas | `fact_huella_carbono` (grano: un hogar) | Todas las tarjetas y gráficas |
| R1–R7 | Emisiones y huella (hechos) | Valores negativos o huella menor que sus componentes | DQ22 y DQ23 (Critical) | `ExpectColumnValuesToBeBetween(min 0)` y `ExpectColumnPairValuesAToBeGreaterThanB` | Cálculo de emisiones (S3, S8, S9) | `emisiones_energia_kg`, `emisiones_coccion_kg`, `huella_total_anual_kg` | Todas |

La tabla completa de reglas (DQ01 a DQ30) y los resultados de validación de cada corrida están en [`docs/quality_rules.md`](docs/quality_rules.md) y `gx/validation_results/<run_id>/`.

## 8. Pipeline de streaming

> **Responsable:** Diana Panesso (Persona 3: Transformación, DW y Streaming)

### 8.1 Componentes

| Componente | Archivo | Función |
|---|---|---|
| Contrato del evento | [`kafka/event_contract.py`](kafka/event_contract.py) | Campos obligatorios, tipos, rangos y función `validate_event()`. Lo usan el productor y el consumidor |
| Productor | [`kafka/producer_events.py`](kafka/producer_events.py) | Lee los hogares del DW, arma un evento por hogar y lo publica en `huella-events` |
| Consumidor | [`kafka/consumer_events.py`](kafka/consumer_events.py) | Valida cada evento, guarda válidos e inválidos, actualiza las métricas y confirma offsets |
| Inyector de eventos inválidos | [`kafka/send_invalid_events.py`](kafka/send_invalid_events.py) | Experimento: publica 8 eventos que rompen el contrato |
| Reinicio de la demo | [`kafka/reset_streaming.py`](kafka/reset_streaming.py) | Borra los topics, el grupo de consumidores y las tablas `stream_*` (no toca el esquema estrella) |
| Configuración común | [`kafka/common.py`](kafka/common.py) | Lee el `.env`, conecta a MySQL y crea los topics |
| Broker | `docker-compose.yaml` (servicios `zookeeper` y `kafka`) | `confluentinc/cp-kafka:7.4.0`. Desde el equipo: `localhost:9092`; dentro de Docker: `kafka:29092`. Retención de 7 días |

### 8.2 Generación de eventos (productor)

1. Consulta `fact_huella_carbono` unida con `dim_ubicacion`, `dim_hogar` y `dim_clima` (los hogares sin clima, como Vaupés, quedan con piso térmico "Sin clima").
2. Ordena los hogares con `ORDER BY RAND(semilla)`: el orden es aleatorio pero se repite igual en cada corrida (`--seed 2025`).
3. Por cada hogar arma el evento, **lo valida con el mismo contrato** (el productor nunca publica un evento que lo rompa) y lo publica con `acks=all`: espera la confirmación del broker antes de seguir.
4. Publica a la tasa indicada (`--rate`, por defecto 2 eventos/s). `--limit` corta la corrida y `--skip` continúa donde quedó otra (el productor imprime el valor al terminar).

**Por qué 2 eventos por segundo:** permite ver en el dashboard cómo cambian las métricas evento a evento (con refresco de 5 s, unos 10 eventos por refresco) sin saturar la demostración; el consumidor procesa mucho más rápido (en la prueba de interrupción procesó 177 eventos atrasados en un solo lote).

### 8.3 Configuración del topic

| Parámetro | Valor | Motivo |
|---|---|---|
| Topic de eventos | `huella-events` | Lo crea el productor (o el consumidor) si no existe |
| Particiones | 3 (`KAFKA_TOPIC_PARTITIONS`) | Permite repartir la carga entre hasta 3 consumidores del mismo grupo |
| Factor de replicación | 1 | Hay un solo broker (entorno local); ver limitaciones (sección 12.2) |
| Llave del mensaje | código DANE del departamento (texto, p. ej. `"76"`) | Todos los eventos de un departamento van a la misma partición y se procesan en orden |
| Retención | 7 días (`KAFKA_LOG_RETENTION_HOURS=168`) | Si el consumidor se detiene, los eventos esperan en Kafka y se procesan al volver |
| Topic de errores (DLQ) | `huella-events-dlq` | Copia de cada mensaje inválido, con el motivo en la cabecera `error` |
| Grupo de consumidores | `huella-stream-consumer` | Guarda en Kafka el último offset confirmado |

### 8.4 Contrato del evento (versión 1)

Ejemplo real de un evento publicado:

```json
{
  "event_id": "3f1c2a8e-5b7d-4e2a-9c41-0d6f8a2b7e10",
  "event_type": "household_footprint",
  "schema_version": 1,
  "event_ts": "2026-11-19T02:44:10.512Z",
  "simulated": true,
  "sk_fact": 40213,
  "codigo_departamento": 47,
  "nombre_departamento": "Magdalena",
  "estrato": 1,
  "piso_termico": "Cálido",
  "num_personas": 4,
  "kwh_mes": 138.26,
  "emisiones_energia_kg": 365.0,
  "emisiones_coccion_kg": 269.0,
  "huella_total_anual_kg": 634.0
}
```

*(Los valores del ejemplo son ilustrativos; la forma y los tipos son los del contrato.)*

| Campo | Tipo | Regla |
|---|---|---|
| `event_id` | texto | **Identificador del evento.** UUID v4 único; es la llave primaria en `stream_events` |
| `event_type` | texto | Siempre `household_footprint` |
| `schema_version` | entero | Siempre `1` (permite cambiar el contrato sin romper consumidores) |
| `event_ts` | texto | **Marca de tiempo** ISO 8601 en UTC, terminada en `Z` |
| `simulated` | booleano | `true`: el evento re-emite un hogar histórico |
| `sk_fact` | entero | Fila de `fact_huella_carbono` que se re-emite |
| `codigo_departamento` | entero | Código DANE de 5 a 99. **Llave del evento:** debe coincidir con la llave del mensaje de Kafka |
| `nombre_departamento` | texto | — |
| `estrato` | entero | 0 (sin estrato) a 6 |
| `piso_termico` | texto | `Cálido`, `Templado`, `Frío` o `Sin clima` |
| `num_personas` | entero | ≥ 1 |
| `kwh_mes` | número | 0 a 5.000 |
| `emisiones_energia_kg`, `emisiones_coccion_kg` | número | ≥ 0 |
| `huella_total_anual_kg` | número | ≥ 0 y ≥ energía + cocción (misma idea que la regla DQ23 del batch) |

**Validaciones del consumidor.** Un mensaje es inválido si no es JSON, si falta un campo o sobra uno que el contrato no define, si un tipo no coincide (un `bool` no se acepta como número), si un número no es finito, si `event_id` no es UUID, si `event_ts` no es ISO 8601 UTC, si un valor está fuera de rango o si la llave del mensaje no coincide con `codigo_departamento`. El evento inválido **no se suma a las métricas**: se guarda en `stream_invalid_events` con el motivo, se copia al topic `huella-events-dlq` y el consumidor sigue con el siguiente.

### 8.5 Consumidor y métricas de streaming

Por cada lote (hasta 200 mensajes, espera máxima de 1 s):

1. Valida cada mensaje con el contrato.
2. Válido → `INSERT IGNORE` en `stream_events` y actualiza las métricas en memoria. Si el `event_id` ya existía (re-entrega de Kafka), se ignora y no se cuenta.
3. Inválido → `stream_invalid_events` + copia a la DLQ.
4. Escribe una foto de las métricas en `stream_metrics_snapshot`.
5. Confirma la transacción en MySQL y **después** el offset en Kafka (`enable_auto_commit=False`).

Sin eventos, escribe una foto cada 5 s para que "eventos en el último minuto" baje a 0 cuando el productor se detiene.

| # | Métrica de streaming | Cómo se calcula | Requisito |
|---|---|---|---|
| 1 | Eventos válidos, inválidos y % de inválidos | Contadores acumulados | S-R3 |
| 2 | Eventos en el último minuto | Eventos válidos con `processed_at` en los últimos 60 s (ventana deslizante) | S-R3 |
| 3 | Huella media acumulada | Suma de `huella_total_anual_kg` / eventos válidos | S-R2 |
| 4 | kWh/mes medio: cálido contra templado/frío | Medias acumuladas por grupo de piso térmico | S-R2 (R6 en vivo) |
| 5 | Alertas de alta huella | Eventos con huella > p95 histórico de `fact_huella_carbono` (1.868,2 kg; se calcula al arrancar, o fijo con `STREAM_ALERT_KG`) | S-R1 |
| 6 | Latencia media | `processed_at − event_ts` de los eventos del lote | S-R3 |

**Al reiniciar, las métricas se reconstruyen desde `stream_events` y `stream_invalid_events`**, así que detener el consumidor no pierde el estado.

### 8.6 Garantías de entrega y comportamiento ante fallas

- **Al menos una vez, sin duplicados en el resultado.** Si el consumidor se cae después de guardar en MySQL pero antes de confirmar el offset, Kafka le entrega otra vez esos mensajes; `event_id` (y la posición partición/offset en los inválidos) evita contarlos dos veces.
- **Consumidor detenido:** los eventos se acumulan en Kafka (retraso o *lag* del grupo) y se procesan al volver, desde el último offset confirmado. Los eventos atrasados muestran una latencia alta porque `event_ts` es la hora de publicación (sección 11.4).
- **Evento inválido:** se aísla; no detiene el consumidor ni cambia las métricas, salvo el contador de inválidos.
- **MySQL caído:** la transacción falla, el offset no se confirma y el consumidor termina con error; al reiniciarlo reprocesa el lote pendiente.

## 9. Dashboard en tiempo real

> **Responsables:** Sebastián Celorio (Persona 4: Analítica y Negocio) & Diana Panesso (Persona 3: Streaming)

### 9.1 Integración consumidor → dashboard

```
productor → Kafka (huella-events) → consumidor → MySQL (tablas stream_*) → Grafana (consulta cada 5 s)
```

- **Mecanismo:** el consumidor escribe una fila en `stream_metrics_snapshot` por lote procesado (y cada 5 s sin eventos) y guarda cada evento en `stream_events`. Grafana consulta esas tablas con SQL directamente en MySQL.
- **Frecuencia de actualización:** el dashboard se refresca **cada 5 s** (`refresh: 5s`); el consumidor procesa cada evento en milisegundos, así que lo que se ve tiene como máximo unos 5 s de retraso.
- **Herramienta y reproducibilidad:** Grafana 11.3.0 corre en el mismo `docker-compose.yaml`. La conexión al DW ([`grafana/provisioning/datasources/mysql_dw.yaml`](grafana/provisioning/datasources/mysql_dw.yaml)) y el dashboard ([`grafana/dashboards/huella_streaming.json`](grafana/dashboards/huella_streaming.json)) se cargan solos al arrancar; no hay pasos manuales. Las credenciales salen del `.env`. El JSON se genera con [`scripts/build_grafana_dashboard.py`](scripts/build_grafana_dashboard.py), donde las consultas SQL de cada panel quedan legibles.
- **Por qué Grafana y no Power BI:** el conector de MySQL de Power BI trabaja en modo de importación, y la actualización automática de página necesita DirectQuery; Grafana consulta la base en cada refresco y su configuración queda versionada en el repositorio.

### 9.2 Paneles

| Panel | Métrica | Cómo cambia con eventos nuevos |
|---|---|---|
| Eventos válidos procesados | Métrica 1 | Sube con cada evento válido |
| Eventos en el último minuto | Métrica 2 | ≈120 con el productor a 2 eventos/s; baja a 0 si el productor o el consumidor se detienen; sube de golpe cuando el consumidor procesa eventos atrasados |
| Huella media acumulada (tarjeta y serie con el umbral p95) | Métrica 3 | Oscila con los primeros eventos y se estabiliza cerca de la media histórica (≈859 kg) |
| % eventos inválidos | Métrica 1 | Sube al inyectar eventos inválidos y baja a medida que llegan válidos |
| Alertas de alta huella (tarjeta y tabla de las últimas) | Métrica 5 | Aumenta cuando llega un hogar por encima de 1.868,2 kg |
| Latencia media del último lote | Métrica 6 | Decenas de ms en operación normal; decenas de segundos al ponerse al día tras una interrupción |
| kWh/mes: cálido contra templado/frío (serie y barras por piso térmico) | Métrica 4 | Las dos medias se separan: los hogares de departamentos cálidos consumen más (R6 en vivo) |
| Últimos eventos inválidos | Motivo, posición en Kafka y llave | Muestra cada rechazo apenas ocurre |
| Huella media por departamento (top 10) | Media por departamento con ≥ 5 eventos | El ranking cambia a medida que llegan hogares |

### 9.3 Principales hallazgos de la corrida

- Con 600 hogares re-emitidos, la huella media acumulada quedó en **835,1 kg CO₂/año**, cerca de la media histórica del DW (858,8 kg): la muestra aleatoria es representativa.
- La brecha de R6 aparece en vivo: **140 kWh/mes en piso cálido contra 124 en templado y 89 en frío** (barras por piso térmico).
- **30 de 600 hogares (5 %)** superaron el umbral p95, como se espera de un percentil 95.
- Latencia típica de **8 a 25 ms** entre la publicación y el procesamiento.

### 9.4 Diferencia con el dashboard analítico (batch)

| | Dashboard analítico (Power BI, sección 7) | Dashboard en tiempo real (Grafana) |
|---|---|---|
| Pregunta | ¿Qué pasó en la muestra de la ECV 2025? (R1–R7) | ¿Qué está pasando ahora con los reportes que llegan? (S-R1–S-R3) |
| Datos | Esquema estrella completo (86.848 hogares) | Tablas `stream_*`, que crecen evento a evento |
| Actualización | Cuando el DAG recarga el DW y se refresca el informe | Cada 5 s, automáticamente |
| Uso | Análisis histórico, comparaciones y conclusiones | Operación: alertas, salud del flujo y seguimiento de indicadores |

## 10. Ejecución y reproducibilidad

> **Responsable:** Nikol Hurtado (Persona 1: Infraestructura y Orquestación)

### 10.1 Instrucciones de instalación y ejecución

**Requisitos:** Docker Desktop (con Docker Compose) y unos 5 GB libres. No hace falta instalar Python ni MySQL en el equipo.

1. Clonar el repositorio.
2. Copiar `.env.example` a `.env` y reemplazar las contraseñas (sin los caracteres `@ : /`). `MYSQL_USER` puede cambiarse; la base y el usuario se crean con los valores del `.env`. El archivo `.env` no se versiona.
3. Descargar los CSV de la ECV 2025 desde https://microdatos.dane.gov.co/index.php/catalog/905 y dejarlos en `data/raw/ecv/` (no se versionan por su tamaño). El pipeline usa cuatro capítulos: "Datos de la vivienda", "Servicios del hogar", "Características y composición del hogar" y "Condiciones de vida del hogar y tenencia de bienes".
4. Construir y arrancar:
   ```
   docker compose build
   docker compose up airflow-init
   docker compose up -d
   ```
5. Abrir Airflow en http://localhost:8080 (usuario y contraseña de `.env`), activar y disparar el DAG `etl_huella_carbono` con el parámetro `ecv_source = raw`. Una corrida completa tarda unos 2 a 3 minutos.
6. El Data Warehouse MySQL queda en `localhost:3307` (base `huella_carbono_dw_v2`) para SQL y para el dashboard. Para comprobar la carga:
   ```
   "SELECT COUNT(*) FROM fact_huella_carbono;" | docker compose exec -T mysql-dw sh -c 'mysql -u $MYSQL_USER -p$MYSQL_PASSWORD $MYSQL_DATABASE'
   ```
   Debe dar 86.848.
7. Dashboard: abrir `dashboard/dashboard_huella_carbono.pbix` en Power BI Desktop (necesita el conector de MySQL, *MySQL Connector/NET*) o crearlo siguiendo [`docs/bi_dashboard_guide.md`](docs/bi_dashboard_guide.md). Servidor `localhost:3307`, base `huella_carbono_dw_v2`; obtener `MYSQL_USER` y `MYSQL_PASSWORD` del `.env` local y no compartirlos en chats. Usar visuales nativos de Power BI, sin visuales de Python.
8. Opcional, antes de correr: `python scripts/verificar_cobertura_api.py` para revisar la cobertura de estaciones del IDEAM.

*Configuración opcional.* `BIOMASS_CO2_NEUTRAL=0` en `.env` restaura el factor completo de CO₂ para la leña (decisión D-04). Con el valor por defecto (`1`) la leña cuenta como carbono neutra.

*Nota sobre consultas desde PowerShell:* los textos con tilde (por ejemplo `'Sí'`) se pueden estropear al enviarlos por la tubería. Las consultas del repositorio ya evitan esa dependencia.

### 10.2 Ejecutar el streaming y el dashboard en tiempo real

Requisitos adicionales: Python 3.10 o superior en el equipo (probado con 3.13). Los scripts de `kafka/` corren fuera de Docker y se conectan a Kafka en `localhost:9092` y al DW en `localhost:3307` con las credenciales del `.env`.

1. Agregar al `.env` la contraseña de Grafana (ver `.env.example`):
   ```
   GRAFANA_ADMIN_PASSWORD=una_contrasena
   ```
2. Con el DW ya cargado (sección 10.1), confirmar que Kafka, Zookeeper y Grafana estén arriba (`docker compose up -d` los inicia junto con el resto):
   ```
   docker compose up -d zookeeper kafka grafana
   ```
3. Instalar las dependencias de los scripts (una vez), de preferencia en un entorno virtual:
   ```
   python -m venv .venv
   .venv\Scripts\activate          # en Linux o macOS: source .venv/bin/activate
   pip install -r requirements-streaming.txt
   ```
4. Terminal 1, consumidor (crea las tablas `stream_*` si no existen):
   ```
   python kafka/consumer_events.py
   ```
5. Terminal 2, productor (2 eventos/s; `Ctrl+C` para detener):
   ```
   python kafka/producer_events.py --rate 2 --limit 600
   ```
6. Abrir el dashboard en http://localhost:3000 (se abre directo, en modo de solo lectura; para editar, usuario `admin` y la contraseña del `.env`).
7. Experimentos (sección 11.4):
   - Evento inválido: `python kafka/send_invalid_events.py`
   - Interrupción: detener el consumidor con `Ctrl+C` mientras el productor sigue, revisar el retraso y volver a iniciarlo:
     ```
     docker compose exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group huella-stream-consumer
     ```
8. Para repetir la demostración desde cero (detener antes productor y consumidor): `python kafka/reset_streaming.py`.

### 10.3 Verificación del sistema completo

| Qué verificar | Cómo | Resultado esperado |
|---|---|---|
| DAG batch | Airflow, http://localhost:8080 | Todas las tareas en verde (sección 11.1) |
| Carga del DW | Consulta de conteo del paso 6 de la sección 10.1 | 86.848 filas en `fact_huella_carbono` |
| Dashboard analítico | `dashboard/dashboard_huella_carbono.pbix` | Páginas 1 y 2 con R1–R7 |
| Productor y consumidor | Salida de las dos terminales | El consumidor imprime un resumen por lote con las métricas |
| Contrato | `python kafka/send_invalid_events.py` | 8 mensajes `[INVÁLIDO]` con su motivo; el consumidor sigue |
| Dashboard en tiempo real | http://localhost:3000 | Las tarjetas cambian cada 5 s mientras corre el productor |
| Sin duplicados | `SELECT COUNT(*), COUNT(DISTINCT event_id) FROM stream_events;` | Los dos conteos son iguales |

## 11. Evidencia funcional

> **Responsables:** Equipo Completo — Subsecciones: **11.1 y 11.3:** Nikol Hurtado · **11.2:** David Aponte · **11.4:** Diana Panesso

### 11.1 Evidencia de la corrida exitosa (Prueba A)

**Qué se ejecutó.** El DAG `etl_huella_carbono` en Docker (Airflow 3.1.8, MySQL 8), disparado a mano por el usuario `admin` con `ecv_source = raw` (ECV 2025 real) y la API del IDEAM para todo 2025 (ventanas de 7 días). Corrida `manual__2026-10-21T04:07:03.156493+00:00`.

![Corrida exitosa: 7 tareas en Éxito](docs/evidence/prueba_A_exito.png)

| Tarea | Estado | Intento | Duración |
|---|---|---|---|
| `extract_primary` | Éxito | 1 | 21,7 s |
| `extract_api` | Éxito | 1 | 1 min 28 s |
| `validate_primary_raw` | Éxito | 1 | 46,7 s |
| `validate_api_raw` | Éxito | 1 | 13,0 s |
| `transform_and_clean_and_integrate` | Éxito | 1 | 6,1 s |
| `validate_prepared` | Éxito | 1 | 11,4 s |
| `load_dimensional_dw` | Éxito | 1 | 12,0 s |

La corrida completa duró 2 min 17 s (inicio 23:07:05, fin 23:09:22, hora de Colombia).

**Conciliación en el Data Warehouse.** Conteo hecho directamente en MySQL después de la carga:

![Conteos del DW tras la Prueba A](docs/evidence/prueba_A_conteos_dw.png)

| Tabla | Filas |
|---|---|
| `dim_hogar` | 86.848 |
| `dim_ubicacion` | 192 |
| `dim_clima` | 32 |
| `dim_combustibles` | 3 |
| `dim_tarifas` | 6 |
| `fact_huella_carbono` | 86.848 |

**Interpretación.**
- Las 7 tareas pasaron al **primer intento**: no hizo falta ningún reintento, y las dos compuertas raw y la de datos preparados no encontraron fallas Critical.
- `fact_huella_carbono` tiene exactamente las mismas filas que `dim_hogar` (86.848): una fila de hechos por hogar, que es el grano declarado, y coincide con los 86.848 hogares finales de la Primera Entrega.
- `dim_clima` tiene 32 filas: los 32 departamentos con dato de temperatura del IDEAM. Vaupés no tiene estaciones con dato; su tratamiento está documentado en la decisión D-02 de [`docs/supuestos_y_trazabilidad.md`](docs/supuestos_y_trazabilidad.md) y en [`docs/evidence/cobertura_api_ideam_2025.md`](docs/evidence/cobertura_api_ideam_2025.md).
- La carga quedó registrada en `etl_audit_log` (6 filas, una por tabla, estado `SUCCESS`).

**Corrida final después de las correcciones D-04 y D-06.** Después de corregir tres puntos (leña como carbono neutra, factor eléctrico de la UPME y significado de `P1077S11`, aire acondicionado, y `P1077S12`, ventilador o abanico) se volvió a ejecutar el DAG completo con `ecv_source = raw`. Corrida `manual__2026-10-22T00:56:02.910409+00:00`: las 7 tareas terminaron en **Éxito al primer intento** en 2 min 1 s.

![Corrida final: 7 tareas en Éxito](docs/evidence/prueba_D_dag_final.png)

Los conteos de carga no cambiaron (mismos 86.848 hogares), porque las correcciones modifican valores, no filas. Lo que sí cambió son las medidas de emisiones. Consulta directa al DW después de esta corrida:

| Medida | Valor |
|---|---|
| Huella media por hogar | 905 kg CO₂/año |
| Emisión eléctrica media | 351 kg CO₂/año |
| Cocina con GLP (31.489 hogares) | 994 kg · huella total 1.323 kg |
| Cocina con gas natural (38.400 hogares) | 436 kg · huella total 849 kg |
| Cocina con leña (13.943 hogares) | 0 kg (carbono neutra) · huella total 245 kg |
| Sin combustible de cocina (3.016 hogares) | 0 kg · huella total 293 kg |

Estos valores fueron superados por la corrida posterior a D-07 y D-08, que sigue. Los de la primera entrega no son comparables en valor absoluto (sección 12.1).

**Corrida después de las correcciones D-07 y D-08.** Al revisar el diccionario de datos del DANE se encontró que la variable usada como estrato (`P5010`) es el número de cuartos donde duermen las personas, que los pagos de energía y gas natural pueden cubrir varios meses (`P5018S1` y `P3163S1`) y que los valores 98 y 99 no son montos. Se corrigió (decisiones D-07 y D-08, supuestos S10 y S17) y se volvió a ejecutar el DAG completo con `ecv_source = raw`. Corrida `manual__2026-10-23T04:01:15.889885+00:00`: las 7 tareas terminaron en **Éxito al primer intento** en 3 min 35 s.

Los conteos de carga no cambiaron (86.848 hogares), salvo `dim_ubicacion`: pasa de 193 a **192** filas, porque ahora el estrato tiene valores 0 (sin estrato) a 6 y antes tenía hasta 8 valores distintos. Consulta directa al DW después de esta corrida:

| Medida | Valor |
|---|---|
| Huella media por hogar | 859 kg CO₂/año |
| Emisión eléctrica media | 307 kg CO₂/año |
| Cocina con GLP (31.489 hogares) | 994 kg · huella total 1.291 kg |
| Cocina con gas natural (38.400 hogares) | 433 kg · huella total 805 kg |
| Cocina con leña (13.943 hogares) | 0 kg (carbono neutra) · huella total 164 kg |
| Sin combustible de cocina (3.016 hogares) | 0 kg · huella total 243 kg |

Por eso las secciones 2.2 y 7.2 usan los valores de esta corrida.

### 11.2 Evidencia de la falla controlada (Prueba B)

**Nota sobre las pruebas B, C y C-2.** Sus capturas se tomaron antes de la corrección D-07. En ellas `dim_ubicacion` aparece con 193 filas y, en el log de la Prueba B, la regla DQ05 menciona la columna `P5010`; con D-07 son 192 filas y la regla revisa `P8520S1A1` (suite de vivienda). El comportamiento que demuestran (bloqueo por Critical, repetibilidad y rollback) no depende de esos valores.

**Cómo se provoca.** `src/make_bad_data.py` no toca los archivos originales: crea una copia de `Datos de la vivienda.csv` en `data/test/` con **una sola** violación determinista. En la fila 0, `P1_DEPARTAMENTO` (código del departamento) pasa de `13` a `999`, un código que no existe en DIVIPOLA. Rompe la regla **DQ08 (Critical)**, porque ese código es la llave de integración con la API.

```
docker compose exec airflow-scheduler python /opt/airflow/src/make_bad_data.py
```

Después se dispara el DAG con `ecv_source = test`.

![Salida de make_bad_data.py](docs/evidence/prueba_B_datos_danados.png)

**Resultado en Airflow.** Corrida `manual__2026-10-21T21:18:07.523926+00:00`, estado final **Fallido**.

![Corrida en ejecución, con la falla ya visible](docs/evidence/prueba_B_en_ejecucion.png)

![Corrida terminada: falla en validate_primary_raw y carga bloqueada](docs/evidence/prueba_B_falla_controlada.png)

| Tarea | Estado | Intento |
|---|---|---|
| `extract_primary` | Éxito | 1 |
| `extract_api` | Éxito (4 min 53 s) | 1 |
| `validate_api_raw` | Éxito | 1 |
| `validate_primary_raw` | **Fallido** | 1 |
| `transform_and_clean_and_integrate` | Fallido en Upstream (no se ejecutó) | 0 |
| `validate_prepared` | Fallido en Upstream (no se ejecutó) | 0 |
| `load_dimensional_dw` | Fallido en Upstream (no se ejecutó) | 0 |

**Log de la falla.** Las suites de servicios, condiciones y características pasaron (“Compuerta superada según la política”). Solo falló `ecv_vivienda_raw_suite`, por la regla DQ08:

![Log de validate_primary_raw con la regla DQ08](docs/evidence/prueba_B_log_DQ08.png)

```
ValueError: [validate_primary_raw] Validacion con severidad CRITICA:
{'ecv_vivienda_raw_suite': ['DQ08']}. La transformacion queda bloqueada.
```

**El Data Warehouse queda intacto.** Conteo en MySQL después de la corrida fallida: los mismos valores de la Prueba A, y `etl_audit_log` conserva solo las cargas buenas anteriores (ninguna fila nueva de esta corrida).

![DW sin cambios tras la falla](docs/evidence/prueba_B_conteos_dw_intacto.png)

**Interpretación.**
- La falla es **determinista**: repetir la validación con el mismo archivo daría el mismo resultado. Por eso `validate_primary_raw` falló en el **intento 1** y Airflow no reintentó, como define la política de la sección 4.3 (Critical de Great Expectations: sin reintento).
- Las tres tareas posteriores quedaron en “Fallido en Upstream” con 0 intentos: Airflow ni siquiera las empezó. Eso demuestra que la compuerta bloquea la transformación y la carga.
- La rama de la API siguió su curso y terminó bien (`extract_api` y `validate_api_raw` en Éxito). Las dos ramas son independientes; la transformación solo corre cuando **ambas** cumplen la política.
- Las reglas de severidad `warning` (por ejemplo DQ04 y DQ05, en el mismo log) se reportan pero no bloquean; solo una falla Critical detiene el flujo.

Para volver a los datos reales basta ejecutar con `ecv_source = raw`. `data/test/` está en `.gitignore`.

### 11.3 Repetibilidad y estrategia de re-ejecución segura (Prueba C)

**Estrategia (`src/load.py`).**
1. **Esquema fuera de la transacción.** `CREATE TABLE IF NOT EXISTS` se aplica antes, en su propia conexión, porque en MySQL el DDL confirma de forma implícita y no puede ser parte de una transacción.
2. **Una sola transacción para los datos.** La conexión se abre con `autocommit=False`. Dentro de ella: `DELETE` (primero los hechos, luego las dimensiones, por las llaves foráneas), `INSERT` (primero las dimensiones, luego los hechos), conciliación de conteos contra los archivos preparados, registro en `etl_audit_log` y `COMMIT`.
3. **Sin `TRUNCATE`.** Es DDL y confirmaría el borrado antes de saber si la inserción sale bien.
4. **Rollback ante cualquier falla.** Si algo falla, incluida una conciliación que no cuadra, se hace `ROLLBACK` y el DW queda como estaba antes del intento. El intento fallido se deja registrado en `etl_audit_log` con estado `FAILED`, en una conexión aparte.
5. **Llaves deterministas.** Las llaves surrogate vienen de los archivos preparados, así que cargar el mismo lote dos veces deja el mismo contenido.

**Qué se ejecutó.** En la misma corrida de la Prueba A se limpió solo la tarea `load_dimensional_dw` (botón “Limpiar tarea”, sin tareas anteriores ni posteriores), lo que la repite sobre el mismo lote de datos preparados.

![Limpiar solo load_dimensional_dw](docs/evidence/prueba_C_limpiar_tarea.png)

La tarea terminó en Éxito en el **intento 2** (29 s). El log muestra que volvió a insertar exactamente las mismas cantidades y cerró con “Carga exitosa del lote… Conciliación”:

![Intento 2 de la carga, mismo lote](docs/evidence/prueba_C_reejecucion.png)

**Resultado.** Los conteos del DW son idénticos a los de la Prueba A, y `etl_audit_log` muestra las dos cargas del mismo lote, cada una con las 6 tablas en `SUCCESS` y las mismas filas cargadas (las horas están en UTC):

![Conteos y auditoría tras la repetición](docs/evidence/prueba_C_conteos_auditoria.png)

| Carga | Hora (UTC) | Tablas | Estado |
|---|---|---|---|
| Intento 1 | 2026-10-21 04:09:22 | 6 | SUCCESS |
| Intento 2 (repetición) | 2026-10-21 21:08:37 | 6 | SUCCESS |

**Interpretación.** Repetir la carga sobre el mismo lote no duplica ni pierde datos: `dim_hogar` y `fact_huella_carbono` siguen en 86.848 filas. Eso se debe a que la carga borra y reinserta dentro de una transacción con llaves deterministas, y a que la conciliación verifica los conteos antes de confirmar.

**Prueba C-2 — Falla a mitad de la carga (rollback demostrado en Docker).** La Prueba C comprueba la repetición. Esta prueba comprueba el otro caso que pide la guía: cómo se comporta el diseño cuando la carga se rompe **después** de haber borrado e insertado datos.

*Cómo se provoca, sin tocar el código.* Con `root` (el administrador de la base; el pipeline solo usa el usuario de aplicación) se crea un *trigger* temporal en la tabla de hechos que rechaza cualquier inserción. La carga borra todo, inserta las cinco dimensiones y falla al insertar la primera fila de hechos:

```
"CREATE TRIGGER trg_prueba_rollback BEFORE INSERT ON fact_huella_carbono FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Falla simulada para la prueba de rollback';" | docker compose exec -T mysql-dw sh -c 'mysql -u root -p$MYSQL_ROOT_PASSWORD $MYSQL_DATABASE'
```

![Trigger de falla creado](docs/evidence/prueba_C2_trigger_creado.png)

Luego se dispara el DAG con datos reales (`ecv_source = raw`). Corrida `manual__2026-10-21T22:11:02.143248+00:00`:

![Corrida con la carga fallida](docs/evidence/prueba_C2_falla_en_carga.png)

`extract_primary`, `extract_api`, `validate_primary_raw`, `validate_api_raw`, `transform_and_clean_and_integrate` y `validate_prepared` terminan en Éxito. `load_dimensional_dw` termina en **Fallido en el intento 1**, sin reintentos.

![Log de la carga: inserta las dimensiones y hace ROLLBACK](docs/evidence/prueba_C2_log_rollback.png)

El log muestra la secuencia: insertó 86.848 filas en `dim_hogar` y 193, 32, 3 y 6 en las otras dimensiones, y después:

```
ERROR - Falla en la carga del lote manual__2026-10-21T22:11:02.143248+00:00; ROLLBACK,
el DW queda como estaba: (1644, 'Falla simulada para la prueba de rollback')
AirflowFailException: Falla DETERMINISTA, no se reintenta: (1644, ...)
```

*Resultado en el Data Warehouse.* Conteo en MySQL después de la falla:

![DW intacto tras la falla a mitad de la carga](docs/evidence/prueba_C2_conteos_intactos.png)

| Tabla | Filas antes de la prueba | Filas después de la falla |
|---|---|---|
| `dim_hogar` | 86.848 | 86.848 |
| `dim_ubicacion` | 193 | 193 |
| `dim_clima` | 32 | 32 |
| `dim_combustibles` | 3 | 3 |
| `dim_tarifas` | 6 | 6 |
| `fact_huella_carbono` | 86.848 | 86.848 |

`etl_audit_log` conserva las dos cargas buenas anteriores (6 tablas, `SUCCESS`) y suma **una** fila del intento fallido (`FAILED`, detalle `RUN_FAILED: OperationalError`, 2026-10-21 22:17:14 UTC), escrita en una conexión aparte después del rollback.

*Interpretación.*
- Cuando la falla llegó, la transacción ya había ejecutado los `DELETE` de todas las tablas y los `INSERT` de las cinco dimensiones. Aun así, el DW quedó con exactamente los mismos conteos: el `ROLLBACK` deshizo todo, y no quedó una carga parcial.
- El error 1644 es de datos o de la regla del servidor, no de conexión. `is_transient` lo clasifica como **determinista**, así que Airflow no gasta reintentos que repetirían el mismo error (política de la sección 4.3, fila "Error de esquema o de datos en la carga").
- La falla queda **visible y auditada**: tarea en rojo, log con la causa y una fila `FAILED` en `etl_audit_log`.

*Limpieza.* El trigger se eliminó al terminar (`DROP TRIGGER trg_prueba_rollback;`) y `SHOW TRIGGERS` quedó vacío:

![Trigger eliminado](docs/evidence/prueba_C2_trigger_eliminado.png)

Para reproducir la prueba: crear el trigger con el comando de arriba, disparar el DAG, revisar y **quitar el trigger** antes de volver a usar el pipeline:

```
"DROP TRIGGER trg_prueba_rollback;" | docker compose exec -T mysql-dw sh -c 'mysql -u root -p$MYSQL_ROOT_PASSWORD $MYSQL_DATABASE'
```

### 11.4 Evidencia del streaming y del dashboard en tiempo real

Corrida del 19 de noviembre de 2026 (02:40 a 02:50 UTC). Se partió de cero (`kafka/reset_streaming.py`), con el DW de la corrida final (86.848 hogares) y el productor a 2 eventos/s con `--limit 600`. Los registros completos están en [`docs/evidence/streaming/`](docs/evidence/streaming/): [`producer_corrida.log`](docs/evidence/streaming/producer_corrida.log), [`consumer_corrida1.log`](docs/evidence/streaming/consumer_corrida1.log) y [`consumer_corrida2_reinicio.log`](docs/evidence/streaming/consumer_corrida2_reinicio.log).

**1. Operación normal: las métricas cambian con cada evento.** Productor y consumidor corriendo; el dashboard muestra 147 eventos válidos, ≈120 por minuto y la huella media estabilizándose.

![Dashboard en tiempo real en operación normal](docs/evidence/streaming/grafana_t1.png)

**2. Experimento: eventos inválidos.** Con el flujo normal en marcha se publicaron 8 eventos que rompen el contrato ([`send_invalid_events.log`](docs/evidence/streaming/send_invalid_events.log)). El consumidor rechazó los 8, cada uno con su motivo, y siguió procesando válidos sin detenerse:

```
[INVÁLIDO] p0 o75: JSON mal formado: Expecting value
[INVÁLIDO] p0 o76: faltan campos obligatorios: huella_total_anual_kg
[INVÁLIDO] p0 o77: kwh_mes debe ser número, llegó str
[INVÁLIDO] p0 o78: estrato fuera de 0 a 6
[INVÁLIDO] p0 o79: huella_total_anual_kg es negativo
[INVÁLIDO] p0 o80: la llave del mensaje ('5') no coincide con codigo_departamento
[INVÁLIDO] p0 o81: huella_total_anual_kg es menor que energía + cocción
[INVÁLIDO] p0 o82: event_ts debe estar en UTC y terminar en 'Z'
```

*Comportamiento observado:* ninguno de los 8 cambió la huella media ni las alertas; el % de inválidos subió a 4,4 % (8 de 182) y la tabla "Últimos eventos inválidos" los mostró en el siguiente refresco. Los 8 quedaron también en el topic `huella-events-dlq` (8 mensajes en la partición 0). Todos cayeron en la partición 0 porque comparten la llave `76`.

![Dashboard con los eventos inválidos](docs/evidence/streaming/grafana_t2_invalidos.png)

**3. Experimento: consumidor detenido.** Se detuvo el consumidor de forma abrupta (sin `Ctrl+C`, simulando una caída) mientras el productor seguía publicando. A los ≈40 s, el grupo tenía **125 eventos pendientes** ([`consumer_detenido_lag.txt`](docs/evidence/streaming/consumer_detenido_lag.txt)):

```
GROUP                  TOPIC          PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
huella-stream-consumer huella-events  0          171             239             68
huella-stream-consumer huella-events  1          106             143             37
huella-stream-consumer huella-events  2          54              74              20
```

En el dashboard, "eventos en el último minuto" empezó a bajar y los demás valores se congelaron: Grafana muestra la última foto guardada, no datos inventados.

![Dashboard con el consumidor detenido](docs/evidence/streaming/grafana_t3_consumidor_detenido.png)

**4. Reinicio y recuperación.** Al volver a iniciarlo, el consumidor reconstruyó su estado desde MySQL y procesó los atrasados en un solo lote:

```
Estado restaurado: 323 válidos, 8 inválidos, 18 alertas. Umbral de alerta (p95 histórico): 1,868.2 kg CO2/año.
Lote de 177 | válidos    500 | inválidos   8 (1.57%) | último min  177 | huella media 826.01 kg | ... | latencia 46223.9 ms
Lote de   2 | válidos    502 | inválidos   8 (1.57%) | último min  179 | huella media 828.64 kg | ... | latencia 379.0 ms
```

El retraso volvió a 0 en las tres particiones y, al terminar el productor, MySQL tenía **600 eventos y 600 `event_id` distintos** (ninguno perdido ni duplicado) y 8 inválidos ([`consumer_reiniciado_lag.txt`](docs/evidence/streaming/consumer_reiniciado_lag.txt)). En la gráfica de eventos por minuto se ve la caída durante la interrupción y el pico al ponerse al día.

![Dashboard después del reinicio del consumidor](docs/evidence/streaming/grafana_t4_reinicio.png)

*Comportamiento observado y limitaciones:*
- La latencia del lote de recuperación fue de **46,2 s**: es el tiempo que esos eventos esperaron en Kafka, no lentitud del consumidor (los lotes siguientes volvieron a 8–25 ms).
- La métrica "eventos en el último minuto" mide eventos **procesados**, por eso sube a 177 al recuperar aunque el productor siga a 2 eventos/s.
- Mientras el consumidor está detenido, el dashboard no distingue "no llegan eventos" de "el consumidor no procesa": las dos situaciones bajan la tasa a 0. El retraso del grupo en Kafka (comando de la sección 10.2) sí las distingue.
- Las alertas de esos 125 eventos llegaron con hasta 46 s de retraso; con retención de 7 días no se pierde ninguno, pero una caída más larga retrasaría las alertas en la misma medida.

## 12. Decisiones de ingeniería y limitaciones

> **Responsable:** Sebastián Celorio (Persona 4: Analítica y Negocio)

### 12.1 Supuestos y limitaciones del batch

- Los kWh de la ECV son estimados desde el pago y la tarifa; no son consumo medido y el pago refleja subsidios.
- La temperatura se promedia por departamento y oculta diferencias internas (por altitud, por ejemplo). Arauca, Guainía y Guaviare tienen una sola estación del IDEAM; su temperatura depende de un único punto. En Arauca el resultado (10,2 °C) es inconsistente con su clima y con su tenencia de ventiladores; se conserva y se reporta con y sin Arauca (decisión D-05, supuesto S16).
- **Vaupés no tiene datos de temperatura** y queda fuera de R6 y R7 (≈1,7 % de los hogares). Se declara como ausencia (NULL), no como 0 °C (decisión D-02).
- Con 33 departamentos, la relación temperatura–consumo es una asociación, no una causa.
- El IDEAM no validó los valores publicados (aviso de la ficha del dataset).
- Los precios de combustible de cocina siguen siendo fijos (limitación de la primera entrega, no resuelta en esta).
- **Leña (supuesto S9, decisión D-04):** la tabla de factores del proyecto marca la leña como carbono neutra ("no suma al total neto de CO₂, sí contabiliza CH₄ y N₂O"), y el IPCC (2006) respalda la convención: el CO₂ de la biomasa no se incluye en los totales. El pipeline lo aplica: el CO₂ de la leña se cuenta como 0 en la huella neta (`BIOMASS_CO2_NEUTRAL=1`, reversible con `0`). Esto **corrige la primera entrega**, que sumaba el factor completo (112.000 kg/TJ): el hallazgo original de R2 ("la leña emite 3 veces más") ya no se sostiene. Limitaciones: no se incluyen CH₄ ni N₂O (el IPCC da factores por defecto, 300 y 4 kg/TJ para leña residencial, pero la huella del proyecto se define como CO₂ y agregarlos exigiría aplicarlos a todos los combustibles; equivale a ≈8,4 kg de CH₄ y ≈0,11 kg de N₂O por hogar con leña y año), y la neutralidad supone cosecha sostenible, algo que la ECV no permite verificar.
- **Factor de emisión eléctrica (supuesto S3, decisión D-06):** la primera entrega usaba 0,097 kg CO₂/kWh, un valor atribuido a XM cuyo origen no se pudo verificar. La UPME publica el factor del SIN para inventarios y huellas de carbono: 0,177 en 2023 y **0,220 en 2024**. Esta entrega usa **0,220**, el dato oficial más cercano al periodo de la ECV (agosto 2024 a noviembre 2025). Esto **corrige la primera entrega**: las emisiones eléctricas y las huellas totales son ≈2,3 veces mayores en valor absoluto, mientras que los kWh y las comparaciones porcentuales no cambian. El factor es un promedio anual constante (no se modela variación mensual).
- **Efecto combinado de D-04 y D-06:** la huella media por hogar pasa de ≈1.213 kg/año (primera entrega) a ≈905 (−505 por la leña neutra y +196 por el factor eléctrico) y, con las correcciones D-07 y D-08, a ≈859 (−46 kg más, casi todo en la emisión eléctrica: de 351 a 307); un hogar con leña pasa de ≈3.253 a ≈164 (huella total). Por eso los valores absolutos de R1 a R5 de la primera entrega no son comparables con los de esta (sección 7.2).
- **Estrato (decisión D-07, supuesto S10):** la primera entrega usó `P5010` como "estrato". Según el diccionario del DANE esa variable es el número de cuartos donde duermen las personas del hogar; el estrato para tarifa de energía es `P8520S1A1` (capítulo Datos de la vivienda), con los códigos 0 (factura sin estrato o servicio ilegal), 8 (vivienda con planta eléctrica) y 9 (no sabe el estrato y no tiene factura). Esta entrega usa `P8520S1A1`. Los 10.060 hogares (11,6 %) sin estrato 1 a 6 se agrupan como "sin estrato" (valor 0 en la base): se usan en R2, R4, R5 y R7, no se interpretan en las vistas por estrato y, para estimar kWh, reciben la tarifa del estrato 1 como respaldo. Los hogares con planta eléctrica pueden pagar poco o nada por la red, así que su consumo estimado subestima el real.
- **Pagos de varios meses (decisión D-08):** `P5018S1` y `P3163S1` indican a cuántos meses corresponde el pago. El 10,6 % de los pagos de energía informados cubre 2 meses o más; se divide entre los meses. Si el dato de meses falta o está fuera de 1 a 12, se supone 1 mes. El pago de energía puede incluir el alumbrado público si viene en la factura (nota del DANE), lo que sobreestima algo el consumo.
- **Códigos 98 y 99 (supuesto S17):** en `P8540` el DANE define 98 como propano pagado con la administración o el arriendo (valor no separable) y deja 99 sin definir; en `P5018` y `P3163` el diccionario no los define, pero el siguiente valor más bajo es 1.000. Se tratan como dato no informado (233, 208 y 471 hogares).
- **Combustible de cocina (supuesto S18):** el carbón de leña (`P8536` = 7, 428 hogares) se agrupa con la leña por ser biomasa; esa agrupación es una decisión del equipo, no del DANE. Los hogares que cocinan con electricidad, petróleo o derivados, carbón mineral o material de desecho, y los que no informan, no suman emisión de cocina.
- **Supuestos sin fuente oficial.** Siguen sin respaldo documental el uso de tarifas fijas por estrato (S1), el precio del gas natural (S4), el precio del GLP (S5), el poder calorífico del gas natural (parte de S6) y el consumo de 150 kg de leña por mes (S7, el más fuerte porque no depende del gasto del hogar). Los subsidios de S2 tienen solo una fuente secundaria (prensa). Se declaran en [`docs/supuestos_y_trazabilidad.md`](docs/supuestos_y_trazabilidad.md). En cambio, los factores de emisión de combustibles (S8), el poder calorífico de leña y GLP (S6) y el factor eléctrico (S3) tienen fuente oficial.
- **Solo CO₂.** La huella se mide en kg de CO₂, no en CO₂ equivalente: no incluye CH₄ ni N₂O.
- **Muestra, no población.** Las cifras describen los 86.848 hogares de la muestra de la ECV. El pipeline no aplica factores de expansión, así que no son totales nacionales.

### 12.2 Decisiones y limitaciones del streaming

| Decisión | Motivo | Alternativa descartada |
|---|---|---|
| Re-emitir hogares del DW como eventos (simulación declarada con `simulated: true`) | La ECV no tiene una fuente de eventos real; el enunciado permite simular la llegada con datos históricos | Publicar los mismos KPI cada pocos segundos (versión anterior): el enunciado lo desaconseja porque los valores no cambian |
| Llave = código DANE del departamento, 3 particiones | Orden por departamento y posibilidad de escalar a 3 consumidores | Sin llave: reparto al azar y sin orden por departamento |
| Offsets manuales, confirmados después de MySQL; `event_id` como llave primaria | Ningún evento se pierde y ninguno se cuenta dos veces | `enable_auto_commit=True`: puede confirmar mensajes que nunca se guardaron |
| Eventos inválidos a una tabla y a una DLQ, sin detener el consumidor | Un mensaje dañado no debe parar el flujo, pero hay que poder revisarlo | Descartarlos en silencio o detener el consumidor |
| Umbral de alerta = p95 histórico del DW | Se recalcula solo si el DW cambia; con la ECV 2025 da 1.868,2 kg | Un umbral fijo sin respaldo en los datos |
| Grafana con conexión y dashboard aprovisionados | Refresco automático cada 5 s y configuración reproducible desde el repositorio | Power BI: con el conector de MySQL en modo de importación no hay actualización automática de página |
| Scripts de Kafka fuera de Docker | Se ejecutan y detienen a mano en la demostración (experimento de interrupción) | Contenedores: más reproducibles, pero menos directos para mostrar el `Ctrl+C` y el reinicio |

**Limitaciones del streaming.**
- **Los eventos son históricos re-emitidos**, no reportes nuevos: las métricas describen la muestra de la ECV en orden aleatorio, no la evolución real de los hogares.
- **Un solo broker y factor de replicación 1:** si el contenedor de Kafka se pierde con su volumen, se pierden los eventos no procesados. En producción se usarían al menos 3 brokers.
- **Entrega al menos una vez:** el resultado no tiene duplicados gracias a la llave primaria, pero un evento puede leerse dos veces de Kafka.
- **El estado vive en MySQL:** las métricas se reconstruyen al reiniciar, pero si la base no está disponible el consumidor se detiene.
- **Las métricas son acumuladas desde el último reinicio de la demo** (`reset_streaming.py`), no por ventanas de tiempo, salvo "eventos en el último minuto".
- **El contrato no tiene registro de esquemas** (Schema Registry): la versión se controla con `schema_version` y el código compartido.

**Seguridad y confiabilidad.**
- Ninguna credencial está en el repositorio: MySQL, Airflow y Grafana leen sus contraseñas del `.env`, que está en `.gitignore`; la conexión de Grafana usa variables de entorno y guarda la contraseña como dato cifrado (`secureJsonData`).
- Kafka usa `PLAINTEXT` sin autenticación y Grafana permite ver el dashboard sin iniciar sesión (solo lectura). Es aceptable en un entorno local de demostración; en producción se activarían TLS/SASL en Kafka y se desactivaría el acceso anónimo.
- El productor publica con `acks=all` y reintentos, y valida cada evento antes de enviarlo.

## 13. Integrantes y distribución de responsabilidades

> **Responsables:** Equipo Completo (Nikol Hurtado, David Aponte, Diana Panesso, Sebastián Celorio)

Este proyecto fue desarrollado de manera colaborativa para la **Entrega Final (Fase 3)**, asegurando la integración del Data Warehouse dimensional, la orquestación con Apache Airflow, compuertas de calidad con Great Expectations, analítica de negocio en Power BI y el nuevo pipeline de streaming en tiempo real con Apache Kafka y Grafana.

---

### 13.1 Detalle de roles y entregables

#### 1. Nikol Hurtado (Persona 1: Infraestructura y Orquestación)
* **Integración de Kafka:** Actualización y configuración de `docker-compose.yaml` para incorporar los servicios de Apache Zookeeper y el broker de Apache Kafka (`confluentinc/cp-kafka`), garantizando su coexistencia y arranque armónico junto con Apache Airflow, PostgreSQL y MySQL.
* **Refinamiento del DAG:** Optimización del DAG de Airflow (`etl_huella_carbono_dag.py`) asegurando la separación modular y explícita de cada etapa del pipeline (extracción, limpieza, validación de calidad, transformación y carga), documentando dependencias, políticas de reintento (`retries`, `retry_delay`) y manejo de excepciones deterministas vs transitorias.
* **Gestión del Repositorio y Entorno:** Estructuración del repositorio, definición de dependencias en `requirements.txt` y `requirements-streaming.txt` (confluent-kafka, kafka-python, etc.), y redacción detallada de las instrucciones de despliegue, ejecución y reproducibilidad en Docker.
* **Secciones del README a cargo:**
  * [Sección 3. Arquitectura del sistema](#3-arquitectura-del-sistema)
  * [Sección 4. Pipeline ETL batch](#4-pipeline-etl-batch)
  * [Sección 10. Ejecución y reproducibilidad](#10-ejecución-y-reproducibilidad)
  * [Sección 11.1 y 11.3. Evidencia de orquestación y tolerancia a fallos](#11-evidencia-funcional)

#### 2. David Aponte (Persona 2: Calidad de Datos y API)
* **Cierre de Great Expectations:** Configuración y mantenimiento de las 3 compuertas de calidad (`validate_primary_raw`, `validate_api_raw`, `validate_prepared`), garantizando el bloqueo de cargas ante fallas críticas (`Critical`) y la notificación ante advertencias (`Warning`). Exportación y documentación de la *expectation suite* final y evidencias de validación (Data Docs).
* **Documentación EDA y Perfilamiento:** Redacción del análisis exploratorio de datos (EDA) sobre la ECV 2025 del DANE y los datos del IDEAM, detallando cómo los hallazgos empíricos (nulos en pago de energía, inconsistencias de estrato, fallas de sensor en estaciones meteorológicas) guiaron las reglas de calidad y decisiones técnicas de imputación.
* **Justificación e Integración de la API:** Documentación técnica y analítica del endpoint SODA del IDEAM (dataset `sbwg-7ju4`), justificando formalmente el valor agregado frente al dataset estático original al permitir la desagregación climática por piso térmico.
* **Secciones del README a cargo:**
  * [Sección 2. Fuentes de datos y EDA](#2-fuentes-de-datos-y-eda)
  * [Sección 5. Calidad de datos](#5-calidad-de-datos)
  * [Sección 11.2. Evidencia de la falla controlada (Prueba B)](#112-evidencia-de-la-falla-controlada-prueba-b)
  * Documentación complementaria: [`docs/quality_rules.md`](docs/quality_rules.md)

#### 3. Diana Panesso (Persona 3: Transformación, DW y Streaming)
* **Mantenimiento del Data Warehouse Dimensional:** Implementación y validación del modelo estrella en MySQL 8 compuesto por la tabla de hechos `fact_huella_carbono` y sus 5 dimensiones (`dim_hogar`, `dim_ubicacion`, `dim_clima`, `dim_combustibles`, `dim_tarifas`), con cargas transaccionales atómicas (DELETE + INSERT + COMMIT/ROLLBACK) y conciliación estricta de registros.
* **Productor Kafka (`producer_events.py` / `producer_metrics.py`):** Desarrollo del script productor que se conecta a la base de datos MySQL, calcula y emite eventos bajo un contrato estricto (`event_contract.py`), publicando continuamente métricas agregadas y registros de hogares hacia el tópico `residential_emissions`.
* **Consumidor Kafka (`consumer_events.py` / `consumer_metrics.py`):** Desarrollo del script consumidor que se suscribe al tópico de Kafka, procesa métricas en tiempo real con bajas latencias (< 30 ms), gestiona alertas de alta huella (percentil 95), enruta mensajes defectuosos a la Dead Letter Queue (DLQ) y persiste el estado en MySQL para el monitoreo.
* **Secciones del README a cargo:**
  * [Sección 6. Data Warehouse](#6-data-warehouse)
  * [Sección 8. Pipeline de streaming](#8-pipeline-de-streaming)
  * [Sección 11.4. Evidencia funcional del streaming](#114-evidencia-funcional-del-pipeline-de-streaming)
  * Documentación complementaria: [`docs/supuestos_y_trazabilidad.md`](docs/supuestos_y_trazabilidad.md)

#### 4. Sebastián Celorio (Persona 4: Analítica y Negocio)
* **Tablero Analítico en Power BI:** Diseño, modelado y construcción del dashboard analítico en Power BI conectado de manera exclusiva al Data Warehouse en MySQL 8 (sin orígenes CSV locales intermedios), respondiendo a los requerimientos y preguntas clave del negocio (KPI R1 a R7).
* **Matriz de Logro de Objetivos de Negocio:** Elaboración de la matriz integral exigida en la rúbrica (*Business Objectives Achievement*), alineando Objetivo de Negocio, Pregunta Analítica, KPI, Evidencia Cuantitativa en el DW y Decisión Respaldada para cada dimensión del problema.
* **Documentación Analítica y Estratégica:** Redacción de las conclusiones de negocio, interpretación de patrones de consumo residencial, correlación entre temperatura y huella eléctrica, y síntesis de limitaciones, desafíos y recomendaciones de ingeniería a futuro.
* **Secciones del README a cargo:**
  * [Sección 1. Descripción del proyecto y objetivos](#1-descripción-del-proyecto-y-objetivos)
  * [Sección 7. Dashboard analítico (batch)](#7-dashboard-analítico-batch)
  * [Sección 9. Dashboard en tiempo real](#9-dashboard-en-tiempo-real)
  * [Sección 12. Decisiones de ingeniería y limitaciones](#12-decisiones-de-ingeniería-y-limitaciones)
  * Documentación complementaria: [`docs/bi_dashboard_guide.md`](docs/bi_dashboard_guide.md)

---

### 13.2 Matriz de trazabilidad: Secciones del README vs. Roles

| Sección del README | Título de la Sección | Responsable Principal | Rol / Enfoque |
|---|---|---|---|
| **1** | [Descripción del proyecto y objetivos](#1-descripción-del-proyecto-y-objetivos) | **Sebastián Celorio** | Analítica y Negocio (R1–R7, Matriz de Objetivos) |
| **2** | [Fuentes de datos y EDA](#2-fuentes-de-datos-y-eda) | **David Aponte** | Calidad de Datos y API (DANE ECV, IDEAM SODA, EDA) |
| **3** | [Arquitectura del sistema](#3-arquitectura-del-sistema) | **Nikol Hurtado** | Infraestructura (Docker, Airflow, Kafka, MySQL) |
| **4** | [Pipeline ETL batch](#4-pipeline-etl-batch) | **Nikol Hurtado** | Orquestación (DAG Airflow, dependencias, reintentos) |
| **5** | [Calidad de datos](#5-calidad-de-datos) | **David Aponte** | Calidad de Datos (Great Expectations, compuertas DQ) |
| **6** | [Data Warehouse](#6-data-warehouse) | **Diana Panesso** | DW & Modelo (Esquema estrella, integridad, transacciones) |
| **7** | [Dashboard analítico (batch)](#7-dashboard-analítico-batch) | **Sebastián Celorio** | Analítica (Power BI, análisis de KPI R1–R7) |
| **8** | [Pipeline de streaming](#8-pipeline-de-streaming) | **Diana Panesso** | Streaming (Kafka, productor, consumidor, contratos, DLQ) |
| **9** | [Dashboard en tiempo real](#9-dashboard-en-tiempo-real) | **Sebastián Celorio** / **Diana Panesso** | Analítica y Streaming (Grafana, métricas en vivo) |
| **10** | [Ejecución y reproducibilidad](#10-ejecución-y-reproducibilidad) | **Nikol Hurtado** | Infraestructura (Comandos Docker Compose, scripts) |
| **11** | [Evidencia funcional](#11-evidencia-funcional) | **Equipo Completo** | Pruebas funcionales A, B, C, C-2 y Streaming |
| **12** | [Decisiones de ingeniería y limitaciones](#12-decisiones-de-ingeniería-y-limitaciones) | **Sebastián Celorio** | Negocio y Arquitectura (Supuestos, limitaciones y lecciones) |
| **13** | [Integrantes y responsabilidades](#13-integrantes-y-distribución-de-responsabilidades) | **Equipo Completo** | Coordinación y distribución de trabajo de la Entrega Final |

# Paso 1 – Modelo de Datos

## 1. Principios de diseño

El modelo de datos está orientado a:
- Comparar resultados entre elecciones
- Realizar agregaciones por puesto de votación
- Minimizar el volumen de datos para análisis
- Separar datos crudos de datos agregados

No se realizan análisis directamente sobre datos mesa a mesa.

## 2. Niveles de datos

El sistema maneja tres niveles:

- RAW: datos originales mesa a mesa
- CLEAN: datos normalizados y validados
- AGGREGATED: resultados consolidados por puesto de votación

## 3. Entidades principales

- Departamento
- Municipio
- Puesto de votación
- Elección
- Resultados electorales

## 4. Decisiones clave

- El puesto de votación es la unidad principal de análisis
- Las comparaciones entre elecciones se realizan sobre datos agregados
- Se utilizan claves foráneas para garantizar integridad
- Se crean índices para optimizar rankings y cruces

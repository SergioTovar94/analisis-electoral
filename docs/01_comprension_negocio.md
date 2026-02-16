# Fase 1 – Comprensión del negocio

## 1.1 Contexto electoral

Análisis presentado para las elecciones a la Cámara de Representantes de 2026.

## 1.2 Problema de negocio

Las campañas políticas cuentan con grandes volúmenes de datos electorales
dispersos en múltiples archivos y formatos. Analizar estos datos manualmente
en hojas de cálculo es lento, propenso a errores y no escalable.

Se requiere un sistema que permita transformar datos electorales crudos en
información estratégica accionable.

## 1.3 Objetivos del análisis

**Principal:** Entregar listas de puestos de votación clave para pauta en redes sociales.

## 1.4. Preguntas que el sistema debe responder

- ¿Cuáles son los 100 puestos de votación donde más votos perdió X candidato entre
  dos elecciones?
- ¿Cuáles son los 100 puestos donde históricamente se obtienen mejores resultados?
- ¿Cuáles son los 100 puestos con mayor cantidad de votos nulos?
- ¿Qué puestos cumplen simultáneamente múltiples criterios?
  - Ejemplo: alta nulidad y baja pérdida de votos

## 1.5. Operaciones principales

- Ingesta de datos desde archivos
- Mapping interactivo de columnas
- Validación de calidad de datos
- Normalización de identificadores geográficos
- Agregación de resultados por puesto de votación
- Comparación entre elecciones
- Generación de rankings
- Exportación de resultados a Excel

## 1.6. Restricciones

- El sistema no debe caerse por volumen de datos
- El procesamiento debe ser reproducible
- Los resultados deben ser consistentes entre ejecuciones
- El backend debe estar desacoplado del frontend

## 1.7. Métricas de éxito

- Rankings generados en segundos
- Datos agregados coherentes entre elecciones
- Capacidad de cruzar rankings sin reprocesar datos crudos
- Exportación correcta a Excel
